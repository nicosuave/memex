# Memex chat capability review

Current Memex has the core chat and local workspace workflows. The strongest remaining issues are **required MCP form replies**, **interactive MCP app rendering**, **remote files/review/terminals**, and **plugin/custom-MCP management**. These are narrower than “missing agents,” “missing remote execution,” or “missing app tools.”

This is a source-backed inventory, not a new product test campaign. Priorities are judgments about workflow consequences, not measured usage or severity scores. [Open the filterable report](index.html) · [Download all findings](findings.json).

## What should matter next

| Priority | Workflow | Finding | Why it matters |
|---|---|---|---|
| P1 | [MCP structured form elicitation](#codex-conversation-11) | defect | A form that requires entered values cannot be completed correctly through this path; clicking Allow does not supply them. |
| P1 | [Interactive MCP app results](#codex-conversation-14) | missing | Runtime protocol support alone does not make interactive tool apps usable inside Memex. |
| P1 | [Files, review and terminals for remote chats](#remote-panels) | missing | A remote chat can execute, but native workspace inspection and shell UX disappear rather than switching to host-aware APIs. |
| P1 | [Install, configure, authenticate and remove plugins/custom MCP servers](#cx-p01) | partial | Users must manage tools through provider configuration or other apps even though local skills can be selected in Memex. |

Fix required-form correctness first: there is no form UI collecting the required values, and Allow sends empty content. The adapter advertises proprietary OpenAI form elicitation as disabled; the finding applies when a generic content-bearing MCP form reaches the traced Codex path. Interactive MCP apps need a native host/renderer, not only runtime metadata. Remote workspace panels should extend existing paired-host execution. Plugin management is a usability gap around configuration/authentication, not proof that provider-configured tools cannot execute.

The next tier is selective: richer PR review, subagent navigation, editable plan documents, scheduled fresh chats and run triage, SSH onboarding, same-chat host/worktree migration, and richer input/context handling. Custom sections, shortcut remapping, external-app control, dictation and provider breadth should follow explicit product demand. Queue-to-side-chat is convenience, P3.

## Capabilities and strengths to retain

The inspected expansion closes earlier claims about first-prompt configuration, attachments, mentions, queue/steer, uncertain-delivery recovery, plans, fork/rewind, outline, notifications, local files/Git/terminal and basic worktrees. Remote execution and durable recurring schedules already exist. The separate opt-in memex-control stdio MCP supplies conversation, queue, schedule, worktree and granted browser control.

Memex also has useful protective behavior: durable captured prompt bytes, explicit uncertain-send recovery, non-destructive stash policy, disk-fingerprint conflict checks, checkpoint index preservation and evidence-oriented history retrieval. T3 defects below are reasons to preserve these properties, not Memex backlog items.

## Versions, scope and validation

- **Codex:** `26.930.51102 (13100)`; `/Applications/ChatGPT.app`. Info.plist CFBundleIdentifier com.openai.codex; readArchive/readEntry extraction only.
- **Memex:** `17e958778a7398db8238cc9c8415ca12649067e5`; `/Users/nico/Code/memex-chat-capabilities`. Contract pinned source baseline.
- **Memex runtime:** `7818518b094f44911aecfec7dbeb905c57ea24a9`; `/Users/nico/Code/memex-chat-runtime`. Contract pinned optional SQACPHost source; availability gated by Memex Package.swift build configuration.
- **T3:** `ecfdda5fa804582066c1aa62afea419677c24883`; `/private/tmp/t3-chat-inventory-ecfdda5`. Fresh git ls-remote --symref https://github.com/pingdotgg/t3code.git HEAD then detached clone; package server/desktop version 0.0.45.

Installed Codex archive SHA-256: `a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. [Codex asset hashes](codex/version-manifest.json); [T3 provenance](t3/manifest.json). Local proprietary evidence is referenced by copied exact path, anchor/byte offset and hash; it is not served or uploaded. Public source links use pinned commits.

The optional private runtime/history-only build split is a packaging qualifier, not a P1 gap in the supplied runtime-enabled baseline. Sidebar snooze is explicitly excluded. No absent comparator feature is inferred when only the other tree covered it. The earlier live queue observation is narrow historical evidence and is not used to certify these source findings.

- No new product builds, tests, provider prompts, latency benchmarks or assistive-technology checks were performed. Existing test files are evidence of intended contracts, not new passes.
- Bundled Codex code does not prove current account entitlement, feature-flag state or successful native backend operation. Dictation, side-chat, plan editing, MCP apps and computer use retain their gates.
- Hosted cloud parity is unresolved. ChatGPT Work/Pages/consumer-only assets do not establish Codex desktop parity or Memex service availability.
- T3 universal plugin marketplace, arbitrary OS control and managed compute are unproven. Completed Codex facets separately establish bounded Memex plugin UI and external-app control gaps.
- Long-history full projection is an unmeasured architecture concern; no latency or memory regression is asserted.
- Advanced math/Mermaid/audio/video rendering equivalence and asynchronous question-resolution parity were not established. Provider capabilities and installed executable versions vary.
- Codex snapshot pruning is evidenced by renderer/native request contracts; native snapshot retention/exclusion implementation was not extracted.

## Unified capability table

72 source findings map to 54 workflow rows; these counts describe inventory bookkeeping, not completeness or parity scores. Every original finding is preserved in JSON facets with its raw ID and comparator-specific status. Stale “another worker owns this” notes are resolved by the completed facets.

| Workflow | Memex disposition / subject | Priority | Comparator coverage | Raw IDs |
|---|---|---|---|---|
| [MCP structured form elicitation](#codex-conversation-11) | Memex: defect | P1 | Codex | codex-conversation-11 |
| [T3 checkpoint staging-state loss](#t3-w04) | T3: defect | P1 | T3 | T3-W04 |
| [T3 editor concurrent-write overwrite](#t3-w06) | T3: defect | P1 | T3 | T3-W06 |
| [T3 ordinary send clears durable draft before acknowledgement and retries with new identity](#t3-i14) | T3: defect | P1 | T3 | T3-I14 |
| [Files, review and terminals for remote chats](#remote-panels) | Memex: missing | P1 | T3, Codex | T3-W09, codex-workspace-10 |
| [Interactive MCP app results](#codex-conversation-14) | Memex: missing | P1 | Codex | codex-conversation-14 |
| [Install, configure, authenticate and remove plugins/custom MCP servers](#cx-p01) | Memex: partial | P1 | Codex | CX-P01 |
| [T3 non-live approval overflow actions remain enabled](#t3-i09) | T3: defect | P2 | T3 | T3-I09 |
| [T3 stash image loss window](#t3-i11) | T3: defect | P2 | T3 | T3-I11 |
| [Integrated dictation and dictation recovery](#codex-conversation-03) | Memex: missing | P2 | Codex | codex-conversation-03 |
| [Move an existing chat and Git state between checkout/worktree/host](#codex-workspace-11) | Memex: missing | P2 | Codex | codex-workspace-11 |
| [Agent-callable app toolkit and desktop controls](#app-control) | Memex: partial | P2 | T3, Codex | T3-W11, CX-P07 |
| [Annotation-aware attachment editing](#codex-conversation-02) | Memex: partial | P2 | Codex | codex-conversation-02 |
| [App-owned browser control and action breadth](#browser) | Memex: partial | P2 | T3, Codex | T3-W08, codex-workspace-07 |
| [Automatic pruning with recoverable snapshots](#codex-workspace-02) | Memex: partial | P2 | Codex | codex-workspace-02 |
| [Editable plan document workflow](#codex-conversation-07) | Memex: partial | P2 | Codex | codex-conversation-07 |
| [Files, pasted images, and media ergonomics](#t3-i02) | Memex: partial | P2 | T3 | T3-I02 |
| [Inline GitHub PR review and diff controls](#pr-review) | Memex: partial | P2 | T3, Codex | T3-W02, codex-workspace-04 |
| [Native apps and external browser control](#codex-workspace-08) | Memex: partial | P2 | Codex | codex-workspace-08 |
| [Organize conversations with custom sections and read/unread state](#cx-p03) | Memex: partial | P2 | Codex | CX-P03 |
| [Policy amendment and reviewer detail UI](#codex-conversation-12) | Memex: partial | P2 | Codex | codex-conversation-12 |
| [Provider catalog, model/effort and permission configuration](#t3-i12) | Memex: partial | P2 | T3 | T3-I12 |
| [Provider-specific steering fallback](#t3-i05) | Memex: partial | P2 | T3 | T3-I05 |
| [Recurring work, new-chat targets and run triage](#schedules) | Memex: partial | P2 | T3, Codex | T3-W10, CX-P02 |
| [Remote execution and integrated SSH onboarding](#codex-workspace-09) | Memex: partial | P2 | Codex | codex-workspace-09 |
| [Shortcut customization and appearance controls](#keyboard-settings) | Memex: partial | P2 | T3, Codex | T3-TR-10, CX-P04 |
| [Structured questions and media answers](#questions) | Memex: partial | P2 | T3, Codex | T3-I07, codex-conversation-10 |
| [Subagent visibility, detail and navigation](#subagents) | Memex: partial | P2 | T3, Codex | T3-TR-02, codex-conversation-08 |
| [Queued message to side chat](#codex-conversation-05) | Memex: missing | P3 | Codex | codex-conversation-05 |
| [Accessibility hooks](#t3-tr-13) | Memex: present | none | T3 | T3-TR-13 |
| [Checkpoint and conversation rewind](#t3-w03) | Memex: present | none | T3 | T3-W03 |
| [Choose models, reasoning and provider-specific permissions](#cx-p06) | Memex: present | none | Codex | CX-P06 |
| [Commit, push, draft PR and local diffs](#codex-workspace-03) | Memex: present | none | Codex | codex-workspace-03 |
| [Completion, approval and question notifications](#notifications) | Memex: present | none | T3, Codex | T3-TR-09, CX-P05 |
| [Composer, attachments and captured context](#composer-context) | Memex: present | none | T3, Codex | T3-I01, T3-I03, codex-conversation-01 |
| [Conversation outline and reading position](#t3-tr-07) | Memex: present | none | T3 | T3-TR-07 |
| [Durable queue, edit, reorder and steering](#queue) | Memex: present | none | T3, Codex | T3-I04, codex-conversation-04 |
| [Exact in-conversation Find and raw evidence](#t3-tr-06) | Memex: present | none | T3 | T3-TR-06 |
| [Historical search scope](#t3-tr-05) | Memex: present | none | T3 | T3-TR-05 |
| [Local file browsing, editing and conflict protection](#files) | Memex: present | none | T3, Codex | T3-W05, codex-workspace-05 |
| [Native forks, lineage and context branches](#forks) | Memex: present | none | T3, Codex | T3-TR-03, codex-conversation-09 |
| [Persistent local workspace terminals](#terminal) | Memex: present | none | T3, Codex | T3-W07, codex-workspace-06 |
| [Projects and managed worktree lifecycle](#worktrees) | Memex: present | none | T3, Codex | T3-W01, codex-workspace-01 |
| [Prompt stash and recall](#t3-i10) | Memex: present | none | T3 | T3-I10 |
| [Provider-specific approval options](#t3-i08) | Memex: present | none | T3 | T3-I08 |
| [Readable structured transcript and raw evidence](#transcript) | Memex: present | none | T3, Codex | T3-TR-04, codex-conversation-13 |
| [Rename, pin, archive, removal and bulk ordering](#t3-tr-08) | Memex: present | none | T3 | T3-TR-08 |
| [Stop acknowledgement and uncertain delivery recovery](#recovery) | Memex: present | none | T3, Codex | T3-I06, codex-conversation-06 |
| [Structured plans and implementation](#t3-tr-01) | Memex: present | none | T3 | T3-TR-01 |
| [Hosted cloud task and browser surface](#codex-workspace-12) | Validation: unknown | none | Codex | codex-workspace-12 |
| [Long-history publish architecture](#t3-tr-12) | Validation: unknown | none | T3 | T3-TR-12 |
| [Unproven universal capabilities](#t3-w12) | Validation: unknown | none | T3 | T3-W12 |
| [Build/runtime availability boundary](#t3-i13) | Scope: not-applicable | none | T3 | T3-I13 |
| [Sidebar snooze](#t3-tr-11) | Memex: intentionally-excluded | none | T3 | T3-TR-11 |

## Detailed findings

<a id="codex-conversation-11"></a>

### MCP structured form elicitation

**Memex: defect · P1 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

A form that requires entered values cannot be completed correctly through this path; clicking Allow does not supply them.

**Recommendation:** Add a typed elicitation form/reply path or explicitly reject unsupported required forms instead of offering a misleading empty accept.

**codex-conversation-11 — Codex installed desktop:** MCP elicitation dispatcher recognizes formElicitation/openaiForm/legacyOpenAIForm and opens a dedicated form component.

**Memex:** The Codex transport projects MCP elicitation as generic approval. There is no schema-backed input form; Allow returns content:null. Required form values cannot be collected or returned.

**Limits:** Source-proven limitation of Memex Codex transport, not a claim that every MCP elicitation requires content or that every ACP provider has the same behavior. The Codex adapter advertises mcpServerOpenaiFormElicitation:false at line282. The defect is the generic MCP elicitation request path when a content-bearing form reaches it, not a promise that proprietary OpenAI forms are negotiated.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/mcp-server-elicitation-request-panel-15e9f832a59b.js:1](</tmp/codex-parity-20261005/webview/assets/mcp-server-elicitation-request-panel-15e9f832a59b.js:1>) — Dedicated form elicitation dispatch, separate from simple approvals. Version: `26.930.51102 (13100)`. Anchor: `` case`formElicitation` ``. Byte: 9653. Asset SHA-256: `15f178962c0ced0609a53b502c79ccb1f4110e50fc2cc8f9f4fa4295547a8385`.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2094–2123](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2094>) — Elicitation is projected as approval, not a schema-backed question form. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` handleServerRequest `.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1575–1583](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1575>) — Accepted response explicitly contains NSNull content. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` respondPermission / mcpServer/elicitation/request `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:50–65](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L50-L65) — Memex supplies only title/detail/options to approval UI. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` pendingApprovals `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/request-panel-49ba9fe401fd.js:2](</tmp/codex-parity-20261005/webview/assets/request-panel-49ba9fe401fd.js:2>) — Actual form submit serializes collected form content t into elicitation response; not just a dedicated label. Version: `26.930.51102 (13100)`. Anchor: ` replyWithMcpServerElicitationResponse(a,c,pe(e,null,t??{})) `. Byte: 63825. Asset SHA-256: `1ded037b6b95456d619c6ac7b505333269c2dd426946702d887bfbc30a58caf4`.

<a id="t3-w04"></a>

### T3 checkpoint staging-state loss

**T3: defect · P1 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

T3 cannot reconstruct the original staged/unstaged split from this checkpoint shape; copying it regresses Memex.

**Recommendation:** Keep separate index/worktree snapshots. Add an upstream regression test if pursuing a T3 fix.

**T3-W04 — T3:** Capture stores one final workspace tree; restore writes that tree to both worktree and staging index.

**Memex:** Captures a separate index tree and restores it after raw worktree restoration.

**Limits:** Deterministic source-level defect, not reproduced by executing Git or tests in this inventory.

**Evidence:**

- [T3 · apps/server/src/vcs/GitVcsDriver.ts:1030–1069](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/vcs/GitVcsDriver.ts#L1030-L1069) — Single staged temporary-index tree becomes checkpoint commit. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` captureCheckpoint `.

- [T3 · apps/server/src/vcs/GitVcsDriver.ts:1091–1102](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/vcs/GitVcsDriver.ts#L1091-L1102) — git restore passes both --worktree and --staged with the same source. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` restoreCheckpoint `.

- [Memex · apps/macos/Sources/Memex/WorkspaceCheckpoints.swift:98–101](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceCheckpoints.swift#L98-L101) — Restores checkpoint.indexTree separately. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` restore `.

- [Memex · apps/macos/Tests/MemexTests/WorkspaceGitWorkflowTests.swift:47–76](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/WorkspaceGitWorkflowTests.swift#L47-L76) — Existing test distinguishes staged contents and verifies recovery snapshot. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` checkpointsPreserveIndexAndRestoreOnlyOwnedIsolatedWorktree `.

<a id="t3-w06"></a>

### T3 editor concurrent-write overwrite

**T3: defect · P1 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

T3 autosave can silently overwrite concurrent agent edits; this is a concrete design to avoid.

**Recommendation:** Preserve Memex optimistic concurrency and explicit disk-versus-draft reconciliation.

**T3-W06 — T3:** Save sends only cwd, path and new contents; server unconditionally writes contents with no expected revision.

**Memex:** Save passes loaded fingerprint and throws a conflict if the disk version differs; draft remains available.

**Limits:** Source-level race scenario; no runtime reproduction performed.

**Evidence:**

- [T3 · apps/web/src/components/files/useFileSaveCoordinator.ts:31–40](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/useFileSaveCoordinator.ts#L31-L40) — No expected content revision in write request. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` persist `.

- [T3 · apps/server/src/workspace/WorkspaceFileSystem.ts:305–340](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/workspace/WorkspaceFileSystem.ts#L305-L340) — Path resolution followed by unconditional file write. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` writeFile `.

- [Memex · apps/macos/Sources/Memex/WorkspaceFilesView.swift:224–240](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFilesView.swift#L224-L240) — Passes fingerprint and loads disk version after conflict. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` save `.

- [Memex · apps/macos/Sources/Memex/WorkspaceFiles.swift:58–78](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFiles.swift#L58-L78) — Checks expectedFingerprint before write. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` save `.

<a id="t3-i14"></a>

### T3 ordinary send clears durable draft before acknowledgement and retries with new identity

**T3: defect · P1 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

A reload/close during a pre-dispatch await can discard the only recoverable prompt snapshot. After accepted-then-lost acknowledgement, a manually retried ordinary send has new identities, so server receipt deduplication cannot identify it as the original command. Duplicate execution is a supported risk, not an observed runtime result.

**Recommendation:** Do not copy this client recovery design. Persist the complete outbound intent and original command/message IDs before clearing, reconcile receipt/native state after reconnect, and distinguish rejected from ambiguous outcomes.

**T3-I14 — T3:** The web path snapshots content into local function variables and an optimistic React state row, clears composer content, then awaits settings/upload preparation and startThreadTurn. Failure restores from that closure only when the current draft is empty. A fresh onSend allocates a new messageId; startThreadTurn allocates a fresh commandId because the call supplies no retained commandId. The ordinary path has no durable uncertain-intent record or same-identity retry guard.

**Memex:** Memex persists the outgoing command identity and captured attachments before provider dispatch; ambiguous errors become uncertain. Recovery requires reconnect/review, and restoring the pending prompt does not send it. Queue recovery also retains unconfirmed command identity.

**Limits:** Bounded to the ordinary single-target web send branch. Multi-model fanout has a separate in-memory uncertain-submission map; it was not audited here for persistence. Server-side receipts remain useful for retries of the same command ID; the defect is the UI's loss of original outbound intent/identity. No test claiming an induced reload or duplicate execution was run.

**Evidence:**

- [T3 · apps/web/src/components/ChatView.tsx:1816](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L1816-L1816) — Ordinary optimistic input is React useState. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` optimisticUserMessages `.

- [T3 · apps/web/src/components/ChatView.tsx:8911–8944](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L8911-L8944) — Content remains in closure; new message ID allocated for each submission. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` ordinary send snapshot `.

- [T3 · apps/web/src/components/ChatView.tsx:9449–9451](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9449-L9451) — Clears composer before later asynchronous work. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` clear before awaits `.

- [T3 · apps/web/src/components/ChatView.tsx:9485–9507](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9485-L9507) — Pre-dispatch awaits occur after durable composer clear. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` settings and attachment awaits `.

- [T3 · apps/web/src/components/ChatView.tsx:9549–9593](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9549-L9593) — New ordinary send passes messageId but no retained commandId. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` startThreadTurn invocation `.

- [T3 · apps/web/src/components/ChatView.tsx:9665–9723](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9665-L9723) — Restores local snapshots conditionally and reports generic failure. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` failure restoration `.

- [T3 · packages/client-runtime/src/operations/commands.ts:261–270](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L261-L270) — Missing caller ID creates a random UUID. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` allocateCommandId/dispatch `.

- [T3 · packages/client-runtime/src/operations/commands.ts:626–639](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L626-L639) — Allocates command identity before preparing attachment command payload. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` startThreadTurn `.

- [T3 · packages/client-runtime/src/operations/commands.ts:755–772](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L755-L772) — Dispatch uses newly allocated command and message identity. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` message.dispatch `.

- [T3 · apps/web/src/components/ChatView.tsx:9156–9168](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9156-L9168) — Separate multi-model branch has a guard; it is excluded from this finding. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` fanout uncertainty guard `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:779–818](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L779-L818) — Persistent intent precedes provider boundary, ambiguous result requires reconnect. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` perform `.

- [Memex · apps/macos/Sources/Memex/ConversationPendingView.swift:26–34](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPendingView.swift#L26-L34) — Restore draft warns review first and does not resend. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` recovery control `.

- [Memex · apps/macos/Tests/MemexTests/LiveConversationTests.swift:159–208](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/LiveConversationTests.swift#L159-L208) — Existing tests cover no replay and native identity confirmation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` uncertain/native identity tests `.

<a id="remote-panels"></a>

### Files, review and terminals for remote chats

**Memex: missing · P1 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

A remote chat can execute, but native workspace inspection and shell UX disappear rather than switching to host-aware APIs.

**Recommendation:** Add host-scoped files/diff/terminal transport and bind the existing panels to verified remote workspace identity.


**Current Memex:** Paired execution hosts already create and resume durable remote conversations and manage remote worktrees. However, selectedWorkspace excludes paired-host and server-owned conversations, so native Files, Changes/review and terminal panes remain local-only instead of routing to host-aware APIs.

**T3-W09 — T3:** Environment-scoped clients and SSH/connect/direct routes feed the same workspace panels.

**codex-workspace-10 — Installed ChatGPT Codex desktop:** Non-durable terminal and editable-file branches pass explicit hostId to their implementations; local-conversation Git action queries include hostConfig/cwd. Durable cloud-specific branches are separate and are not the evidence for SSH host parity.

**Limits:** No claim that T3 or Memex provides arbitrary managed cloud compute. Deployment availability was not tested. Codex backend terminal/file operation correctness is not runtime-tested. Memex absence is established at the exact UI gating boundary, not inferred from search.

**Evidence:**

- [T3 · apps/web/src/components/settings/EnvironmentRow.tsx:20–44](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/EnvironmentRow.tsx#L20-L44) — SSH, WSL, T3 Connect and direct remote routes. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` environment transport presentation `.

- [T3 · apps/web/src/components/settings/EnvironmentRoutesList.tsx:39–68](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/EnvironmentRoutesList.tsx#L39-L68) — Prioritized connection routes and active route. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` EnvironmentRoutesList `.

- [T3 · apps/web/src/components/files/useFileSaveCoordinator.ts:19–38](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/useFileSaveCoordinator.ts#L19-L38) — File operations route by environmentId. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` persist `.

- [Memex · apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:42–77](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L42-L77) — Pair host with URL/token. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Memex · apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:80–111](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L80-L111) — Open and create host conversations. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Memex · apps/macos/Sources/Memex/Store.swift:216–224](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Store.swift#L216-L224) — Explicit local-only workspace boundary. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` selectedWorkspace / canAccessLocalFiles `.

- [Memex · apps/macos/Sources/Memex/WorkspaceTerminalView.swift:20–45](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalView.swift#L20-L45) — Terminal unavailable without local working folder. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/terminal-tab-e527a3db0b81.js:2](</tmp/codex-parity-20261005/webview/assets/terminal-tab-e527a3db0b81.js:2>) — Non-durable terminal qe passes explicit hostId through to terminal component; durable-specific je is a separate branch. Version: `26.930.51102 (13100)`. Anchor: ` conversationTitle:i,cwd:a,hostId:o,initialPromptHint:k `. Byte: 15395. Asset SHA-256: `16bd204b78343ec7d1b31f061f4c9b71d4d5abe729d7392a010c4af4e3bb9b5c`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1](</tmp/codex-parity-20261005/webview/assets/editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1>) — Non-durable file/editor branch De passes explicit hostId to editor; cloud environment branch is separate. Version: `26.930.51102 (13100)`. Anchor: ` commentProps:N,conversationId:k,cwd:n,editor:L,headerActions:r,hideFileNavigation:a,hostId:o `. Byte: 10313. Asset SHA-256: `388c9d123cefdab2f7527afd6e0e165fa8372e3867fec07b2c81b2400ef68eca`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-git-actions-290387202be8.js:1](</tmp/codex-parity-20261005/webview/assets/local-conversation-git-actions-290387202be8.js:1>) — Local-conversation Git action state requests include hostConfig with cwd, including unstaged inspection. Version: `26.930.51102 (13100)`. Anchor: ` hostConfig:g,includeUnstaged:!0 `. Byte: 3603. Asset SHA-256: `89ea20656e5609c2fbed87f9926537dd6516cb61217fc4c873f4bd8ad4f27346`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Store.swift:216–225](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Store.swift#L216-L225) — Explicitly excludes server-owned and paired-host sessions from local file access. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` selectedWorkspace / canAccessLocalFiles `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspacePanelView.swift:17–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L17-L46) — Changes and files require selectedWorkspace. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspacePanelView `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalView.swift:20–37](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalView.swift#L20-L37) — Terminal requires selectedWorkspace; otherwise unavailable. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceTerminalView `.

<a id="codex-conversation-14"></a>

### Interactive MCP app results

**Memex: missing · P1 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Runtime protocol support alone does not make interactive tool apps usable inside Memex.

**Recommendation:** Wire typed runtime app metadata to a native host with scoped messaging and restore behavior, retaining generic fallback.

**codex-conversation-14 — Codex installed desktop:** Local turn renderer passes renderMcpApps into tool activity; MCP tool component resolves app metadata/resource, open/reopen widget activity, and side-panel app representation.

**Memex:** Runtime has AgentMcpAppProjection, but native Memex renderer/projection does not consume mcpApp; its rich-content cases cover text/code/images/attachments and tool results are generic.

**Limits:** Absence boundary: searched apps/macos/Sources/Memex and MemexExecutionHostCore for mcpApp/MCPApp/McpApp/uiResource; no consumers, then traced Reader→TranscriptController→RichContentView/ToolContentRenderer. Codex rendering remains metadata/feature gated; not every tool result is an app.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1](</tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1>) — Local thread turn forwards MCP app rendering controls. Version: `26.930.51102 (13100)`. Anchor: ` renderMcpApps:S,shouldAutoExpandMcpApps:C `. Byte: 16083. Asset SHA-256: `0319cf6508b693232800baee3dc5f8642468c260ae870b4304715bf3abeaa61e`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1](</tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1>) — Tool result offers app opening/open/error/reopen states. Version: `26.930.51102 (13100)`. Anchor: ` mcpApp.activity.status `. Byte: 22163. Asset SHA-256: `22f08427983004be66b5d358b9c5a0f72a5276612c59dba653226a4ca322a5db`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1](</tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1>) — App resource and metadata are resolved for MCP result. Version: `26.930.51102 (13100)`. Anchor: ` invocationResourceUri:n `. Byte: 24263. Asset SHA-256: `22f08427983004be66b5d358b9c5a0f72a5276612c59dba653226a4ca322a5db`.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2412–2420](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2412>) — Underlying runtime retains typed MCP app projection. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` mcpAppProjection `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/TranscriptController.swift:700–728](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptController.swift#L700-L728) — Reachable native reader uses SourceContent and ToolContentRenderer. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` measurement `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RichContentView.swift:46–82](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/RichContentView.swift#L46-L82) — Complete native rich block inventory has no interactive app host. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` RichContentBlock / RichContentDocument `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ToolContentRenderer.swift:19–78](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ToolContentRenderer.swift#L19-L78) — Typed media/text/tool blocks do not create MCP app hosts. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` blocks `.

<a id="cx-p01"></a>

### Install, configure, authenticate and remove plugins/custom MCP servers

**Memex: partial · P1 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Users must manage tools through provider configuration or other apps even though local skills can be selected in Memex.

**Recommendation:** Prioritize a bounded per-provider MCP inventory/configuration/OAuth surface; do not invent one universal provider plugin contract.

**CX-P01 — Installed ChatGPT Codex surface:** Codex-local/work-local settings route exposes plugin management subject to capability gates; custom MCP editor supports stdio and streamable HTTP, saves/uninstalls, and dispatches OAuth login. Plugin installation dispatch reaches app-server plugin/install.

**Memex:** Memex discovers local skill/command files and uses native provider runtimes, but has no plugin/MCP management UI. Optional runtime plugin startup support is not passed by Memex resume connection. This does not mean native provider-configured tools cannot run.

**Limits:** Codex plugin marketplace access depends on host/account capabilities. Installed UI confirmation belongs to root. Absence is native Memex management UI, not absence of MCP execution in its providers.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/use-visible-settings-sections-dc3bbd6366c8.js:1](</tmp/codex-parity-20261005/webview/assets/use-visible-settings-sections-dc3bbd6366c8.js:1>) — Routes bind MCP management to Codex or local Work; skills-settings standalone route explicitly hidden. Version: `26.930.51102 (13100)`. Anchor: `` "mcp-settings":`codexOrWorkLocal` ``. Byte: 40168. Asset SHA-256: `cfaa4b3d5c2a82400a350e73d0f2d4a4e86cf805300aa5f1d8e5e4760f8be5a1`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/plugins-settings-146885f88b18.js:1](</tmp/codex-parity-20261005/webview/assets/plugins-settings-146885f88b18.js:1>) — Custom MCP form Ui validates transport fields and delegates onSave/onUninstall; includes STDIO/streamable_http fields. Version: `26.930.51102 (13100)`. Anchor: ` function Ui(e) `. Byte: 10489. Asset SHA-256: `e6a475adb3408d8e93a07caad7d442b3944bd5d80c4c9a2d44f369001b64a537`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/plugins-settings-146885f88b18.js:1](</tmp/codex-parity-20261005/webview/assets/plugins-settings-146885f88b18.js:1>) — Per-host authenticate action executes MCP OAuth login, validates URL then opens external browser. Version: `26.930.51102 (13100)`. Anchor: `` sendRequest(`mcpServer/oauth/login` ``. Byte: 26146. Asset SHA-256: `e6a475adb3408d8e93a07caad7d442b3944bd5d80c4c9a2d44f369001b64a537`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1592](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1592>) — Installation helper pun sends plugin/install to host app-server. Version: `26.930.51102 (13100)`. Anchor: `` sendRequest(`plugin/install` ``. Byte: 1933382. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposerCatalog.swift:56](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposerCatalog.swift#L56-L56) — Catalog implements local skills/commands discovery, not installation/authentication. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` let skills = [ `. Byte: 2894.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/InAppAgentRuntime.swift:131](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L131-L131) — Memex existing Codex connection passes executable/environment only. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` try service.connectCodex(binding `. Byte: 6104.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:391](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:391>) — Runtime capability exists but defaults to nil; no native Memex management caller. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` pluginStartupOptions: AcpClient.Configuration.StartupOptions? = nil `. Byte: 15552.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:9](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L9-L9) — Native Settings scene is empty; native menus route execution hosts/schedules and workspace controls. App source inventory has no plugin manager. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Settings { EmptyView() } `. Byte: 181.

<a id="t3-i09"></a>

### T3 non-live approval overflow actions remain enabled

**T3: defect · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

This is a concrete misleading/dead-control defect in T3, not an authorization bypass.

**Recommendation:** Do not copy the inconsistent gate: apply response availability to all decisions and explain expired requests.

**T3-I09 — T3:** Primary buttons disable on !canRespond, but More trigger and its menu items only disable on isResponding. The parent callback immediately returns for a non-live request, so these enabled controls silently do nothing.

**Memex:** Memex supplies canRespond to the shared approval component and validates the current request/option in its response method. No equivalent T3 split primary/overflow gate is introduced by the Memex integration.

**Limits:** Source-established UI defect; no interactive reproduction performed. Memex connected-state gating is not identical to T3 native responseCapability; no universal superiority claim.

**Evidence:**

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:6651–6658](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L6651-L6658) — Reachable component receives live-only canRespond. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` approval call site `.

- [T3 · apps/web/src/components/chat/ComposerPendingApprovalActions.tsx:49–85](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx#L49-L85) — Primary respects canRespond; menu does not. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` approval disabled conditions `.

- [T3 · apps/web/src/components/ChatView.tsx:9750–9757](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9750-L9757) — Non-live callbacks silently return. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` onRespondToApproval `.

- [T3 · apps/web/src/components/chat/ComposerPendingApprovalActions.test.tsx:7–63](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.test.tsx#L7-L63) — Tests all use canRespond=true; inspected suite does not cover expired overflow actions. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` existing tests `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:52–67](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L52-L67) — Shared pending-item canRespond is supplied. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` approval integration `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:757–760](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L757-L760) — Current request and option guards. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` approve `.

- [Memex runtime · packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:895–909](</Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:895>) — Shared approval control disables options using approval.canRespond. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` approval option disabled state `.

<a id="t3-i11"></a>

### T3 stash image loss window

**T3: defect · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

T3 has a source-established data-loss window for unsent image context. Its warning/recovery design preserves text but cannot recover those image bytes.

**Recommendation:** Do not copy the two-phase destructive image stash. Retain the original draft or durably stage image bytes before clearing.

**T3-I11 — T3:** T3 writes a text-only entry with pendingImageCount, clears the composer and releases image uploads, then asynchronously encodes and persists images. Hydration explicitly reports those pending images lost if reload interrupts the encode. Storage quota or encoding limits can also omit images; warnings disclose those outcomes.

**Memex:** Memex saves text plus captured attachment content atomically before clearing the unchanged draft and refuses a failed save.

**Limits:** This is a bounded crash/reload defect, not a claim that normal completed stash operations lose images. T3 intentionally compresses stash images and warns about lossy/omitted content; preserve that distinction from accidental silent loss.

**Evidence:**

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:4898–4956](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4898-L4956) — Text-only stash followed by composer clear and upload release. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` stash first phase `.

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:4984–5025](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4984-L5025) — Image encoding occurs after clear and may be non-durable. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` image finalization `.

- [T3 · apps/web/src/promptStashStore.ts:75–101](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/promptStashStore.ts#L75-L101) — Reload explicitly converts interrupted encodes to missing-image notices. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` clearOrphanedPendingImages `.

- [Memex · apps/macos/Sources/Memex/ConversationPromptLibrary.swift:40–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptLibrary.swift#L40-L74) — Whole captured entry saved atomically. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` stash/save `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:233–241](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L233-L241) — Clears only after awaited successful save and only if input is unchanged. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` stashPrompt `.

<a id="codex-conversation-03"></a>

### Integrated dictation and dictation recovery

**Memex: missing · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

System text-field dictation is different from an integrated recording/retry workflow.

**Recommendation:** Consider a dedicated native dictation path if voice input is a product goal.

**codex-conversation-03 — Codex installed desktop:** Shared local composer footer accepts dictation controls; implementation has audio history read/retry/download and transcription.

**Memex:** Native Memex composer and SQACPUI input surface expose text, mentions, file/image context and send/stop; no app-owned recording/transcription flow is wired.

**Limits:** Codex dictation is feature/platform/service dependent; installed source proves integration, not that this account currently has it. No claim is made that macOS system dictation cannot type into Memex.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:118](</tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:118>) — Local composer footer receives dictation state and controls. Version: `26.930.51102 (13100)`. Anchor: ` composerInput:i,dictation:hn,dictationEnabled:n `. Byte: 848733. Asset SHA-256: `234429db84d9319850ff0766a4a5052a9e898bfa0611883633f80018080dd6c0`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:7](</tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:7>) — Dictation history retry reads audio and transcribes it. Version: `26.930.51102 (13100)`. Anchor: ` async function Yze(e,t) `. Byte: 190150. Asset SHA-256: `234429db84d9319850ff0766a4a5052a9e898bfa0611883633f80018080dd6c0`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:42–94](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L42-L94) — Full composer input/callback inventory contains no dictation integration. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` AcpComposerView `.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:1–95](</Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:1>) — Shared native composer types and implementation inspected; no dictation/speech recording APIs found. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` AcpComposerPendingApprovalItem / input types `.

<a id="codex-workspace-11"></a>

### Move an existing chat and Git state between checkout/worktree/host

**Memex: missing · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Context sharing and worktree management do not provide identity-preserving task-and-Git migration.

**Recommendation:** If needed, design explicit handoff preserving native session, repo state and host ownership; do not label context forks equivalent.

**codex-workspace-11 — Installed ChatGPT Codex desktop:** Local-thread environment menu exposes Move to main checkout/Move to worktree; handoff dialog has remote destinations and dirty-workspace checks.

**Memex:** Execution API has context fork/delegate and worktree create/archive/reattach/cleanup; no same-chat workspace/host migration mutation. Native forks explicitly create context handoffs.

**Limits:** Cross-host Codex handler backend is not included in renderer inventory; UI/workflow wiring is observed, not end-to-end migration.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8](</tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8>) — Reachable thread environment action. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` defaultMessage:`Move to main checkout` ``. Byte: 95707. Asset SHA-256: `7b6df941fcb572d046d60a93695af704309b899dce346887bc301484fbef9879`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8](</tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8>) — Cross-host handoff state in local thread UI. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` to-host-worktree `. Byte: 94964. Asset SHA-256: `7b6df941fcb572d046d60a93695af704309b899dce346887bc301484fbef9879`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/thread-handoff-modal-63cd776f0ead.js:1](</tmp/codex-parity-20261005/webview/assets/thread-handoff-modal-63cd776f0ead.js:1>) — Handoff checks destination cleanliness. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` Stash or commit your local changes to hand off `. Byte: 6812. Asset SHA-256: `0a7c11d658f5be9d8faed89fc6059fa04fc1bec396383f1172c55fcbf3ace177`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:137–142](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L137-L142) — Complete mutation inventory lacks move/transfer operation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` allowed `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:218–250](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L218-L250) — Fork/delegate creates new conversation with explicit context handoff rather than moving same task. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` conversation.fork / context_handoff `.

<a id="app-control"></a>

### Agent-callable app toolkit and desktop controls

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex has a real agent-callable app-control server. The remaining differences are registration/discovery ergonomics and the narrower explicit operation inventory, including no matching PR/device/HTML app tools in the inspected host dispatcher.

**Recommendation:** Credit existing control MCP. Reuse its host owners and explicit authority boundary if adding PR/device/HTML operations or friendlier schema discovery; manual opt-in registration is not a missing API.


**Current Memex:** The separate opt-in memex-control stdio MCP registers a generic control(method, params) tool forwarding to the execution host. It supports conversation lifecycle/control/queue/fork/delegate, schedules, workspace/worktree operations and granted browser actions; ordinary memex mcp remains retrieval-only. Remaining differences are registration/discovery ergonomics and narrower explicit operations, including desktop sidebar/panel controls and T3 PR/device/HTML tools.

**T3-W11 — T3:** Authenticated MCP /mcp mounts orchestrator/thread/project/environment/attachment/worktree/PR/device/HTML/preview toolkits into server.

**CX-P07 — Installed ChatGPT Codex surface:** Installed Codex source has desktop tool schemas for thread create/read/list/wait/send, sidebar sections and open_in_codex; tools are host/context gated.

**Limits:** Source-only acceptance correction: Rust MCP entry point and forwarding path verified. Parent reports packaging/smoke evidence separately; this worker did not rerun it. Does not claim provider configuration auto-registers the opt-in server. Codex schema presence is source evidence, not proof every tool is exposed in every thread or account. No claim that Pages/Work/ChatGPT-only integrations must be copied into a coding client.

**Evidence:**

- [T3 · apps/server/src/mcp/McpHttpServer.ts:748–766](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/McpHttpServer.ts#L748-L766) — Authenticated transport and registered toolkits. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` layer `.

- [T3 · apps/server/src/server.ts:681–685](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/server.ts#L681-L685) — Server mounts the MCP layer. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` McpHttpServer.layer `.

- [Memex · apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:104–142](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L104-L142) — Declared host capabilities and method dispatch boundary. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` handle `.

- [Memex · apps/macos/Sources/Memex/WorkspaceBrowserExecutionBridge.swift:24–43](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserExecutionBridge.swift#L24-L43) — Browser-specific app bridge dispatch. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` start `.

- [Memex · src/control_mcp.rs:44–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/control_mcp.rs#L44-L74) — Registered agent-callable control MCP forwards method and params to the execution host; includes conversation, worktree, schedules and browser operations. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ControlServer.control `.

- [Memex · src/bin/memex-control.rs:5–26](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/bin/memex-control.rs#L5-L26) — Separate opt-in binary invokes control_mcp::run with optional root. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` main `.

- [Memex · src/control_mcp.rs:78–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/control_mcp.rs#L78-L96) — Advertises tool capability and serves the stdio MCP transport. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ServerHandler / run `.

- [Memex · docs/execution-host.md:92–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/docs/execution-host.md#L92-L96) — Documents opt-in same-user authority and scoped browser describe/dispatch bridge. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Control MCP and browser boundaries `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:4604](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:4604>) — Desktop sidebar tool schemas and siblings are declared in B0s. Version: `26.930.51102 (13100)`. Anchor: `` b0s=`create_sidebar_section` ``. Byte: 9456423. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:4604](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:4604>) — Desktop thread tools and schema declarations include list/wait/create/read/send. Version: `26.930.51102 (13100)`. Anchor: `` X0s=`list_threads` ``. Byte: 9461411. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3695](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3695>) — Panel opening tool schema supports files, browser, terminal, review and pages; Pages-specific capabilities excluded from recommendation. Version: `26.930.51102 (13100)`. Anchor: `` xda=`open_in_codex` ``. Byte: 5474284. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:137](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L137-L137) — Explicit mutation allowlist shows supported Memex host controls and bounds negative claim. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` let allowed: Set<String> `. Byte: 7162.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:101](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L101-L101) — Capability announcement covers conversations, queues, schedules, workspace/worktree/context fork and browser availability. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` case "host.info": `. Byte: 4823.

- [Memex · /Users/nico/Code/memex-chat-capabilities/src/control_mcp.rs:44–66](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/control_mcp.rs#L44-L66) — Opt-in MCP forwards method+params to execution_host::request; existing tool describes full execution controls. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` async fn control `.

<a id="codex-conversation-02"></a>

### Annotation-aware attachment editing

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Users can supply the underlying text or screenshot, but cannot preserve and revise Codex-style contextual annotations in the composer.

**Recommendation:** Add annotation identity/location only for concrete supported Memex source surfaces; preserve current immutable capture behavior.

**codex-conversation-02 — Codex installed desktop:** Composer renders response annotations and image/diff/browser comment attachment collections with edit/navigation/removal callbacks.

**Memex:** Memex provides text/image/file context and source labels; native composer and attachment APIs do not model response annotation identities or image comment drafts.

**Limits:** Bounded absence: native ConversationComposer, ConversationPromptContext, ConversationAttachment and their public context item mapping. This does not claim no selection capture: terminal/browser selections have existing capture paths.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:116](</tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:116>) — Composer context UI exposes annotation edit and navigation callbacks. Version: `26.930.51102 (13100)`. Anchor: ` onEditResponseTextAnnotation: `. Byte: 585733. Asset SHA-256: `234429db84d9319850ff0766a4a5052a9e898bfa0611883633f80018080dd6c0`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1](</tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1>) — Queued messages retain image comment collections and annotation counts. Version: `26.930.51102 (13100)`. Anchor: ` imageCommentDrafts?.reduce `. Byte: 6030. Asset SHA-256: `6387ca194a268297d5218098906dba8894858dbfdc61a786c9325a0cf7243bed`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:70–80](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L70-L80) — Memex attachment UI maps only title/path/id and removal. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` contextItems / mentionConfiguration `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptContext.swift:10–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptContext.swift#L10-L46) — Native capture boundary stores generic text or image blocks, not editable annotation structures. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationAttachment.text / image / validate `.

<a id="browser"></a>

### App-owned browser control and action breadth

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Both products expose agent-callable browser control. The narrower Memex action inventory, not absence of a tool bridge, is the gap.

**Recommendation:** Consider first-class navigation/wait/keyboard/recording actions and clearer discovery; preserve opt-in registration and app-owned tab grants.


**Current Memex:** Chat-owned WKWebView tabs expose explicitly granted snapshot, click, type, scroll and optional JavaScript evaluation through the execution host/desktop bridge and opt-in memex-control MCP. Grants can be revoked. First-class navigation, wait, keyboard and recording actions are narrower than T3; this does not establish arbitrary OS-app control.

**T3-W08 — T3:** Preview panel plus MCP open/navigate/resize/snapshot/click/type/press/scroll/evaluate/wait/recording tools.

**codex-workspace-07 — Installed ChatGPT Codex desktop:** Browser settings and native computer/browser bridge are shipped; policy/gate checks exist.

**Limits:** Does not establish general OS computer-use parity. T3 device tools are separate device surfaces, not evidence of arbitrary macOS app control. No signed-in browser interaction was performed; actual provider invocation depends on execution bridge and grant.

**Evidence:**

- [T3 · apps/web/src/components/ChatView.tsx:10588–10600](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10588-L10600) — Browser preview mounts in chat. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` PreviewPanel `.

- [T3 · apps/server/src/mcp/toolkits/preview/tools.ts:67–91](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/toolkits/preview/tools.ts#L67-L91) — Agent navigation controls. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` preview_open / preview_navigate `.

- [T3 · apps/server/src/mcp/toolkits/preview/tools.ts:201–244](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/toolkits/preview/tools.ts#L201-L244) — Evaluate, wait and recording tools. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` preview tools `.

- [Memex · apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:68–100](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift#L68-L100) — Grant and tab/action validation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` allow / dispatch `.

- [Memex · apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:5–6](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift#L5-L6) — Five supported action cases. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceBrowserCapability `.

- [Memex · apps/macos/Sources/Memex/MemexApp.swift:78–81](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L78-L81) — Starts browser execution bridge. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` applicationDidFinishLaunching `.

- [Memex · src/control_mcp.rs:44–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/control_mcp.rs#L44-L74) — Registered agent-callable control MCP forwards method and params to the execution host; includes conversation, worktree, schedules and browser operations. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ControlServer.control `.

- [Memex · src/bin/memex-control.rs:5–26](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/bin/memex-control.rs#L5-L26) — Separate opt-in binary invokes control_mcp::run with optional root. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` main `.

- [Memex · docs/execution-host.md:92–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/docs/execution-host.md#L92-L96) — Documents opt-in same-user authority and scoped browser describe/dispatch bridge. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Control MCP and browser boundaries `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/browser-use-settings-visibility-a47f5656ea55.js:1](</tmp/codex-parity-20261005/webview/assets/browser-use-settings-visibility-a47f5656ea55.js:1>) — Browser settings visibility is gated. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` enabled:p,isLoading:m `. Byte: 1604. Asset SHA-256: `f38ecd8dfd51b87e71871ce375dffe4c6ff4179a2e79ba53af666f677105f673`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserTabView.swift:15–44](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserTabView.swift#L15-L44) — Tabs plus explicit agent automation grant/revoke UI. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceBrowserTabView `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:85–124](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift#L85-L124) — Dispatches allowed snapshot/click/type/scroll/evaluate operations. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` dispatch `.

<a id="codex-workspace-02"></a>

### Automatic pruning with recoverable snapshots

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex protects dirty work by retaining disk usage; it does not implement the compared snapshot-and-prune lifecycle.

**Recommendation:** Consider explicit snapshot recovery before optional pruning. Preserve ignored-file safety guarantees.

**codex-workspace-02 — Installed ChatGPT Codex desktop:** Worktree settings expose automatic deletion, retention limit and snapshot-before-delete recovery; actual native snapshot implementation not in extracted renderer.

**Memex:** Archive retains all files. Cleanup refuses dirty, untracked or ignored files; reattach recreates from retained branch.

**Limits:** Codex snapshot coverage and recovery correctness are UI/native-contract evidence, not runtime verification. This is an optional disk-management enhancement, not a data-loss defect.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/worktrees-settings-page-21ceeff5dac4.js:2](</tmp/codex-parity-20261005/webview/assets/worktrees-settings-page-21ceeff5dac4.js:2>) — Installed UI describes recoverable snapshot-before-prune semantics. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` ChatGPT snapshots worktrees before deleting `. Byte: 11198. Asset SHA-256: `6fd750ee851b7b6dec7923506dd007f16f21dc3e2a3f56b53a4f3fb8835c7a2f`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/worktrees-settings-page-21ceeff5dac4.js:2](</tmp/codex-parity-20261005/webview/assets/worktrees-settings-page-21ceeff5dac4.js:2>) — Delete handler calls native worktree-delete after archiving linked conversations. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` await ne(`worktree-delete` ``. Byte: 17748. Asset SHA-256: `6fd750ee851b7b6dec7923506dd007f16f21dc3e2a3f56b53a4f3fb8835c7a2f`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceLifecycle.swift:76–121](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceLifecycle.swift#L76-L121) — Archive only changes lifecycle metadata; cleanup requires empty status; reattach uses git worktree add from branch. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` setArchived / removeCleanCheckout / reattach `.

<a id="codex-conversation-07"></a>

### Editable plan document workflow

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Planning exists, but document-oriented planning and direct editing are not equivalent to the step summary.

**Recommendation:** Preserve full plan identity/text and add an editable plan view with save/implementation provenance.

**codex-conversation-07 — Codex installed desktop:** Completed plan text can open an editable PLAN.md under the host Codex home; saves existing edits and falls back to a read-only panel when file writing is unavailable.

**Memex:** Memex projects structured plan steps from update_plan/TodoWrite/native plan arrays and offers Refine/Implement/Implement in new conversation. Markdown plan text can be shown as tool detail, but does not become an editable plan document.

**Limits:** Editable Codex plan requires workspaceFiles.write and a supported host. Do not claim the existing Memex planning mode or implementation actions are missing. Acceptance review: categorized as a workflow enhancement (P2), not a correctness defect; existing Memex structured planning and generic file editor remain present.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-plan-model-5c6fbe48a2a4.js:1](</tmp/codex-parity-20261005/webview/assets/local-conversation-plan-model-5c6fbe48a2a4.js:1>) — Completed plan text is selected from conversation items. Version: `26.930.51102 (13100)`. Anchor: `` r.type!==`plan` ``. Byte: 273. Asset SHA-256: `298cb8445882d01a2af8cd607daa7a0d211e99b933b6df049619c7cf29d46386`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8](</tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8>) — Thread summary opens selected plan. Version: `26.930.51102 (13100)`. Anchor: ` Pv(t,{content:r.content `. Byte: 149357. Asset SHA-256: `7b6df941fcb572d046d60a93695af704309b899dce346887bc301484fbef9879`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/plan-side-panel-2f427e3e9737.js:1](</tmp/codex-parity-20261005/webview/assets/plan-side-panel-2f427e3e9737.js:1>) — Plan panel creates/opens editable file with conflict and save handling; fallback read-only. Version: `26.930.51102 (13100)`. Anchor: `` m=l(p,`PLAN.md`) ``. Byte: 1033. Asset SHA-256: `c405475a794ae0649663be62ccbaafa7c55326f9c16db27b1fa5485addc2c030`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:27–62](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L27-L62) — Only structured steps become the plan summary. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationWork.project `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:117–136](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L117-L136) — Refine/Implement buttons attach step text to a draft. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationWorkView `.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2405–2408](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2405>) — Markdown text is retained in details; structured payload is derived from arrays. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` case "plan" `.

<a id="t3-i02"></a>

### Files, pasted images, and media ergonomics

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Basic attachment parity exists; large screenshots and phone-photo files can require manual conversion/resizing in Memex.

**Recommendation:** Add a bounded image-normalization path only if these common inputs are a product priority; preserve captured-byte durability.

**T3-I02 — T3:** Image/generic-file classification, HEIC-to-JPEG conversion, video preview eligibility and server capability/size validation are implemented.

**Memex:** Explicit file panel and paste/drop capture are implemented; runtime supports capability-checked images/audio/file contents. Images are limited to PNG/JPEG/GIF/WebP and 3 MiB each, with ten attachments and 20 MiB total. File-URL HEIC receives no conversion before validation. Clipboard TIFF/PNG/JPEG bytes are converted to PNG.

**Limits:** T3 accepting a generic video file is not proof every model can understand video. Attachment capability remains provider/model-specific.

**Evidence:**

- [T3 · apps/web/src/components/chat/composerAttachmentFiles.ts:65–136](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/composerAttachmentFiles.ts#L65-L136) — Classifies HEIC, generic files, previewable video; blocks unsupported server upload state. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` classifyComposerAttachmentFile/fileAttachmentCapabilityBlockReason `.

- [T3 · apps/web/src/lib/imageCompression.ts:482–507](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/lib/imageCompression.ts#L482-L507) — Converts HEIC/HEIF to compatible JPEG. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` HEIC conversion `.

- [T3 · apps/web/src/components/chat/composerAttachmentFiles.test.ts:45–60](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/composerAttachmentFiles.test.ts#L45-L60) — Existing tests cover image classification. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` HEIC and unsupported image tests `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:85–92](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L85-L92) — Follow-up composer wires clipboard/drop capture. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` paste/drop modifiers `.

- [Memex · apps/macos/Sources/Memex/ConversationAttachment.swift:9–20](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationAttachment.swift#L9-L20) — File attachments go directly through runtime loadFile and validation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` capture `.

- [Memex · apps/macos/Sources/Memex/ConversationPromptContext.swift:80–107](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptContext.swift#L80-L107) — File URL branch loads original bytes; bitmap paste converts to PNG. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationClipboard.capture `.

- [Memex runtime · packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:62–95](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:62>) — Original file bytes and runtime limits. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` loadFile/constants `.

- [Memex runtime · packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:115–151](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:115>) — Provider capability gates, image MIME validation and byte limits. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` validate `.

<a id="pr-review"></a>

### Inline GitHub PR review and diff controls

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex already handles local Git, but its inspected native workflow ends at an external PR URL.

**Recommendation:** If PR review parity is desired, add a first-class PR detail/review surface rather than duplicating local Git controls.


**Current Memex:** Local review supports current, branch and checkpoint comparisons plus file-level comment context added to chat. Git controls stage, commit, push and create a draft GitHub PR through gh, then open the PR externally. The native surface lacks the compared inline PR detail/review-thread workflow and richer diff controls.

**T3-W02 — T3:** Git workflow controls plus an environment-scoped PullRequestDetailPanel inside the chat.

**codex-workspace-04 — Installed ChatGPT Codex desktop:** PR renderer has reply/resolved threads and diff options: hide whitespace/imports, word diffs and full-file loading.

**Limits:** Source inspection only; no runtime or test execution. No GitHub write action was performed. Provider shell/MCP access can offer PR operations without native product UI. Native workspace UI boundary, not a claim that all provider tools lack GitHub.

**Evidence:**

- [T3 · apps/web/src/components/ChatView.tsx:10621–10665](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10621-L10665) — Diff and PR detail surfaces are mounted from selected panel. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` rightPanelContent `.

- [T3 · apps/web/src/components/GitActionsControl.tsx:1730–1748](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/GitActionsControl.tsx#L1730-L1748) — Git initialization and quick action UI. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` GitActionsControl `.

- [Memex · apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:19–28](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift#L19-L28) — Complete native Git menu inventory. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Memex · apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:84–100](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift#L84-L100) — Draft PR success opens its URL externally. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` execute `.

- [Memex · apps/macos/Sources/Memex/WorkspaceChangesView.swift:63–80](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceChangesView.swift#L63-L80) — Current and branch comparison plus Git actions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Memex · apps/macos/Sources/Memex/WorkspaceChangesView.swift:149–154](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceChangesView.swift#L149-L154) — Adds review context to chat. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/pull-request-code-review-28722845bd32.js:2](</tmp/codex-parity-20261005/webview/assets/pull-request-code-review-28722845bd32.js:2>) — PR review thread reply UI. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` defaultMessage:`Reply` ``. Byte: 47285. Asset SHA-256: `a01870c72d7c38425602f42ee236bbe4197c1799b3af224868eb1dd4be2e7de2`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/pull-request-code-review-28722845bd32.js:1](</tmp/codex-parity-20261005/webview/assets/pull-request-code-review-28722845bd32.js:1>) — PR diff filtering controls. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` defaultMessage:`Hide white space` ``. Byte: 26014. Asset SHA-256: `a01870c72d7c38425602f42ee236bbe4197c1799b3af224868eb1dd4be2e7de2`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceReview.swift:3–26](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceReview.swift#L3-L26) — Local scopes and review context contract. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceDiffScope / WorkspaceReviewContext `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceChangesView.swift:149–156](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceChangesView.swift#L149-L156) — Review comment adds local diff context to the active chat. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Add review to chat `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:19–28](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift#L19-L28) — Git action boundary includes create PR but no open/reply PR workflow. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Action `.

<a id="codex-workspace-08"></a>

### Native apps and external browser control

**Memex: partial · P2 · qualified confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

The native Memex control surface stops at its own browser tabs.

**Recommendation:** Treat broader desktop control as an explicitly permissioned integration, if desired.

**codex-workspace-08 — Installed ChatGPT Codex desktop:** Shipped Computer use settings include Any App, Chrome, Edge, Safari and per-app approvals; native settings bridge and policy checks are present.

**Memex:** Inspected desktop execution API exposes browser.describe/browser.dispatch into chat-owned tabs; grant text explicitly excludes other apps and conversations.

**Limits:** Codex feature visibility is policy/plugin/platform dependent; no live enablement asserted. External provider-installed MCP tools are outside the native inventory boundary. Runtime generated computer_use protocol types alone do not implement this UI.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/computer-use-settings-8cfbd3f42052.js:1](</tmp/codex-parity-20261005/webview/assets/computer-use-settings-8cfbd3f42052.js:1>) — Native/external app computer-use settings. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` defaultMessage:`Any App` ``. Byte: 4479. Asset SHA-256: `7de8551f6908a9c69a072029f909ffcee53bd514edc53e4221bc48fd520be2f4`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/computer-use-app-approvals-query-a8b3c61a321c.js:1](</tmp/codex-parity-20261005/webview/assets/computer-use-app-approvals-query-a8b3c61a321c.js:1>) — Native bridge reads app approval policy. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` n.computerUseSettings.getAppApprovals() `. Byte: 1550. Asset SHA-256: `b52aee0f3bf5cfca455af0c5dcd4a114a036b492ae3c21cb9c01580b2152d86e`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/computer-use-app-approvals-query-a8b3c61a321c.js:1](</tmp/codex-parity-20261005/webview/assets/computer-use-app-approvals-query-a8b3c61a321c.js:1>) — Computer-use requirements restrict locked-computer capability. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` allowLockedComputerUse!==!1 `. Byte: 580. Asset SHA-256: `b52aee0f3bf5cfca455af0c5dcd4a114a036b492ae3c21cb9c01580b2152d86e`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserTabView.swift:35–44](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserTabView.swift#L35-L44) — Control is explicitly limited to app-owned tabs. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` automation grant message `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:137–142](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L137-L142) — Native execution mutation boundary exposes browser.dispatch, not desktop-app control. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` allowed `.

<a id="cx-p03"></a>

### Organize conversations with custom sections and read/unread state

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex covers the baseline organization workflow but cannot form cross-project custom sections or manually triage unread chats.

**Recommendation:** Extend the existing library metadata and sidebar rather than replacing organization.

**CX-P03 — Installed ChatGPT Codex surface:** Codex sidebar custom-section create/move and local-thread read/unread actions are wired; command palette includes markThreadUnread.

**Memex:** Memex has pinned, archive, removed/restore, rename, manual reorder and project grouping. Its persistent Entry schema and sidebar actions have no custom section or read/unread state.

**Limits:** Sidebar snooze is intentionally excluded.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3788](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3788>) — Custom-section submenu maps selected items to section membership. Version: `26.930.51102 (13100)`. Anchor: `` id:`move-to-custom-section` ``. Byte: 6913254. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3939](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3939>) — Local-thread read/unread action dispatches conversationId/hostId, not just label text. Version: `26.930.51102 (13100)`. Anchor: `` case`mark-thread-read`:case`mark-thread-unread`:return ``. Byte: 6989310. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationLibrary.swift:21](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationLibrary.swift#L21-L21) — Persistent library Entry stores session/title/pinned/archived/removed/order only. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` struct Entry: Codable `. Byte: 592.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/SidebarConversations.swift:206](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/SidebarConversations.swift#L206-L206) — Native management actions expose rename/pin/archive/remove/restore; no custom-section/read-state action. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` private func managementActions `. Byte: 10424.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/SidebarConversations.swift:15](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/SidebarConversations.swift#L15-L15) — Project grouping and sort already implemented. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` var sidebarGroups: `. Byte: 380.

<a id="codex-conversation-12"></a>

### Policy amendment and reviewer detail UI

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Approval works, but users do not get the same scoped rule controls or review explanation UI.

**Recommendation:** Project only supported typed decisions and review events; keep unknown data inspectable.

**codex-conversation-12 — Codex installed desktop:** Approval panel exposes exec/network policy amendments; automatic-review details show denied/high-risk rationale and review state.

**Memex:** Generic Memex approval panel supports allow once/session/deny and raw request detail. Codex adapter filters command decisions to string accept/acceptForSession, and its notification switch lacks dedicated guardian review start/completion projection.

**Limits:** Memex already configures auto_review for noninteractive approval policy. The gap is specialized decision/presentation, not lack of auto-review backend selection.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1](</tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1>) — Native network policy amendment action. Version: `26.930.51102 (13100)`. Anchor: ` applyNetworkPolicyAmendment `. Byte: 78402. Asset SHA-256: `ebeba4e61951e938bf514daa1ec024a85854ce938929b4766e61cb51cc37fe10`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1](</tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1>) — Exec amendment is part of specialized approval UI. Version: `26.930.51102 (13100)`. Anchor: ` proposedExecpolicyAmendment `. Byte: 74218. Asset SHA-256: `ebeba4e61951e938bf514daa1ec024a85854ce938929b4766e61cb51cc37fe10`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/automatic-approval-review-details-4ade28f5c96a.js:1](</tmp/codex-parity-20261005/webview/assets/automatic-approval-review-details-4ade28f5c96a.js:1>) — Review detail presentation distinguishes high risk denials. Version: `26.930.51102 (13100)`. Anchor: ` Requires explicit authorization because this action is considered high risk `. Byte: 3836. Asset SHA-256: `47ab4b167125e6c391493c60ce092d6ebe79946471b43b0f20c72c42855667fa`.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2613–2642](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2613>) — Only accept, session, decline/cancel decisions are surfaced/serialized. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` approvalOptions / commandDecision `.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2023–2050](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2023>) — Generic guardianWarning is handled; dedicated review lifecycle is not projected here. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` notification switch / default `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProjection.swift:21–28](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L21-L28) — UI projection carries generic title, raw detail and options. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` approval projection `.

<a id="t3-i12"></a>

### Provider catalog, model/effort and permission configuration

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Provider breadth remains a real gap, but describing Memex as Codex/Claude-only is also incomplete.

**Recommendation:** Add dedicated adapters only for demonstrated workflow demand; retain negotiated ACP capability boundaries and explicit installation identity.

**T3-I12 — T3:** T3 ships eight driver types: Codex, Claude, Cursor, Grok, OpenCode, Antigravity, Pi and ACP Registry. Model picker is instance-aware; traits and runtime modes are provider-filtered.

**Memex:** Memex has Codex and Claude native integrations plus explicitly configured ACP executables/homes/arguments. It discovers model and nonempty configuration options, preserves unknown inherited values, remembers explicit provider-scoped choices, and waits for acknowledgement. It does not ship the remaining dedicated T3 driver integrations merely because it ingests their history.

**Limits:** Driver presence is not proof that the required binary/account is configured or that every provider supports every interaction. Existing chat model/effort changes are blocked while working in Memex; settings are applied when idle.

**Evidence:**

- [T3 · apps/server/src/provider/builtInDrivers.ts:52–62](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/provider/builtInDrivers.ts#L52-L62) — Authoritative shipped driver list. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` BUILT_IN_DRIVERS `.

- [T3 · apps/web/src/components/chat/ProviderModelPicker.tsx:75–96](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ProviderModelPicker.tsx#L75-L96) — Account/instance-aware selected model state. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` active instance model label `.

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:2131–2155](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2131-L2155) — Provider-supported mode filtering and model state. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` provider modes/models `.

- [Memex · apps/macos/Sources/Memex/ConversationProviderCatalog.swift:28–67](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProviderCatalog.swift#L28-L67) — Configured ACP and two native built-ins. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` descriptors/catalog `.

- [Memex · apps/macos/Sources/Memex/InAppAgentRuntime.swift:124–135](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L124-L135) — Actual configured ACP/Codex/Claude connection paths. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` connect dispatch `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:139–182](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L139-L182) — Reported model/config selectors and inherited defaults. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` controls `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:540–569](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L540-L569) — Observed setting acknowledgement and timeout. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` applySettings/waitForSettings `.

- [Memex · apps/macos/Tests/MemexTests/ConversationProviderCatalogTests.swift:6–55](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationProviderCatalogTests.swift#L6-L55) — Executable-vs-ingestion identity and resume capability tests. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` provider catalog tests `.

<a id="t3-i05"></a>

### Provider-specific steering fallback

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

T3 covers more active-instruction strategies; Memex does not promise generic ACP steering merely because ACP is configurable.

**Recommendation:** Consider explicit interrupt-and-restart as a separately labeled future capability; never silently equate it to native steering.

**T3-I05 — T3:** T3 policy chooses native active steering or interrupt/restart when explicitly supported. Common ACP adapter advertises interrupt/restart and app queues; Cursor also uses restart, while Pi and Claude advertise native steering without generic restart.

**Memex:** Memex native Codex/Claude advertise steering. Configured ACP descriptors explicitly mark steering unavailable; UI gates on observed actions. Unsupported steer leaves draft and queue intact.

**Limits:** Adapter declarations plus orchestration policy establish implementation intent, not live compatibility with every installed ACP agent version.

**Evidence:**

- [T3 · apps/server/src/orchestration-v2/CommandPolicy.ts:238–269](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/CommandPolicy.ts#L238-L269) — Capability-checked native versus restart policy. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` decideSteeringExecution `.

- [T3 · apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.ts:553–565](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.ts#L553-L565) — Common ACP interrupt/restart and app queue capabilities. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` ACP capabilities `.

- [T3 · apps/server/src/orchestration-v2/Adapters/CursorAdapterV2.ts:102–109](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/Adapters/CursorAdapterV2.ts#L102-L109) — No native steering; restart support. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` Cursor capabilities `.

- [Memex · apps/macos/Sources/Memex/ConversationProviderCatalog.swift:28–32](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProviderCatalog.swift#L28-L32) — Configured ACP steering unavailable. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` configured descriptor `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:214–218](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L214-L218) — Observed capability and interaction gates. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` canSteer `.

- [Memex · apps/macos/Sources/Memex/InAppAgentRuntime.swift:207–213](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L207-L213) — Steer comes from actual available action. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` snapshot capabilities `.

- [Memex · apps/macos/Tests/MemexTests/ConversationQueueTests.swift:174–191](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L174-L191) — Existing no-loss unsupported-steer test. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` unsupportedSteerLeavesDraftAndQueueUnchanged `.

<a id="schedules"></a>

### Recurring work, new-chat targets and run triage

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Do not classify automation as absent. Event triggers and fresh-chat-per-run are the inspected gaps.

**Recommendation:** Extend the durable host scheduler only if event-driven/fresh-task workflows are required; keep stopped queue semantics.


**Current Memex:** The durable execution-host scheduler targets existing hosted conversations with interval or weekday/local-time/timezone recurrence, edit/pause/run/delete controls and held-queue recovery. The inspected model/UI lacks fresh-chat-per-run targets, T3-style webhook triggers, and Codex-style run inbox/per-schedule notification policy.

**T3-W10 — T3:** Reachable settings supports scheduled/webhook tasks; dispatch either launches a new thread or queues an existing one.

**CX-P02 — Installed ChatGPT Codex surface:** Codex automation source supports heartbeat linked to a thread, cron project targets, run-now/pause, run history and per-automation notification policy.

**Limits:** Source inspection only; no runtime or test execution. Codex cloud automation and ChatGPT scheduled tasks are excluded from the parity claim. Heartbeat naming alone is not a gap: Memex can recur on an existing conversation.

**Evidence:**

- [T3 · apps/web/src/routes/settings.scheduled-tasks.tsx:1–8](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/routes/settings.scheduled-tasks.tsx#L1-L8) — Mounted scheduled task settings. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` route `.

- [T3 · apps/server/src/scheduledTasks/ScheduledTaskService.ts:238–252](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/scheduledTasks/ScheduledTaskService.ts#L238-L252) — Webhook rotation and verification/dispatch API. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` service interface `.

- [T3 · apps/server/src/scheduledTasks/ScheduledTaskService.ts:795–830](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/scheduledTasks/ScheduledTaskService.ts#L795-L830) — New-thread or existing-thread queued prompt path. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` dispatch `.

- [Memex · apps/macos/Sources/Memex/MemexApp.swift:35](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L35-L35) — Execution Hosts and Schedules entry. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` menu `.

- [Memex · apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:114–174](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L114-L174) — Interval/wall-clock editor and actions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Schedules `.

- [Memex · apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:328–376](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L328-L376) — Existing conversation schedule model and actions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` schedule mutations `.

- [Memex · apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:383–397](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L383-L397) — Recurrence and skipped wall-clock handling. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` tick `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/automations-page-abd560996342.js:1](</tmp/codex-parity-20261005/webview/assets/automations-page-abd560996342.js:1>) — Automation page handles thread-linked heartbeats distinctly; later create/run-now dispatches and history actions are wired. Version: `26.930.51102 (13100)`. Anchor: `` s.kind===`heartbeat` ``. Byte: 18702. Asset SHA-256: `31422790868ee7a46d97712dc7024aa7c5c9c048340cda21cb44a81f4bd84c20`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/automation-detail-panel-59709a145853.js:1](</tmp/codex-parity-20261005/webview/assets/automation-detail-panel-59709a145853.js:1>) — Draft editor exposes per-automation notification policy including failed_runs_only; kind heartbeat and model/reasoning are separate draft fields. Version: `26.930.51102 (13100)`. Anchor: ` notificationPolicy `. Byte: 9307. Asset SHA-256: `f247b714c9cc579db80bdf423cf340b0b94c6da5a8041c4b7160b3856e707d62`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/automations-page-abd560996342.js:1](</tmp/codex-parity-20261005/webview/assets/automations-page-abd560996342.js:1>) — Cron-specific target/project and template state separated from heartbeat. Version: `26.930.51102 (13100)`. Anchor: ` t.pluginTemplateId:null,target:r.projectI `. Byte: 62433. Asset SHA-256: `31422790868ee7a46d97712dc7024aa7c5c9c048340cda21cb44a81f4bd84c20`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:114](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L114-L114) — Reachable host sheet supports schedules and explicit recurrence, run, edit, pause and delete controls. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Section("Schedules") `. Byte: 6773.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:328](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L328-L328) — Schedule schema validates conversation ID and interval/wall-clock recurrence; saves metadata. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` case "schedule.upsert": `. Byte: 21389.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:422](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L422-L422) — Runs enqueue prompt into same schedule.conversationID; no new-thread target or per-run inbox target in this path. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` private func enqueueSchedule `. Byte: 27485.

<a id="codex-workspace-09"></a>

### Remote execution and integrated SSH onboarding

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Multi-host execution already exists; the difference is integrated SSH/account discovery and setup ergonomics.

**Recommendation:** Improve onboarding if required without replacing the execution-host ownership protocol.

**codex-workspace-09 — Installed ChatGPT Codex desktop:** Connections UI includes manual SSH host/port/identity and remote CLI sign-in; account-device connection paths are also bundled.

**Memex:** Paired execution hosts are implemented via explicit HTTPS or loopback-tunnel URL plus token; verify host identity, list/create/resume conversations and manage remote worktrees.

**Limits:** Codex device discovery availability is gate/account dependent; no connectivity test performed.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/remote-connection-editor-dialog-1d8f37e9e898.js:1](</tmp/codex-parity-20261005/webview/assets/remote-connection-editor-dialog-1d8f37e9e898.js:1>) — SSH configuration form. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` defaultMessage:`SSH authentication method` ``. Byte: 8085. Asset SHA-256: `c86f8821b9c18069ee8bdde46a378b6ea24d2df469007a70666fa7260ab1390d`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/remote-connections-settings-d2dabff157cf.js:1](</tmp/codex-parity-20261005/webview/assets/remote-connections-settings-d2dabff157cf.js:1>) — Remote CLI authentication workflow. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` Authenticate the Codex CLI on this remote machine to continue `. Byte: 36063. Asset SHA-256: `68026dcc3268fdc3aca4c70286f3b28a1a2fc13836c9789e91fcceadb6417a66`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:42–77](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L42-L77) — Explicit endpoint/token pairing workflow. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Execution hosts `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnection.swift:67–79](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnection.swift#L67-L79) — HTTPS or loopback HTTP endpoint validation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` validatedEndpoint `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RemoteConversationRuntime.swift:20–52](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/RemoteConversationRuntime.swift#L20-L52) — Imports/resumes exact host-native conversation identity. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` connect `.

<a id="keyboard-settings"></a>

### Shortcut customization and appearance controls

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Keyboard customization is a real feature difference, independent of rendering performance.

**Recommendation:** Consider customization if fixed shortcuts conflict with user workflows.


**Current Memex:** Memex has fixed native command/composer shortcuts and accessibility labels, but no app-level keymap editor or comparable appearance-preference surface; the Settings scene is empty. Native system behavior remains available, and broader accessibility quality is unmeasured.

**T3-TR-10 — T3:** Editable keybindings with recording/conflict handling; browser warns when native browser shortcuts intercept.

**CX-P04 — Installed ChatGPT Codex surface:** Codex keyboard editor captures/replaces/removes bindings with conflict checks; appearance settings expose fonts, UI/code sizing, contrast and reduced motion.

**Limits:** Source inspection only; tests mentioned were read, not executed. Appearance controls may inherit OS settings; app-specific overrides are the bounded comparison. No VoiceOver, reduced-motion, keyboard traversal, responsiveness or memory benchmark was run.

**Evidence:**

- [T3 · apps/web/src/components/settings/KeybindingsSettings.tsx:751–778](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/KeybindingsSettings.tsx#L751-L778) — Conflict checks and recording Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` keybinding editor `.

- [T3 · apps/web/src/components/settings/KeybindingsSettings.tsx:1314–1322](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/KeybindingsSettings.tsx#L1314-L1322) — Browser may intercept shortcuts Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` browser warning `.

- [Memex · apps/macos/Sources/Memex/MemexApp.swift:12–33](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L12-L33) — Statically declared native keyboard shortcuts Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` commands `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/use-visible-settings-sections-dc3bbd6366c8.js:1](</tmp/codex-parity-20261005/webview/assets/use-visible-settings-sections-dc3bbd6366c8.js:1>) — Keyboard-shortcuts settings are in Codex applicability map. Version: `26.930.51102 (13100)`. Anchor: `` "keyboard-shortcuts":`codex` ``. Byte: 40105. Asset SHA-256: `cfaa4b3d5c2a82400a350e73d0f2d4a4e86cf805300aa5f1d8e5e4760f8be5a1`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/keyboard-shortcuts-settings-ef4c455aeec6.js:1](</tmp/codex-parity-20261005/webview/assets/keyboard-shortcuts-settings-ef4c455aeec6.js:1>) — Editor capture persists mutation through B; neighboring handlers remove/reset and conflict validation. Version: `26.930.51102 (13100)`. Anchor: ` onCapture:t=>{B(t,e)} `. Byte: 9255. Asset SHA-256: `f31de7f3b3c6b2449870be9f0fbc8259cd5f494d06105b619a26560976892328`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/general-settings-4f1402fc1fbd.js:1](</tmp/codex-parity-20261005/webview/assets/general-settings-4f1402fc1fbd.js:1>) — Appearance controls include reduced motion, fonts and UI/code sizes; source presence only for unobserved controls. Version: `26.930.51102 (13100)`. Anchor: ` settings.general.appearance.reducedMotion.label `. Byte: 36857. Asset SHA-256: `91ef510e7631785df4c62c25f3b9018a0ca3c15ce3fa1b208f3cf8394abdf535`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:9](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L9-L9) — No native settings preference editor. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Settings { EmptyView() } `. Byte: 181.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:32](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L32-L32) — Application commands have fixed shortcuts including Cmd-J terminal. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` .keyboardShortcut("j") `. Byte: 1586.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/SidebarConversations.swift:306](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/SidebarConversations.swift#L306-L306) — Native row accessibility implemented; no accessibility quality deficit inferred. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` .accessibilityElement(children: .combine) `. Byte: 15906.

<a id="questions"></a>

### Structured questions and media answers

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Multi-question UX is present; media answers and asynchronous dismissal are still meaningful semantic differences.

**Recommendation:** Keep text-only labeling accurate. Add media/dismissal only through provider contracts that can actually represent them.


**Current Memex:** Native questions preserve request IDs, choice values, multi-select, descriptions, secret/default fields and per-question drafts with Back/Next navigation. Answers can attach captured UTF-8 files only, up to 10 files/1 MiB. Image/media answers and dismissal without stopping the conversation remain narrower than T3.

**T3-I07 — T3:** T3 supports choice progress, multi-select, keyboard selection, per-question image/file uploads, non-resumable state and dismissible requests; message-mode requests can remain answerable after their turn ends.

**codex-conversation-10 — Codex installed desktop:** Question picker supports selected options, freeform Other, multi-select and secret fields; request panel dispatches userInput.

**Limits:** Question media is a T3 orchestration feature; no universal assertion that every provider accepts equivalent native media replies. Memex question drafts are view-local state, distinct from durable prompt drafts. This does not imply parity for MCP JSON-schema elicitation or Codex async/autoresolved questions; those use different protocols.

**Evidence:**

- [T3 · apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:67–70](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L67-L70) — Message-mode versus non-resumable response state. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` response capability `.

- [T3 · apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:121–169](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L121-L169) — Multi-select and number keys. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` option and keyboard handling `.

- [T3 · apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:203–225](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L203-L225) — Dismissibility is request-dependent. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` dismiss control `.

- [T3 · apps/web/src/components/ChatView.tsx:9783–9816](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9783-L9816) — Per-question uploaded image/file payload collection. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` onRespondToUserInput `.

- [Memex · apps/macos/Sources/Memex/ConversationQuestionsView.swift:21–78](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQuestionsView.swift#L21-L78) — Navigation, structured draft, Stop cancellation and answer encoding. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` questions/submit `.

- [Memex · apps/macos/Sources/Memex/ConversationQuestionsView.swift:105–111](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQuestionsView.swift#L105-L111) — Passes descriptions, multiSelect and secret/default fields. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` question.item `.

- [Memex · apps/macos/Sources/Memex/ConversationQuestionContext.swift:13–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQuestionContext.swift#L13-L46) — Bounded text-only attachment support. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` capture/answerText `.

- [Memex · apps/macos/Tests/MemexTests/ConversationQuestionTests.swift:6–47](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQuestionTests.swift#L6-L47) — Captured bytes, media rejection, stable native identity navigation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` question tests `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1](</tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1>) — Pending-request dispatcher exposes structured questions. Version: `26.930.51102 (13100)`. Anchor: `` case`userInput` ``. Byte: 64099. Asset SHA-256: `ebeba4e61951e938bf514daa1ec024a85854ce938929b4766e61cb51cc37fe10`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:11](</tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:11>) — Question picker handles multi-selection and secret input. Version: `26.930.51102 (13100)`. Anchor: ` isMultiSelect:he,isSecret:ge `. Byte: 478363. Asset SHA-256: `234429db84d9319850ff0766a4a5052a9e898bfa0611883633f80018080dd6c0`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQuestionsView.swift:20–80](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQuestionsView.swift#L20-L80) — Back/Next retains per-question drafts; submission preserves exact choice values and text context. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationQuestionsView `.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1590–1612](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1590>) — Answers accumulate by native question ID until the group can respond. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` respondUserInput `.

<a id="subagents"></a>

### Subagent visibility, detail and navigation

**Memex: partial · P2 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex has live roster presentation but provider child capabilities exceed the reachable native control surface.

**Recommendation:** Expose verified runtime child history/navigation when indexing has not caught up; keep unknown status explicit.


**Current Memex:** Child rows expose identity, prompt, parent and status. Open resolves an already indexed session on the same provider and machine; the runtime child-read API is not wired into this panel. There is no dedicated active/done side panel or per-child model/effort display.

**T3-TR-02 — T3:** Live projected agent status, grouped timing and child-thread navigation.

**codex-conversation-08 — Codex installed desktop:** Dedicated subagent panel splits active/done and opens selected child details including model/reasoning; local thread summary links into it.

**Limits:** Source inspection only; tests mentioned were read, not executed. Codex child interactivity is explicitly canInteract-gated. No claim of universal direct child messaging is made. Memex parent-owned transport can handle child requests.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:3037–3110](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L3037-L3110) — Combines current projection statuses and timing Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` V2SubagentGroup `.

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:5303–5326](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L5303-L5326) — Opens child thread Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` work entry navigation `.

- [Memex · apps/macos/Sources/Memex/ConversationWork.swift:30–93](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L30-L93) — Collects exact IDs/status; unknown remains unknown Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` project `.

- [Memex · apps/macos/Sources/Memex/ConversationWork.swift:133–152](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L133-L152) — Child Open gated on indexed sessions Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationWorkView `.

- [Memex runtime · packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1284–1296](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1284>) — Runtime API exists Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` readChild `.

- [Memex · apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:105–114](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L105-L114) — Does not confuse tool completion with child completion Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` completedSpawnOperationDoesNotClaimChildCompleted `.

- [Memex runtime · packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:1030–1050](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:1030>) — Publishes native child status metadata to root conversation Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` receive `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1](</tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1>) — Active/completed sections in a dedicated panel. Version: `26.930.51102 (13100)`. Anchor: ` localConversation.subagentsPanel.active `. Byte: 2680. Asset SHA-256: `d87313d9ce52d9e65f0d5886c84648b6f59ea8af806a176214d5b8d16a2b4c19`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1](</tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1>) — Child detail header shows model and effort. Version: `26.930.51102 (13100)`. Anchor: ` localConversation.subagentsPanel.modelAndReasoningEffort `. Byte: 4282. Asset SHA-256: `d87313d9ce52d9e65f0d5886c84648b6f59ea8af806a176214d5b8d16a2b4c19`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:17–25](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L17-L25) — Memex child summary data consists of id, prompt, status, parentID. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Agent / State `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:137–163](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L137-L163) — Open/Parent require matching indexed child sessions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ForEach(state.agents) `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/InAppConversationTarget.swift:22–29](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppConversationTarget.swift#L22-L29) — Standalone resume of subagents is disallowed; parent is the control surface. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` unavailableReason `.

<a id="codex-conversation-05"></a>

### Queued message to side chat

**Memex: missing · P3 · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex users must manually move that prompt to a new conversation.

**Recommendation:** If adopting side chats, reuse queue attachment identity and perform a reviewed transfer rather than duplicate dispatch.

**codex-conversation-05 — Codex installed desktop:** Queue action Open in side chat is supplied conditionally by the composer callback.

**Memex:** Queue action inventory supports edit/reorder/steer/send/remove/pause, but has no branch/side-chat action.

**Limits:** Codex gate oc was not resolved to live entitlement; record as gated shipped source, not observed universal availability.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1](</tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1>) — Queue menu optionally starts a queued message as a side chat. Version: `26.930.51102 (13100)`. Anchor: ` Open in side chat `. Byte: 14924. Asset SHA-256: `6387ca194a268297d5218098906dba8894858dbfdc61a786c9325a0cf7243bed`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:169](</tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:169>) — Caller gates this action; it is not universal. Version: `26.930.51102 (13100)`. Anchor: ` onOpenInSideChatMessage:oc?xl:void 0 `. Byte: 1387971. Asset SHA-256: `234429db84d9319850ff0766a4a5052a9e898bfa0611883633f80018080dd6c0`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQueueView.swift:28–54](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQueueView.swift#L28-L54) — Complete native queue menu has no side-chat operation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Menu `.

<a id="t3-tr-13"></a>

### Accessibility hooks

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Both implement observable accessibility hooks; source does not establish comprehensive accessibility.

**Recommendation:** Keep these hooks when changing presentation; audit with assistive technology for claims beyond source presence.

**T3-TR-13 — T3:** ARIA labels/expanded state and reduced-motion-aware following.

**Memex:** Native copy/disclosure accessibility labels and expanded state.

**Limits:** No screen-reader, keyboard-only usability or compliance audit performed.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:1356–1372](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1356-L1372) — Reduced-motion scrolling Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` MessagesTimeline `.

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:3070–3075](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L3070-L3075) — ARIA label/description Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` subagent disclosure `.

- [Memex · apps/macos/Sources/Memex/TranscriptController.swift:874–921](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptController.swift#L874-L921) — Copy/disclosure labels and values Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` row accessibility `.

<a id="t3-w03"></a>

### Checkpoint and conversation rewind

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Both have rewind, with provider and checkout constraints.

**Recommendation:** Retain separate file/context intent and provider capability gating; do not infer all providers support rollback.

**T3-W03 — T3:** Timeline revert invokes provider rollback then optional checkpoint restore; provider capability errors are explicit.

**Memex:** Workspace pane wires checkpoint controls and conversation rewind; restore protects managed ownership and captures recovery.

**Limits:** Source inspection only; no runtime or test execution.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:2285–2287](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2285-L2287) — Timeline revert entry point. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` RevertUserMessageButton `.

- [T3 · apps/server/src/orchestration-v2/CheckpointRollbackService.ts:257–265](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/CheckpointRollbackService.ts#L257-L265) — Provider rollback followed by optional file restore. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` execute `.

- [Memex · apps/macos/Sources/Memex/WorkspacePanelView.swift:18–25](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L18-L25) — Rewind handler supplied to workspace changes. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Memex · apps/macos/Sources/Memex/WorkspaceCheckpoints.swift:69–101](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceCheckpoints.swift#L69-L101) — Isolation validation, recovery snapshot and index restore. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` restore `.

<a id="cx-p06"></a>

### Choose models, reasoning and provider-specific permissions

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Basic model and reasoning selection is present with provider-specific limits; do not treat provider history import as executable provider support.

**Recommendation:** Preserve negotiated capability boundaries and distinguish native Codex from ACP transport behavior.

**CX-P06 — Installed ChatGPT Codex surface:** Codex model picker derives choices from supportedReasoningEfforts and handles configured/custom model providers and hidden/gated models.

**Memex:** Memex supports native Codex, Claude Code and explicitly configured ACP executables. Model/configuration controls are negotiated and applied through the runtime; ACP resume/configuration are negotiated and native steer unavailable.

**Limits:** Exact available models and authentication state were not queried.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1679](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1679>) — Model/effort menu derives choices from runtime supportedReasoningEfforts. Version: `26.930.51102 (13100)`. Anchor: ` function V9r(e `. Byte: 4095535. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1679](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1679>) — Picker handles custom provider catalog flags; no claim that Codex is restricted to OpenAI-compatible defaults. Version: `26.930.51102 (13100)`. Anchor: ` isCustomModelProvider:s=!1 `. Byte: 4116669. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProviderCatalog.swift:62](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProviderCatalog.swift#L62-L62) — Codex/Claude builtin descriptors; configured ACP catalogs appended. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` static let builtins: `. Byte: 2865.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProviderSetupView.swift:17](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProviderSetupView.swift#L17-L17) — Native view adds/removes ACP executables with explicit home identity and negotiated capability explanation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Text("Conversation providers") `. Byte: 600.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:149](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L149-L149) — Composer exposes negotiated configuration fields with nonempty choices. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ForEach(settings.configurations.filter `. Byte: 8581.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationControls.swift:37](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationControls.swift#L37-L37) — Reasoning is identified from thought_level/reasoning_effort fields. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` var reasoning: Configuration? `. Byte: 1171.

<a id="codex-workspace-03"></a>

### Commit, push, draft PR and local diffs

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Core Git workflow is present; lack of Git support is an incorrect gap claim.

**Recommendation:** Keep focused improvements scoped to concrete review or branch UX differences.

**codex-workspace-03 — Installed ChatGPT Codex desktop:** Git actions expose commit/push/create PR and branch actions.

**Memex:** Changes view has stage/unstage, split diffs and branch comparison; Git actions execute commit/push/gh draft PR.

**Limits:**

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-git-actions-290387202be8.js:1](</tmp/codex-parity-20261005/webview/assets/local-conversation-git-actions-290387202be8.js:1>) — Reachable checkout action menu. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` defaultMessage:`Git actions` ``. Byte: 9393. Asset SHA-256: `89ea20656e5609c2fbed87f9926537dd6516cb61217fc4c873f4bd8ad4f27346`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:19–28](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift#L19-L28) — Commit, push and draft pull-request actions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Action `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGit.swift:54–100](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceGit.swift#L54-L100) — Actual Git and gh execution implementation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` commit / push / createPullRequest `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceChangesView.swift:64–70](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceChangesView.swift#L64-L70) — Current/branch and split-diff controls. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` scope / splitDiff `.

<a id="notifications"></a>

### Completion, approval and question notifications

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Notification delivery is now implemented in Memex, not merely sidebar indicators.

**Recommendation:** Do not carry the old missing-notifications claim forward.


**Current Memex:** Opt-in completion, approval and question notifications have persistent event/sound preferences, sound-only mode, request deduplication and suppression for the viewed conversation unless enabled. Clicking a notification opens its conversation; actual delivery still depends on packaged-app OS authorization.

**T3-TR-09 — T3:** Opt-in per-client sound/desktop/in-app notification path with browser permission gate.

**CX-P05 — Installed ChatGPT Codex surface:** Codex settings include agent turn, permission and question notifications.

**Limits:** OS/browser permissions, running client and user opt-in constrain delivery. No actual notification was emitted during research. Codex labels alone do not prove notifications enabled or OS authorization granted.

**Evidence:**

- [T3 · apps/web/src/components/ThreadNotificationCoordinator.tsx:124–179](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ThreadNotificationCoordinator.tsx#L124-L179) — Transition-driven alerts and sound Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` EnvironmentNotifications `.

- [T3 · apps/web/src/components/ThreadNotificationCoordinator.tsx:209–231](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ThreadNotificationCoordinator.tsx#L209-L231) — Permission-gated delivery Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` desktop notification `.

- [Memex · apps/macos/Sources/Memex/ConversationNotifications.swift:87–103](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L87-L103) — Packaged app and OS authorization Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` setMode `.

- [Memex · apps/macos/Sources/Memex/ConversationNotifications.swift:111–145](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L111-L145) — Identity dedupe and visibility suppression Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` receive `.

- [Memex · apps/macos/Sources/Memex/MemexApp.swift:82–90](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L82-L90) — Activates and navigates notifications Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` notification wiring `.

- [Memex · apps/macos/Tests/MemexTests/ConversationNotificationTests.swift:14–71](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationNotificationTests.swift#L14-L71) — Avoids historical replay and dedupes pending requests Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` notification tests `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/notifications-settings-9ccd12da7ea7.js:1](</tmp/codex-parity-20261005/webview/assets/notifications-settings-9ccd12da7ea7.js:1>) — Codex local agent permissions and question notification settings live with agent controls. Version: `26.930.51102 (13100)`. Anchor: ` notifications.permissions.label `. Byte: 5935. Asset SHA-256: `7528b99262ae8427453725aab295972502e8d5541c55df7ef3501f5aa1db3a3f`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationNotifications.swift:110](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L110-L110) — State transition logic emits completion/approval/question events and applies preferences. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` func receive(_ session: Session `. Byte: 4730.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationNotifications.swift:182](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L182-L182) — User-facing controls configure delivery, completion/input and while-viewing behavior. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` struct ConversationNotificationSettingsView `. Byte: 8620.

<a id="composer-context"></a>

### Composer, attachments and captured context

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

The old claim that Memex first prompts cannot use attachments or explicit settings is stale.

**Recommendation:** Keep the prepared-session/no-send boundary and inherited-versus-explicit settings distinction.


**Current Memex:** Home and follow-up composers support attachments, image/file paste and drop, file/chat mentions, slash commands, local skills, stash and recall. Captured context is immutable and provider-validated. Home can prepare model/permission settings without sending; explicit settings are acknowledged before the first send. Local skills capture file contents, and local commands support $ARGUMENTS substitution.

**T3-I01 — T3:** T3 uses the same rich composer and provider/model state for draft and existing thread targets.

**T3-I03 — T3:** T3 combines thread and path suggestions; built-in /model, /plan, /default, provider slash commands, and skill invocation insertion are wired.

**codex-conversation-01 — Codex installed desktop:** Reachable composer context model carries image/file/pasted-text/selection/MCP/annotation attachment types.

**Limits:** Memex preparation creates a real native session; it is not a purely local settings preview. Runtime-enabled build is required. Local file/skill catalog returns empty for server-owned or non-local sessions; remote inventory belongs to another facet owner. Memex conversation mentions are excerpts, not a permanent live link to all chat context. Provider capability and live account availability were not exercised.

**Evidence:**

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:2133–2155](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2133-L2155) — Provider-filtered runtime modes and model state apply to composerDraftTarget. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` ChatComposer provider state `.

- [Memex · apps/macos/Sources/Memex/HomeConversationComposer.swift:98–119](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/HomeConversationComposer.swift#L98-L119) — Creates a prepared conversation without sending and captures attachments. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` attachToNewDraft/captureClipboard `.

- [Memex · apps/macos/Sources/Memex/HomeConversationComposer.swift:125–144](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/HomeConversationComposer.swift#L125-L144) — Reachable Home attachment and Model and permissions actions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` controls `.

- [Memex · apps/macos/Sources/Memex/StoreNewConversation.swift:71–97](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreNewConversation.swift#L71-L97) — Apply settings, attach, transfer durable Home draft, optionally send. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` startConversationFromHome `.

- [Memex · apps/macos/Tests/MemexTests/ConversationQueueTests.swift:217–239](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L217-L239) — Existing test exercises settings acknowledgement before returning. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` firstTurnSettingsWaitForObservedSelectionBeforeReturning `.

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:2616–2700](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2616-L2700) — Thread/file suggestions and provider-aware slash/skill menu. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` composerMenuItems `.

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:3939–4005](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L3939-L4005) — Built-in settings controls, provider token insertion, $skill insertion. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` command selection `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:300–338](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L300-L338) — Built-in/provider/local prompt suggestions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` slashSuggestions `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:352–405](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L352-L405) — Command dispatch, content capture and argument substitution. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` submit/capturePrompt `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:409–436](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L409-L436) — File and conversation context wiring. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` mentionSuggestions/selectMention `.

- [Memex · apps/macos/Sources/Memex/ConversationComposerCatalog.swift:21–78](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposerCatalog.swift#L21-L78) — Local-only bounded 5000-file/1000-prompt catalog; known skills and command roots. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` load `.

- [Memex · apps/macos/Tests/MemexTests/ConversationComposerTests.swift:74–105](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationComposerTests.swift#L74-L105) — Existing catalog coverage. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` fileAndSkillCatalogUsesWorkspaceAndOwningProviderRoots `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1654](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1654>) — Composer state has typed context collections. Version: `26.930.51102 (13100)`. Anchor: ` mcpAppModelContextAttachments:[],selectedTextAttachments: `. Byte: 3228256. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:75–108](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L75-L108) — Reachable native composer wires attachments, mentions, queue and steer. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` AcpComposerView / onPasteCommand / onDrop `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:186–230](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L186-L230) — Prompt stash, recall, mentions and commands are exposed. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` inputMenu `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:413–438](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L413-L438) — Chat excerpt and file mentions enter attached context. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` mentionSuggestions / selectMention `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptContext.swift:10–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptContext.swift#L10-L46) — Captured contents become real blocks and enforce capability/size limits. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationAttachment.text / validate `.

<a id="t3-tr-07"></a>

### Conversation outline and reading position

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Earlier dormant-outline finding is obsolete.

**Recommendation:** Treat layout differences as design choices, not missing navigation.

**T3-TR-07 — T3:** Timeline minimap, citation targeting, remembered position and anchor preservation.

**Memex:** Wired prompt-outline popover, previous/next selection and exact record reveal, reading/disclosure restoration.

**Limits:** Source inspection only; tests mentioned were read, not executed.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:1342–1385](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1385) — Anchoring plus minimap Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` MessagesTimeline `.

- [Memex · apps/macos/Sources/Memex/Reader.swift:67–73](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L67-L73) — Loads historical/live outline and follows visible record Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` outline refresh `.

- [Memex · apps/macos/Sources/Memex/Reader.swift:197–223](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L197-L223) — Previous/next and prompt selection Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` outlinePopover `.

- [Memex · apps/macos/Sources/Memex/Reader.swift:268–269](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L268-L269) — Reachable outline control Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` toolbar `.

- [Memex · apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:80–89](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L80-L89) — Retains selection Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` outlineLiveRefreshPreservesSelectionAndOriginalOffsets `.

<a id="queue"></a>

### Durable queue, edit, reorder and steering

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

The earlier no-queue/no-steer gap is closed for supported native providers.

**Recommendation:** Keep capability-gated steering and explicit queue hold after uncertainty or stop.


**Current Memex:** A durable queue preserves captured prompt bytes and order, with edit, reorder, remove, hold/resume and promotion controls. Enter follows the selected queue/steer behavior, with explicit Queue and Steer buttons. Steering is gated by observed provider support; Stop holds queued work.

**T3-I04 — T3:** T3 routes running submissions through configurable queue/steer/restart policy and provides queued-run edit/reorder/cancel/promotion controls.

**codex-conversation-04 — Codex installed desktop:** Reachable queue has reorder, edit, delete, steer, default queue/steer toggle and pause after interruption.

**Limits:** Queue implementations have different ownership/lifetimes; this finding covers active input controls rather than claiming identical server recovery. Codex uses drag reorder; Memex exposes explicit earlier/later commands. Memex steering is gated by snapshot.canSteer and request/settings state.

**Evidence:**

- [T3 · packages/client-runtime/src/state/composerDispatch.ts:1–31](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/state/composerDispatch.ts#L1-L31) — Default/alternate queue-versus-steer dispatch. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` resolveComposerDispatchMode `.

- [T3 · apps/web/src/components/chat/QueuedRunsControl.tsx:168–243](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/QueuedRunsControl.tsx#L168-L243) — Reachable queue actions. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` move/steer/remove/editLatest `.

- [T3 · apps/web/src/components/chat/QueuedRunsControl.test.tsx:78–171](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/QueuedRunsControl.test.tsx#L78-L171) — Existing tests cover thumbnails, optimistic acknowledgement and editing visibility. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` queued attachments/edit tests `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:94–105](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L94-L105) — Explicit queue and steer buttons. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` while-working controls `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:352–377](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L352-L377) — Keyboard submission dispatches working follow-up policy. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` submit `.

- [Memex runtime · packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:726–759](</Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:726>) — Text submission is wired independently of idle Send button disabled state. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` AcpPromptBox/onSubmit `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:587–680](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L587-L680) — Durable enqueue and edit/reorder/cancel/hold/resume/promote/drain. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` queue lifecycle `.

- [Memex · apps/macos/Tests/MemexTests/ConversationQueueTests.swift:79–107](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L79-L107) — Existing persisted-queue contract test. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` queueEditsAndOrderSurviveRelaunchWithoutDispatch `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1](</tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1>) — Queue pauses and can resume after interruption. Version: `26.930.51102 (13100)`. Anchor: ` Queue paused because you interrupted `. Byte: 4681. Asset SHA-256: `6387ca194a268297d5218098906dba8894858dbfdc61a786c9325a0cf7243bed`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1](</tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1>) — Send-now action steers without interrupting. Version: `26.930.51102 (13100)`. Anchor: ` Submit without interrupting the model `. Byte: 13009. Asset SHA-256: `6387ca194a268297d5218098906dba8894858dbfdc61a786c9325a0cf7243bed`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQueueView.swift:12–58](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQueueView.swift#L12-L58) — Edit, move earlier/later, promote, remove and pause/resume are reachable. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationQueueView.body `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:587–681](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L587-L681) — Queue persists, checks capability, and dispatches prompt versus steer. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` enqueueDraft / steerDraft / dispatchQueued `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:79–215](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L79-L215) — Existing tests cover persistence, stop settlement, steering identity and uncertain recovery; inspected, not run. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` queueEditsAndOrderSurviveRelaunchWithoutDispatch / recoveryDoesNotReplayAnUnconfirmedDequeuedCommand `.

<a id="t3-tr-06"></a>

### Exact in-conversation Find and raw evidence

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex retains an evidence-navigation advantage; browser Find cannot search rows absent from the virtual DOM.

**Recommendation:** Preserve exact Find and raw access when adding richer rendering.

**T3-TR-06 — T3:** Reviewed MessagesTimeline/ThreadDetailsPanel and shared state expose rich normalized items; no all-history exact-occurrence Find or raw-provider-transcript toggle located.

**Memex:** Paged literal case-insensitive Find with previous/next and exact raw fallback; live mode searches projected records.

**Limits:** T3 absence claim is bounded to reviewed web transcript/shared runtime, not every external browser extension; live Memex raw evidence is the projected record, not a full provider dump.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:1342–1385](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1385) — Virtualized bounded rendered rows, not full-history browser Find Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` MessagesTimeline `.

- [T3 · apps/server/src/orchestration-v2/ThreadSearch.ts:88–143](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L88-L143) — Separate cross-thread search is one match per thread Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` searchRows `.

- [Memex · apps/macos/Sources/Memex/ConversationFind.swift:12–25](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationFind.swift#L12-L25) — Literal occurrence ranges Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ranges `.

- [Memex · apps/macos/Sources/Memex/ConversationFind.swift:83–136](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationFind.swift#L83-L136) — Reads successive pages; preserves live selection Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationFindState `.

- [Memex · apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:92–102](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L92-L102) — Raw hidden match reveals exact source Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` rawOnlyEvidenceCanBeFoundAndRevealedExactly `.

<a id="t3-tr-05"></a>

### Historical search scope

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex remains a history-retrieval product; T3 search is active-thread discovery. Archiving a T3 thread removes its message content from this search.

**Recommendation:** Do not copy T3 search exclusions into Memex; keep retrieval scope clear.

**T3-TR-05 — T3:** Literal SQL LIKE over finished user/assistant messages in active V2 threads/projects, one best result per thread; archived/tool/system/unimported V1 content excluded.

**Memex:** Native UI uses indexed lexical search with provider/project/time/origin/machine filters and record anchors.

**Limits:** Native macOS search is lexical, not the MCP hybrid/semantic surface. The T3 search limitation is an implemented boundary, not an alleged data-loss defect.

**Evidence:**

- [T3 · apps/server/src/orchestration-v2/ThreadSearch.ts:64–68](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L64-L68) — Defines active V2 corpus Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` ThreadSearch contract `.

- [T3 · apps/server/src/orchestration-v2/ThreadSearch.ts:88–143](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L88-L143) — Explicit exclusions and one-best-match ranking Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` searchRows `.

- [Memex · apps/macos/Sources/Memex/Client.swift:126–143](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Client.swift#L126-L143) — Filtered native lexical search Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` search `.

- [Memex · apps/macos/Sources/Memex/Client.swift:235–249](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Client.swift#L235-L249) — Exact source-record anchor lookup Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` initialRecordOffset `.

<a id="files"></a>

### Local file browsing, editing and conflict protection

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Memex files/editor is implemented; visual editor depth is a separate UX question.

**Recommendation:** Retain existing capability and evaluate syntax/navigation refinements only with specific user needs.


**Current Memex:** The workspace Files pane supports browsing, file/folder creation, editing, save, previews and add-to-chat. Saves check the loaded disk fingerprint; conflicts keep the draft available for explicit reconciliation.

**T3-W05 — T3:** Chat file preview mounts editable CodeMirror-like source surface and rendered markdown/media/table previews; save uses environment writeFile.

**codex-workspace-05 — Installed ChatGPT Codex desktop:** Installed file-source editor component exists, including executor-aware file availability.

**Limits:** Source inspection only; no runtime or test execution.

**Evidence:**

- [T3 · apps/web/src/components/files/FilePreviewPanel.tsx:567–590](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/FilePreviewPanel.tsx#L567-L590) — Editable file component. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` EditableFileSurface `.

- [T3 · apps/web/src/components/files/useFileSaveCoordinator.ts:19–40](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/useFileSaveCoordinator.ts#L19-L40) — Editor persistence dispatches environment writeFile. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` useFileSaveCoordinator `.

- [Memex · apps/macos/Sources/Memex/WorkspacePanelView.swift:38–45](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L38-L45) — Files view reachable for local workspace. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Memex · apps/macos/Sources/Memex/WorkspaceFilesView.swift:31–87](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFilesView.swift#L31-L87) — Create, open, add context, save and conflict resolution controls. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1](</tmp/codex-parity-20261005/webview/assets/editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1>) — File editor handles environment-dependent file access. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` Files will be available when the task has an environment `. Byte: 5927. Asset SHA-256: `388c9d123cefdab2f7527afd6e0e165fa8372e3867fec07b2c81b2400ef68eca`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspacePanelView.swift:38–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L38-L46) — Files pane is wired into workspace panel. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceFilesView `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceFilesView.swift:62–89](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFilesView.swift#L62-L89) — Editor and conflict UI. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Save / Keep my edits / Use disk `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceFilesView.swift:224–240](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFilesView.swift#L224-L240) — Write/conflict path. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` save `.

<a id="forks"></a>

### Native forks, lineage and context branches

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Old no-fork/lineage finding is obsolete. Context return is not a Git merge in either product.

**Recommendation:** Retain explicit native-history versus captured-context distinction.


**Current Memex:** Capable connected providers support native history fork and rewind at valid idle boundaries. Context branches can move across providers or prepare a new worktree, while persisted parent/child relationships, context return to the parent draft and an operation journal preserve lineage and recovery. Context-with-worktree is distinct from a native history-preserving worktree fork.

**T3-TR-03 — T3:** Response fork and relationship panel with merge-back.

**codex-conversation-09 — Codex installed desktop:** Fork service retains history, supports same-directory and asynchronous worktree fork, and distinguishes incomplete setup.

**Limits:** Provider capability and idle-state gates apply; Memex context capture is capped at 4 MB and errors rather than silently truncating. Parity is at workflow family level. Memex native mutation requires idle state, empty queue and provider capability; this is not a claim that its context branch is an identical native-history worktree fork.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:2575–2586](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2575-L2586) — Response fork control Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` assistant fork action `.

- [T3 · apps/web/src/components/chat/ThreadRelationshipsControl.tsx:282–300](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ThreadRelationshipsControl.tsx#L282-L300) — Dispatches merge-back and opens parent Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` merge `.

- [Memex · apps/macos/Sources/Memex/ConversationHistoryActions.swift:19–44](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationHistoryActions.swift#L19-L44) — Reachable native/context fork and parent return Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationHistoryActions `.

- [Memex · apps/macos/Sources/Memex/StoreConversationHistory.swift:42–81](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreConversationHistory.swift#L42-L81) — Captures context, preserves draft and relationship Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` branchWithContext `.

- [Memex · apps/macos/Sources/Memex/StoreConversationHistory.swift:110–126](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreConversationHistory.swift#L110-L126) — Adds captured context to parent draft Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` mergeContextToParent `.

- [Memex · apps/macos/Tests/MemexTests/ConversationHistoryTests.swift:50–66](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationHistoryTests.swift#L50-L66) — Durable operation marker Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` pendingNativeMutationSurvivesReopenAndCannotBeRepeated `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1](</tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1>) — Same-directory native fork. Version: `26.930.51102 (13100)`. Anchor: `` case`same-directory` ``. Byte: 534. Asset SHA-256: `d6bceb516c7c25dd17e58f1074bc1f256b28f701e4997e8d1357ff397ea0b4a9`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1](</tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1>) — Asynchronous worktree fork retains working-tree starting state. Version: `26.930.51102 (13100)`. Anchor: `` case`worktree` ``. Byte: 1043. Asset SHA-256: `d6bceb516c7c25dd17e58f1074bc1f256b28f701e4997e8d1357ff397ea0b4a9`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationHistoryActions.swift:19–54](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationHistoryActions.swift#L19-L54) — Reachable branch/fork/rewind actions and recovery notice. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationHistoryActions.body `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/StoreConversationHistory.swift:42–100](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreConversationHistory.swift#L42-L100) — Context branch can request new worktree; native mutation records relationship. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` branchWithContext / mutateNativeHistory `.

- [Memex runtime · /Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:532–628](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:532>) — Native fork/revert requires compatibility and verifies chosen cutoff. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` mutateHistory / fork cutoff verification `.

<a id="terminal"></a>

### Persistent local workspace terminals

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Terminal itself is not a parity gap. Remote terminals are a distinct boundary.

**Recommendation:** Reuse the native terminal; do not replace it solely for comparator parity.


**Current Memex:** Ghostty-backed terminal sessions are retained per local workspace/worktree across pane and chat switches, with multiple shells, tabs/splits, right-pane or drawer placement, scrollback capture, save and context actions. Existing tests cover persistence and independent shells; remote terminals are a separate gap.

**T3-W07 — T3:** Persistent panel/drawer terminals with split and context callbacks; server PTY manager owns execution.

**codex-workspace-06 — Installed ChatGPT Codex desktop:** Terminal tab tracks session/environment, reconnect states and fresh-shell semantics.

**Limits:** Source inspection only; no runtime or test execution. Process retention within app lifetime differs from saved scrollback after restart; no claim of immortal shell sessions.

**Evidence:**

- [T3 · apps/web/src/components/ChatView.tsx:10602–10619](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10602-L10619) — Persistent terminal, split directions, new/close and context callbacks. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` PersistentThreadTerminalPanel `.

- [Memex · apps/macos/Sources/Memex/WorkspacePanelView.swift:56–57](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L56-L57) — Native terminal pane entry. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Memex · apps/macos/Sources/Memex/WorkspaceTerminalView.swift:40–52](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalView.swift#L40-L52) — Retained workspace terminal group selection. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body.task `.

- [Memex · apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift:45–49](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift#L45-L49) — Persistence test exists. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` nativeShellKeepsProcessEnvironmentAndScrollbackAcrossHosts `.

- [Memex · apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift:127–131](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift#L127-L131) — Independent-shell test exists. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` independentShellsInOneWorkspaceKeepStateAndCaptureHistory `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/terminal-tab-e527a3db0b81.js:2](</tmp/codex-parity-20261005/webview/assets/terminal-tab-e527a3db0b81.js:2>) — Terminal UI distinguishes reconnection from replay. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` Opening a fresh shell. Previous commands won’t be rerun `. Byte: 2477. Asset SHA-256: `16bd204b78343ec7d1b31f061f4c9b71d4d5abe729d7392a010c4af4e3bb9b5c`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalGroup.swift:5–8](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalGroup.swift#L5-L8) — Process ownership and retained sessions; snapshots are never replayed. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceTerminalGroup `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalView.swift:86–106](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalView.swift#L86-L106) — New shell, layouts, selection/scrollback context, restart/close. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceTerminalGroupView `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalSession.swift:91–97](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalSession.swift#L91-L97) — Actual Ghostty surface configured with working directory. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` TerminalSurfaceOptions `.

<a id="worktrees"></a>

### Projects and managed worktree lifecycle

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Do not count worktrees as a missing Memex capability.

**Recommendation:** Preserve Memex isolation and retained-chat cleanup guards; compare handoff UX separately.


**Current Memex:** Saved local projects can open an existing directory or prepare a managed worktree with a default base ref and setup command. The native lifecycle supports archive, reattach and safe clean removal; basic project/worktree management is implemented.

**T3-W01 — T3:** Reachable worktree setup UI and authenticated current-thread handoff service.

**codex-workspace-01 — Installed ChatGPT Codex desktop:** Shipped host-aware worktree query and worktree settings.

**Limits:** Source inspection only; no runtime or test execution.

**Evidence:**

- [T3 · apps/web/src/components/chat/WorktreeSetupCard.tsx:284–297](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/WorktreeSetupCard.tsx#L284-L297) — Setup script and settled outcome presentation. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` WorktreeSetupCard `.

- [T3 · apps/server/src/mcp/WorktreeMcpService.ts:248–269](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/WorktreeMcpService.ts#L248-L269) — Creates the worktree under cancellation/rollback protection. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` handoff `.

- [Memex · apps/macos/Sources/Memex/ConversationWorkspace.swift:22–28](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWorkspace.swift#L22-L28) — Delegates existing/new checkout preparation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` prepare `.

- [Memex · apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift:102–110](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift#L102-L110) — Existing workspace branch of preparation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` prepare `.

- [Memex · apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift:155–170](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift#L155-L170) — Creates managed branch and worktree. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` prepare `.

- [Memex · apps/macos/Sources/Memex/WorkspaceLifecycleView.swift:23–60](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceLifecycleView.swift#L23-L60) — Archive, reattach and guarded removal. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` body `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/use-codex-worktrees-3c6a69cec5c9.js:1](</tmp/codex-parity-20261005/webview/assets/use-codex-worktrees-3c6a69cec5c9.js:1>) — Worktree inventory requests include hostConfig and worktreesRoot. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: `` method:`codex-worktrees` ``. Byte: 556. Asset SHA-256: `b9348e02bb5dfd0dc7680dcdb30a510871c71dd5afdf21c01f92ffb3dd27a97a`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LocalProjects.swift:4–16](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LocalProjects.swift#L4-L16) — Project workspace modes, base ref and setup command. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationWorkspaceMode / LocalProject `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceLifecycleView.swift:25–43](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceLifecycleView.swift#L25-L43) — Checkout attach/archive/reattach actions. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` WorkspaceLifecycleView `.

<a id="t3-i10"></a>

### Prompt stash and recall

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Parity exists and Memex's explicit copy/delete policy avoids destructive stash eviction.

**Recommendation:** Keep the non-destructive Memex stash policy rather than adopting every T3 interaction literally.

**T3-I10 — T3:** T3 provides prompt history navigation and a 20-entry stash with rich records/files/images. Capacity evicts the oldest with a warning; restore consumes an entry.

**Memex:** Memex provides previous/next/recent prompt recall, a 20-entry stash that refuses overflow and copies on restore; captured attachment bytes are saved atomically before clearing unchanged input.

**Limits:**

**Evidence:**

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:4299–4367](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4299-L4367) — History handling is wired to composer. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` prompt history `.

- [T3 · apps/web/src/components/chat/ChatComposer.tsx:4961–4973](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4961-L4973) — Oldest entry evicted on overflow. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` stash eviction warning `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:191–269](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L191-L269) — Reachable stash/recall and unchanged-input clearing. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` inputMenu/stashPrompt/restoreStash `.

- [Memex · apps/macos/Sources/Memex/ConversationPromptLibrary.swift:15–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptLibrary.swift#L15-L74) — Copy-on-restore policy, full rejection and atomic captured-payload save. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` stash persistence `.

- [Memex · apps/macos/Tests/MemexTests/ConversationComposerTests.swift:12–61](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationComposerTests.swift#L12-L61) — Roundtrip, full/unreadable store and original draft recall. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` stash/recall tests `.

<a id="t3-i08"></a>

### Provider-specific approval options

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Basic approval interaction is implemented in both. Warning/detail semantics differ by provider and projection.

**Recommendation:** Keep native option identity and explicit response gating; avoid reducing all providers to one universal allow/deny policy.

**T3-I08 — T3:** T3 renders provider-supplied labels, warnings and primary/secondary options, with live response capability supplied by parent.

**Memex:** Memex maps exact option IDs/titles/kinds and approval detail to SQACPUI; replies preserve request/option IDs and are guarded against stale requests. Cancel invokes Stop.

**Limits:**

**Evidence:**

- [T3 · apps/web/src/components/chat/ComposerPendingApprovalActions.tsx:24–59](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx#L24-L59) — Provider options plus default primary decisions and warnings. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` approval rendering `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:52–67](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L52-L67) — Exact options, details and response callbacks. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` approval adapter `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:757–760](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L757-L760) — Validates active approval/option and sends exact IDs. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` approve `.

- [Memex · apps/macos/Tests/MemexTests/LiveConversationTests.swift:508–536](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/LiveConversationTests.swift#L508-L536) — Existing exact-ID response contract. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` sendIsSingleFlightAndStopAndInteractionRepliesUseOriginalIDs `.

<a id="transcript"></a>

### Readable structured transcript and raw evidence

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

The earlier completed-work and nested-Claude-image defects are addressed at the inspected baseline.

**Recommendation:** Avoid repeating the old defects; preserve these composition tests.


**Current Memex:** The native transcript renders Markdown tables, code, links, images and structured tool output, with raw disclosure, completed-work folding, paginated Find and a prompt outline. Exact evidence metadata restores phase, turn and lifecycle; opaque image content and full-output artifacts are retained, and unknown evidence stays raw.

**T3-TR-04 — T3:** Typed work, reasoning, lifecycle/error/compaction/todo rows and rich attachments.

**codex-conversation-13 — Codex installed desktop:** Local turn renderer groups user/assistant/activity entries; thread find command and history search hydration exist.

**Limits:** Not equal rendering of every provider field or media type; artifact references do not prove the underlying file still exists. Source inspection does not measure runtime performance or accessibility quality. Codex find reachability was traced through command dispatch/hydration, not exercised. Memex literal find is not regex search.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:2730–2802](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2730-L2802) — Explicit error/interrupt/handoff/fork/compaction/todo semantics Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` v2EventPresentation `.

- [Memex · apps/macos/Sources/Memex/ConversationProjection.swift:104–125](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L104-L125) — Exact retained evidence join Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` metadataByEntity `.

- [Memex · apps/macos/Sources/Memex/ConversationProjection.swift:177–201](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L177-L201) — Child/lifecycle projection Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` emitMetadata `.

- [Memex · apps/macos/Sources/Memex/ConversationProjection.swift:286–310](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L286-L310) — Retains media/full artifact Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` tool_result `.

- [Memex · apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:6–57](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L6-L57) — Tests phase folding and resumed Claude media/full output Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` fidelity tests `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1](</tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1>) — Local turn rendering separates user content from activity groups. Version: `26.930.51102 (13100)`. Anchor: `` case`user-message` ``. Byte: 20189. Asset SHA-256: `0319cf6508b693232800baee3dc5f8642468c260ae870b4304715bf3abeaa61e`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1522](</tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1522>) — Native find command is dispatched. Version: `26.930.51102 (13100)`. Anchor: `` type:`find-in-thread` ``. Byte: 1475670. Asset SHA-256: `22f3ea455585cfc0508e3d3627eac161c80c849c0aace3d76d09fdebaa0aeca3`.

- [Codex · /tmp/codex-parity-20261005/webview/assets/app-shared-9d148924be0b.js:487](</tmp/codex-parity-20261005/webview/assets/app-shared-9d148924be0b.js:487>) — History search results can be hydrated. Version: `26.930.51102 (13100)`. Anchor: ` hydrateConversationSearchMatch(e) `. Byte: 3132201. Asset SHA-256: `eb2500c5398279d4fbf5c71d7ce982d54631660907761740199f6c22591d71ab`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Reader.swift:31–58](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L31-L58) — Reachable reader includes recovery, work view and find state. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Reader.body `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationFind.swift:85–120](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationFind.swift#L85-L120) — Cancellable paged whole-conversation literal find. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` search(in:) `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/TranscriptPresentation.swift:178–240](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptPresentation.swift#L178-L240) — Completed routine work folds while failures remain visible. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` group `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RichTextRenderer.swift:28–93](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/RichTextRenderer.swift#L28-L93) — Native Markdown code, quotes, headings, lists and tables. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` appendBlock / appendTable `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ToolContentRenderer.swift:80–123](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ToolContentRenderer.swift#L80-L123) — Raw tool content remains inspectable beside structured rendering. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` render `.

<a id="t3-tr-08"></a>

### Rename, pin, archive, removal and bulk ordering

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Old basic-lifecycle absence is obsolete. Memex deliberately protects ingested provider records.

**Recommendation:** Preserve reversible removal semantics.

**T3-TR-08 — T3:** Thread action menu supports rename/pin/archive/delete, with archive distinct from deletion.

**Memex:** Reachable rename/pin/archive/restore/remove and multi-selection; locked atomic local metadata persists and preserves provider history. Removal is reversible local hiding, not destructive deletion.

**Limits:** Source inspection only; tests mentioned were read, not executed.

**Evidence:**

- [T3 · apps/web/src/components/threadActionMenu.logic.ts:121–125](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L121-L125) — Pin/unpin Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` thread actions `.

- [T3 · apps/web/src/components/threadActionMenu.logic.ts:219–235](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L219-L235) — Archive versus permanent deletion Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` thread actions `.

- [Memex · apps/macos/Sources/Memex/ConversationLibrary.swift:83–108](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationLibrary.swift#L83-L108) — Reversible metadata actions Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` organization mutations `.

- [Memex · apps/macos/Sources/Memex/SidebarConversations.swift:211–232](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/SidebarConversations.swift#L211-L232) — Reachable pin/archive/restore/remove Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` bulk context menu `.

- [Memex · apps/macos/Tests/MemexTests/ConversationLibraryTests.swift:11–39](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationLibraryTests.swift#L11-L39) — Native source untouched Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` organizationSurvivesRelaunchWithoutChangingProviderHistory `.

- [Memex · apps/macos/Tests/MemexTests/ConversationLibraryTests.swift:64–106](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationLibraryTests.swift#L64-L106) — Concurrent metadata and filtered order Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` library concurrency/reorder tests `.

<a id="recovery"></a>

### Stop acknowledgement and uncertain delivery recovery

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

The old discarded-interrupt-response and indefinitely stuck stopping findings do not apply to the inspected current baseline.

**Recommendation:** Preserve exact command/native identity checks and never auto-resend an uncertain message.


**Current Memex:** Outgoing prompts and steering commands are persisted before dispatch. Ambiguous delivery becomes uncertain and requires reconnect/review rather than automatic replay. Stop awaits a terminal snapshot, warns on timeout and keeps the queue held.

**T3-I06 — T3:** T3 has durable command receipts/event/effect recording, capability-checked interrupt dispatch and adapter error propagation.

**codex-conversation-06 — Codex installed desktop:** Queue distinguishes sending/pending/outcome-unknown; warns to check conversation before deleting an unconfirmed message.

**Limits:** No failure was induced in a live provider; source and tests were inspected only. T3 process/server restart semantics belong to the lifecycle facet, not this active-send comparison. Server command receipts do not establish a durable ordinary web-client send intent; see T3-I14 for the separate client recovery defect. Provider capability and live account availability were not exercised.

**Evidence:**

- [T3 · apps/server/src/orchestration-v2/EventSink.ts:518–576](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/EventSink.ts#L518-L576) — Command receipt deduplication, event/effect recording and post-commit publication. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` commitCommandEffect `.

- [T3 · apps/server/src/orchestration-v2/ProviderTurnControlService.ts:171–197](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ProviderTurnControlService.ts#L171-L197) — Native interrupt and surfaced errors. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` interrupt `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:366–368](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L366-L368) — Terminal state clears stopping. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` terminal snapshot stop settlement `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:717–754](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L717-L754) — Holds queue, handles pre-dispatch cancellation and missing terminal timeout. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` stop `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:779–818](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L779-L818) — Persist-before-provider, uncertain failure and explicit reconnect. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` perform `.

- [Memex runtime · packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1119–1127](</Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1119>) — Propagates turn/interrupt completion. Version: `7818518b094f44911aecfec7dbeb905c57ea24a9`. Anchor: ` cancelAcknowledged `.

- [Memex · apps/macos/Tests/MemexTests/ConversationQueueTests.swift:108–148](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L108-L148) — Terminal-state, rejected-stop and timeout tests. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` stop tests `.

- [Memex · apps/macos/Tests/MemexTests/ConversationQueueTests.swift:192–216](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L192-L216) — Existing recovery contract. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` recoveryDoesNotReplayAnUnconfirmedDequeuedCommand `.

- [Codex · /tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1](</tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1>) — User-facing unknown-outcome state. Version: `26.930.51102 (13100)`. Anchor: ` Delivery could not be confirmed. Check the conversation before deleting this saved message `. Byte: 9959. Asset SHA-256: `6387ca194a268297d5218098906dba8894858dbfdc61a786c9325a0cf7243bed`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationRecoveryView.swift:4–60](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationRecoveryView.swift#L4-L60) — Distinct archive, authentication, uncertain delivery and reconnect states. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationRecoveryKind / detail `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:484–518](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L484-L518) — Explicit pending-prompt reconciliation. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` restoreUnsentPrompt / restorePendingDraft / reconcilePendingPrompt `.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:192–215](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L192-L215) — Existing no-replay regression test. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` recoveryDoesNotReplayAnUnconfirmedDequeuedCommand `.

<a id="t3-tr-01"></a>

### Structured plans and implementation

**Memex: present · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

The old no-plan-workflow finding is obsolete. The products choose different dispatch semantics.

**Recommendation:** Preserve review-before-send semantics; consider richer proposed-plan export only if wanted.

**T3-TR-01 — T3:** Typed todo_list and proposed-plan cards; direct plan follow-up/new-thread dispatch.

**Memex:** Latest recognized native plan/update_plan/TodoWrite steps; Refine/Implement attach context to unsent draft; new-conversation preparation is wired.

**Limits:** Source inspection only; tests mentioned were read, not executed.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:2688–2705](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2688-L2705) — Renders proposed plan card Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` ProposedPlanTimelineRow `.

- [T3 · apps/web/src/components/ChatView.tsx:10091–10140](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10091-L10140) — Starts implementation follow-up Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` plan follow-up `.

- [Memex · apps/macos/Sources/Memex/ConversationWork.swift:44–66](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L44-L66) — Recognizes structured steps, never prose Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` project `.

- [Memex · apps/macos/Sources/Memex/ConversationWork.swift:114–132](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L114-L132) — Reachable refine/implement/new-conversation actions Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` ConversationWorkView `.

- [Memex · apps/macos/Sources/Memex/Reader.swift:40–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L40-L46) — Wires work panel Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Reader `.

- [Memex · apps/macos/Tests/MemexTests/ConversationHistoryTests.swift:98–110](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationHistoryTests.swift#L98-L110) — Plan branch retains unsent draft Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` providerTransitionCapturesContextIntoUnsentDraftAndMergePreservesParentDraft `.

<a id="codex-workspace-12"></a>

### Hosted cloud task and browser surface

**Validation: unknown · none · qualified confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Do not treat bundled ChatGPT Work cloud UI as universal Codex capability or infer hosted Memex absence.

**Recommendation:** Keep as an explicit unresolved deployment/product-boundary comparison.

**codex-workspace-12 — Installed ChatGPT Codex desktop:** Bundled cloud browser preview has screenshot timeline, live panel and ChatGPT Work labels.

**Memex:** Native app has server-owned/paired execution. Separate runtime repository contains hosted infrastructure, but account provisioning and this native app’s cloud workflow were not fully traced here.

**Limits:** ChatGPT Work-specific chunk; gate/entitlement and Codex applicability unverified. No cloud runtime observation.

**Evidence:**

- [Codex · /tmp/codex-parity-20261005/webview/assets/cloud-browser-preview-6a5c057f3897.js:1](</tmp/codex-parity-20261005/webview/assets/cloud-browser-preview-6a5c057f3897.js:1>) — Cloud browser replay/live surface in installed bundle. Version: `26.930.51102 (13100); app.asar sha256 a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`. Anchor: ` Cloud browser activity timeline `. Byte: 2460. Asset SHA-256: `22106a955f6798be9b0fdadf0f2aafcfa4baa563ce4407d52c67438a1335cb6b`.

- [Memex · /Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Store.swift:216–225](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Store.swift#L216-L225) — Native app distinguishes server-owned execution. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` canAccessLocalFiles `.

<a id="t3-tr-12"></a>

### Long-history publish architecture

**Validation: unknown · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Architecture differs, but source cannot determine latency, CPU or memory cost at actual transcript sizes.

**Recommendation:** Benchmark representative long conversations before deciding whether this path needs optimization.

**T3-TR-12 — T3:** LegendList virtualization and cursor-based progressive history loads.

**Memex:** NSTableView reuse and bounded visible window; native publish reads/reprojects complete runtime conversation before slicing.

**Limits:** Validation need, not established performance defect; no benchmark or runtime measurement was run.

**Evidence:**

- [T3 · apps/web/src/components/chat/MessagesTimeline.tsx:1342–1372](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1372) — Virtualized rows Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` MessagesTimeline `.

- [T3 · packages/client-runtime/src/state/threads.ts:636–691](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/state/threads.ts#L636-L691) — Cursor-based progressive history request Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` loadEarlier `.

- [Memex · apps/macos/Sources/Memex/InAppAgentRuntime.swift:191–218](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L191-L218) — Reads/projects full conversation Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` publish `.

- [Memex · apps/macos/Sources/Memex/LiveConversation.swift:238–245](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L238-L245) — Window after snapshot Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` visibleRecords `.

- [Memex · apps/macos/Sources/Memex/TranscriptController.swift:633–640](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptController.swift#L633-L640) — Reusable native rows Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` tableView `.

<a id="t3-w12"></a>

### Unproven universal capabilities

**Validation: unknown · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

Do not present a universal absence or parity claim from provider-specific or adjacent-product code.

**Recommendation:** Keep T3 marketplace, managed compute and arbitrary OS control unproven. Memex plugin-management and external-app boundaries are resolved separately by CX-P01 and codex-workspace-08; composer skill discovery is present under composer-context.

**T3-W12 — T3:** Inspected server has built-in app MCP toolkits and device tools; these do not prove a universal plugin marketplace, managed compute product or general desktop CUA.

**Memex:** Memex has provider runtimes, paired execution hosts, app-owned browser control and opt-in memex-control. The completed Codex facet confirms narrower native plugin-management and external-app boundaries; broad managed-cloud parity remains unresolved.

**Limits:** Source inspection only; no runtime or test execution.

**Evidence:**

- [T3 · apps/server/src/mcp/McpHttpServer.ts:754–766](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/McpHttpServer.ts#L754-L766) — Bounded built-in app toolkit inventory. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` layer `.

- [T3 · apps/server/src/mcp/toolkits/device/tools.ts:28–81](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/toolkits/device/tools.ts#L28-L81) — Device list/open/screenshot/close is a separate surface. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` device tools `.

- [Memex · apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:104–140](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L104-L140) — Bounded host capabilities. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` handle `.

- [Memex · apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:85–95](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift#L85-L95) — Only registered app-owned HTTP(S) tabs. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` dispatch `.

- [Memex · docs/execution-host.md:92–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/docs/execution-host.md#L92-L96) — Documents opt-in same-user authority and scoped browser describe/dispatch bridge. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` Control MCP and browser boundaries `.

<a id="t3-i13"></a>

### Build/runtime availability boundary

**Scope: not-applicable · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

This is an intentional packaging/provenance qualifier, not a demonstrated feature gap in the supplied runtime-enabled build. Installed-binary runtime provenance was not measured.

**Recommendation:** Make release/runtime provenance explicit in final synthesis; do not count dormant dependency APIs as shipped app capability.

**T3-I13 — T3:** T3 server's shipped provider driver list is built into its normal provider architecture; external provider readiness still controls usability.

**Memex:** Runtime dependencies are included by default when MEMEX_AGENT_RUNTIME_ROOT or .local-runtime-root resolves to a runtime checkout. Package.swift imports SQACP/SQACPHost/SQACPUI from that local root and links its archive. MEMEX_HISTORY_ONLY=1 explicitly removes these dependencies; without a resolved root the conditional Home fallback is EmptyView.

**Limits:** No app was built, launched or provider prompted in this task; installed-binary behavior remains unverified. The supplied Memex and runtime commits are separate baselines rather than an assertion that Package.swift pins the latter.

**Evidence:**

- [T3 · apps/server/src/provider/builtInDrivers.ts:1–18](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/provider/builtInDrivers.ts#L1-L18) — Configured providers resolve through shipped drivers. Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` driver build contract `.

- [Memex · apps/macos/Package.swift:4–38](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Package.swift#L4-L38) — Machine-local optional runtime and archive linkage. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` conditional runtime dependencies `.

- [Memex · apps/macos/Sources/Memex/ConversationComposer.swift:4–8](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L4-L8) — Full composer requires SQACPUI. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` conditional compilation `.

- [Memex · apps/macos/Sources/Memex/HomeConversationComposer.swift:226–231](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/HomeConversationComposer.swift#L226-L231) — Home composer fallback is empty. Version: `17e958778a7398db8238cc9c8415ca12649067e5`. Anchor: ` history-only fallback `.

<a id="t3-tr-11"></a>

### Sidebar snooze

**Memex: intentionally-excluded · none · high confidence.** Source inspection; cited existing tests read, not run. No new product runtime validation.

This is an explicit exclusion, not a backlog recommendation.

**Recommendation:** Do not propose sidebar snooze.

**T3-TR-11 — T3:** T3 supports snooze in thread lifecycle actions.

**Memex:** Not considered for implementation by explicit research contract.

**Limits:** Source inspection only; tests mentioned were read, not executed.

**Evidence:**

- [T3 · apps/web/src/components/threadActionMenu.logic.ts:128–140](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L128-L140) — Snooze is distinct from pin and settle Version: `ecfdda5fa804582066c1aa62afea419677c24883`. Anchor: ` lifecycle actions `.
