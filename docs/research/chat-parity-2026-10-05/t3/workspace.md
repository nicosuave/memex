# T3 / Memex workspace and integration inventory

Source-only comparison at the contract baselines. No source edits, builds, tests, provider requests, UI actions or PR actions were performed.

The main gap is remote workspace surfaces, not remote agent execution. Core local worktrees, Git, files, rewind, terminals, browser automation, schedules and an agent-callable opt-in control MCP server already exist in Memex. T3 is broader in inline PR review, app-tool inventory, preview actions and event/fresh-thread automation.

Two T3 designs should not be copied: checkpoint restore collapses staged and unstaged contents, and editor persistence lacks a disk-revision precondition. Memex already has stronger implementations for both.

Acceptance correction: the native app wiring alone was an incomplete integration boundary. The Rust memex-control executable and control_mcp.rs provide the actual opt-in agent-callable toolkit, including browser bridge dispatch; W08 and W11 now credit it.

## T3-W01 — Managed worktrees and lifecycle

**present · none · source-only.** Do not count worktrees as a missing Memex capability.

T3: Reachable worktree setup UI and authenticated current-thread handoff service.

Memex: Existing/new-worktree preparation and archive, reattach, safe clean removal UI.

Recommendation: Preserve Memex isolation and retained-chat cleanup guards; compare handoff UX separately.

Evidence:
- T3: [WorktreeSetupCard.tsx:284-297](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/WorktreeSetupCard.tsx:284) — Setup script and settled outcome presentation.
- T3: [WorktreeMcpService.ts:248-269](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/mcp/WorktreeMcpService.ts:248) — Creates the worktree under cancellation/rollback protection.
- Memex: [ConversationWorkspace.swift:22-28](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWorkspace.swift:22) — Delegates existing/new checkout preparation.
- Memex: [ManagedWorkspaceStore.swift:102-110](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift:102) — Existing workspace branch of preparation.
- Memex: [ManagedWorkspaceStore.swift:155-170](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift:155) — Creates managed branch and worktree.
- Memex: [WorkspaceLifecycleView.swift:23-60](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceLifecycleView.swift:23) — Archive, reattach and guarded removal.

Source inspection only; no runtime or test execution.

## T3-W02 — Git controls and inline pull-request workspace

**partial · P2 · source-only.** Memex already handles local Git, but its inspected native workflow ends at an external PR URL.

T3: Git workflow controls plus an environment-scoped PullRequestDetailPanel inside the chat.

Memex: Staging, commit, push and GitHub draft PR creation; draft PR opens externally. Local branch diff and review context feed chat.

Recommendation: If PR review parity is desired, add a first-class PR detail/review surface rather than duplicating local Git controls.

Evidence:
- T3: [ChatView.tsx:10621-10665](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:10621) — Diff and PR detail surfaces are mounted from selected panel.
- T3: [GitActionsControl.tsx:1730-1748](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/GitActionsControl.tsx:1730) — Git initialization and quick action UI.
- Memex: [WorkspaceGitActionsView.swift:19-28](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:19) — Complete native Git menu inventory.
- Memex: [WorkspaceGitActionsView.swift:84-100](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:84) — Draft PR success opens its URL externally.
- Memex: [WorkspaceChangesView.swift:63-80](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceChangesView.swift:63) — Current and branch comparison plus Git actions.
- Memex: [WorkspaceChangesView.swift:149-154](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceChangesView.swift:149) — Adds review context to chat.

Source inspection only; no runtime or test execution.

## T3-W03 — Checkpoint and conversation rewind

**present · none · source-only.** Both have rewind, with provider and checkout constraints.

T3: Timeline revert invokes provider rollback then optional checkpoint restore; provider capability errors are explicit.

Memex: Workspace pane wires checkpoint controls and conversation rewind; restore protects managed ownership and captures recovery.

Recommendation: Retain separate file/context intent and provider capability gating; do not infer all providers support rollback.

Evidence:
- T3: [MessagesTimeline.tsx:2285-2287](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/MessagesTimeline.tsx:2285) — Timeline revert entry point.
- T3: [CheckpointRollbackService.ts:257-265](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/orchestration-v2/CheckpointRollbackService.ts:257) — Provider rollback followed by optional file restore.
- Memex: [WorkspacePanelView.swift:18-25](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspacePanelView.swift:18) — Rewind handler supplied to workspace changes.
- Memex: [WorkspaceCheckpoints.swift:69-101](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceCheckpoints.swift:69) — Isolation validation, recovery snapshot and index restore.

Source inspection only; no runtime or test execution.

## T3-W04 — T3 checkpoint staging-state loss

**defect · P1 · source-only.** T3 cannot reconstruct the original staged/unstaged split from this checkpoint shape; copying it regresses Memex.

T3: Capture stores one final workspace tree; restore writes that tree to both worktree and staging index.

Memex: Captures a separate index tree and restores it after raw worktree restoration.

Recommendation: Keep separate index/worktree snapshots. Add an upstream regression test if pursuing a T3 fix.

Evidence:
- T3: [GitVcsDriver.ts:1030-1069](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/vcs/GitVcsDriver.ts:1030) — Single staged temporary-index tree becomes checkpoint commit.
- T3: [GitVcsDriver.ts:1091-1102](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/vcs/GitVcsDriver.ts:1091) — git restore passes both --worktree and --staged with the same source.
- Memex: [WorkspaceCheckpoints.swift:98-101](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceCheckpoints.swift:98) — Restores checkpoint.indexTree separately.
- Memex: [WorkspaceGitWorkflowTests.swift:47-76](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/WorkspaceGitWorkflowTests.swift:47) — Existing test distinguishes staged contents and verifies recovery snapshot.

Deterministic source-level defect, not reproduced by executing Git or tests in this inventory.

## T3-W05 — File browsing, editing and previews

**present · none · source-only.** Memex files/editor is implemented; visual editor depth is a separate UX question.

T3: Chat file preview mounts editable CodeMirror-like source surface and rendered markdown/media/table previews; save uses environment writeFile.

Memex: File/folder creation, text edits, save, disk conflict UI, previews and add-to-chat are reachable in workspace Files.

Recommendation: Retain existing capability and evaluate syntax/navigation refinements only with specific user needs.

Evidence:
- T3: [FilePreviewPanel.tsx:567-590](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/files/FilePreviewPanel.tsx:567) — Editable file component.
- T3: [useFileSaveCoordinator.ts:19-40](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/files/useFileSaveCoordinator.ts:19) — Editor persistence dispatches environment writeFile.
- Memex: [WorkspacePanelView.swift:38-45](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspacePanelView.swift:38) — Files view reachable for local workspace.
- Memex: [WorkspaceFilesView.swift:31-87](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceFilesView.swift:31) — Create, open, add context, save and conflict resolution controls.

Source inspection only; no runtime or test execution.

## T3-W06 — T3 editor concurrent-write overwrite

**defect · P1 · source-only.** T3 autosave can silently overwrite concurrent agent edits; this is a concrete design to avoid.

T3: Save sends only cwd, path and new contents; server unconditionally writes contents with no expected revision.

Memex: Save passes loaded fingerprint and throws a conflict if the disk version differs; draft remains available.

Recommendation: Preserve Memex optimistic concurrency and explicit disk-versus-draft reconciliation.

Evidence:
- T3: [useFileSaveCoordinator.ts:31-40](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/files/useFileSaveCoordinator.ts:31) — No expected content revision in write request.
- T3: [WorkspaceFileSystem.ts:305-340](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/workspace/WorkspaceFileSystem.ts:305) — Path resolution followed by unconditional file write.
- Memex: [WorkspaceFilesView.swift:224-240](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceFilesView.swift:224) — Passes fingerprint and loads disk version after conflict.
- Memex: [WorkspaceFiles.swift:58-78](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceFiles.swift:58) — Checks expectedFingerprint before write.

Source-level race scenario; no runtime reproduction performed.

## T3-W07 — Persistent embedded terminal

**present · none · source-only.** Terminal itself is not a parity gap. Remote terminals are a distinct boundary.

T3: Persistent panel/drawer terminals with split and context callbacks; server PTY manager owns execution.

Memex: Native Ghostty terminals keyed to workspace, multiple shells, right pane/drawer, context capture; existing tests check persistence and independent shells.

Recommendation: Reuse the native terminal; do not replace it solely for comparator parity.

Evidence:
- T3: [ChatView.tsx:10602-10619](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:10602) — Persistent terminal, split directions, new/close and context callbacks.
- Memex: [WorkspacePanelView.swift:56-57](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspacePanelView.swift:56) — Native terminal pane entry.
- Memex: [WorkspaceTerminalView.swift:40-52](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalView.swift:40) — Retained workspace terminal group selection.
- Memex: [WorkspaceTerminalTests.swift:45-49](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift:45) — Persistence test exists.
- Memex: [WorkspaceTerminalTests.swift:127-131](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift:127) — Independent-shell test exists.

Source inspection only; no runtime or test execution.

## T3-W08 — Browser automation and app-owned preview

**partial · P2 · source-only.** Both products expose agent-callable browser control. The narrower Memex action inventory, not absence of a tool bridge, is the gap.

T3: Preview panel plus MCP open/navigate/resize/snapshot/click/type/press/scroll/evaluate/wait/recording tools.

Memex: App-owned WKWebView tabs; explicit per-conversation capability grant; snapshot/click/type/scroll/evaluate dispatch through execution host and desktop bridge. The opt-in memex-control stdio MCP exposes this path via its generic control tool.

Recommendation: Consider first-class navigation/wait/keyboard/recording actions and clearer discovery; preserve opt-in registration and app-owned tab grants.

Evidence:
- T3: [ChatView.tsx:10588-10600](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:10588) — Browser preview mounts in chat.
- T3: [tools.ts:67-91](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/mcp/toolkits/preview/tools.ts:67) — Agent navigation controls.
- T3: [tools.ts:201-244](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/mcp/toolkits/preview/tools.ts:201) — Evaluate, wait and recording tools.
- Memex: [WorkspaceBrowserAutomation.swift:68-100](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:68) — Grant and tab/action validation.
- Memex: [WorkspaceBrowserAutomation.swift:5-6](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:5) — Five supported action cases.
- Memex: [MemexApp.swift:78-81](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:78) — Starts browser execution bridge.
- Memex: [control_mcp.rs:44-74](/Users/nico/Code/memex-chat-capabilities/src/control_mcp.rs:44) — Registered agent-callable control MCP forwards method and params to the execution host; includes conversation, worktree, schedules and browser operations.
- Memex: [memex-control.rs:5-26](/Users/nico/Code/memex-chat-capabilities/src/bin/memex-control.rs:5) — Separate opt-in binary invokes control_mcp::run with optional root.
- Memex: [execution-host.md:92-96](/Users/nico/Code/memex-chat-capabilities/docs/execution-host.md:92) — Documents opt-in same-user authority and scoped browser describe/dispatch bridge.

Does not establish general OS computer-use parity. T3 device tools are separate device surfaces, not evidence of arbitrary macOS app control.

## T3-W09 — Remote execution versus remote workspace surfaces

**partial · P1 · source-only.** The high-value gap is remote workspace tooling, not remote conversation execution.

T3: Environment-scoped clients and SSH/connect/direct routes feed the same workspace panels.

Memex: Paired execution hosts create/resume durable remote conversations, but selectedWorkspace explicitly excludes host-owned conversations; Git/files/terminal panes require local workspace.

Recommendation: Route read/review/terminal operations through authenticated execution-host APIs; preserve host-local path identity.

Evidence:
- T3: [EnvironmentRow.tsx:20-44](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/settings/EnvironmentRow.tsx:20) — SSH, WSL, T3 Connect and direct remote routes.
- T3: [EnvironmentRoutesList.tsx:39-68](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/settings/EnvironmentRoutesList.tsx:39) — Prioritized connection routes and active route.
- T3: [useFileSaveCoordinator.ts:19-38](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/files/useFileSaveCoordinator.ts:19) — File operations route by environmentId.
- Memex: [ExecutionHostConnectionsView.swift:42-77](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:42) — Pair host with URL/token.
- Memex: [ExecutionHostConnectionsView.swift:80-111](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:80) — Open and create host conversations.
- Memex: [Store.swift:216-224](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Store.swift:216) — Explicit local-only workspace boundary.
- Memex: [WorkspaceTerminalView.swift:20-45](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalView.swift:20) — Terminal unavailable without local working folder.

No claim that T3 or Memex provides arbitrary managed cloud compute. Deployment availability was not tested.

## T3-W10 — Time-based and event-triggered recurring work

**partial · P2 · source-only.** Do not classify automation as absent. Event triggers and fresh-chat-per-run are the inspected gaps.

T3: Reachable settings supports scheduled/webhook tasks; dispatch either launches a new thread or queues an existing one.

Memex: Execution-host UI supports interval and weekday/local-time/timezone schedules for an existing hosted conversation, plus pause/run/delete.

Recommendation: Extend the durable host scheduler only if event-driven/fresh-task workflows are required; keep stopped queue semantics.

Evidence:
- T3: [settings.scheduled-tasks.tsx:1-8](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/routes/settings.scheduled-tasks.tsx:1) — Mounted scheduled task settings.
- T3: [ScheduledTaskService.ts:238-252](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/scheduledTasks/ScheduledTaskService.ts:238) — Webhook rotation and verification/dispatch API.
- T3: [ScheduledTaskService.ts:795-830](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/scheduledTasks/ScheduledTaskService.ts:795) — New-thread or existing-thread queued prompt path.
- Memex: [MemexApp.swift:35-35](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:35) — Execution Hosts and Schedules entry.
- Memex: [ExecutionHostConnectionsView.swift:114-174](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:114) — Interval/wall-clock editor and actions.
- Memex: [ExecutionHost.swift:328-376](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:328) — Existing conversation schedule model and actions.
- Memex: [ExecutionHost.swift:383-397](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:383) — Recurrence and skipped wall-clock handling.

Source inspection only; no runtime or test execution.

## T3-W11 — Agent-callable application toolkit breadth and registration

**partial · P2 · source-only.** Memex has a real agent-callable app-control server. The remaining differences are registration/discovery ergonomics and the narrower explicit operation inventory, including no matching PR/device/HTML app tools in the inspected host dispatcher.

T3: Authenticated MCP /mcp mounts orchestrator/thread/project/environment/attachment/worktree/PR/device/HTML/preview toolkits into server.

Memex: Implemented opt-in memex-control stdio MCP registers a generic control(method, params) tool and forwards to the execution host. Supports conversation lifecycle/control/queue/fork/delegate, schedules, workspace listing, worktree lifecycle and granted browser operations. Ordinary memex mcp stays retrieval-only.

Recommendation: Credit existing control MCP. Reuse its host owners and explicit authority boundary if adding PR/device/HTML operations or friendlier schema discovery; manual opt-in registration is not a missing API.

Evidence:
- T3: [McpHttpServer.ts:748-766](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/mcp/McpHttpServer.ts:748) — Authenticated transport and registered toolkits.
- T3: [server.ts:681-685](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/server.ts:681) — Server mounts the MCP layer.
- Memex: [ExecutionHost.swift:104-142](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:104) — Declared host capabilities and method dispatch boundary.
- Memex: [WorkspaceBrowserExecutionBridge.swift:24-43](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserExecutionBridge.swift:24) — Browser-specific app bridge dispatch.
- Memex: [control_mcp.rs:44-74](/Users/nico/Code/memex-chat-capabilities/src/control_mcp.rs:44) — Registered agent-callable control MCP forwards method and params to the execution host; includes conversation, worktree, schedules and browser operations.
- Memex: [memex-control.rs:5-26](/Users/nico/Code/memex-chat-capabilities/src/bin/memex-control.rs:5) — Separate opt-in binary invokes control_mcp::run with optional root.
- Memex: [control_mcp.rs:78-96](/Users/nico/Code/memex-chat-capabilities/src/control_mcp.rs:78) — Advertises tool capability and serves the stdio MCP transport.
- Memex: [execution-host.md:92-96](/Users/nico/Code/memex-chat-capabilities/docs/execution-host.md:92) — Documents opt-in same-user authority and scoped browser describe/dispatch bridge.

Source-only acceptance correction: Rust MCP entry point and forwarding path verified. Parent reports packaging/smoke evidence separately; this worker did not rerun it. Does not claim provider configuration auto-registers the opt-in server.

## T3-W12 — Unproven universal capabilities

**unknown · none · source-only.** Do not present a universal absence or parity claim from provider-specific or adjacent-product code.

T3: Inspected server has built-in app MCP toolkits and device tools; these do not prove a universal plugin marketplace, managed compute product or general desktop CUA.

Memex: Inspected native app has runtime clients, execution hosts and scoped browser control; opt-in memex-control exposes a generic host control MCP tool. This is not evidence of a third-party plugin marketplace or arbitrary OS application control.

Recommendation: Classify managed cloud, third-party plugin lifecycle and arbitrary OS app control as unresolved product-scope questions; composer skill discovery is owned by the other worker.

Evidence:
- T3: [McpHttpServer.ts:754-766](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/mcp/McpHttpServer.ts:754) — Bounded built-in app toolkit inventory.
- T3: [tools.ts:28-81](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/mcp/toolkits/device/tools.ts:28) — Device list/open/screenshot/close is a separate surface.
- Memex: [ExecutionHost.swift:104-140](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:104) — Bounded host capabilities.
- Memex: [WorkspaceBrowserAutomation.swift:85-95](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:85) — Only registered app-owned HTTP(S) tabs.
- Memex: [execution-host.md:92-96](/Users/nico/Code/memex-chat-capabilities/docs/execution-host.md:92) — Documents opt-in same-user authority and scoped browser describe/dispatch bridge.

Source inspection only; no runtime or test execution.
