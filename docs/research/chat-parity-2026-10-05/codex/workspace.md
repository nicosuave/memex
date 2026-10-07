# workspace facet evidence

## codex-workspace-01: Projects and managed checkout creation

**present; none; source inspection; no runtime/UI test.**

Codex: Shipped host-aware worktree query and worktree settings.

Memex: Saved local projects support existingDirectory/newWorktree, default base ref and setup command; managed lifecycle UI exists.

Do not count basic project or worktree support as missing.

Recommendation: Retain existing implementation; compare ergonomics separately.

- [Codex: use-codex-worktrees-3c6a69cec5c9.js:1](/tmp/codex-parity-20261005/webview/assets/use-codex-worktrees-3c6a69cec5c9.js:1) — Worktree inventory requests include hostConfig and worktreesRoot. Anchor/symbol: "method:`codex-worktrees`".
- [Memex: LocalProjects.swift:4](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LocalProjects.swift:4) — Project workspace modes, base ref and setup command. Anchor/symbol: "ConversationWorkspaceMode / LocalProject".
- [Memex: WorkspaceLifecycleView.swift:25](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceLifecycleView.swift:25) — Checkout attach/archive/reattach actions. Anchor/symbol: "WorkspaceLifecycleView".

Limits:

## codex-workspace-02: Automatic pruning with recoverable snapshots

**partial; P2; source inspection; no runtime/UI test.**

Codex: Worktree settings expose automatic deletion, retention limit and snapshot-before-delete recovery; actual native snapshot implementation not in extracted renderer.

Memex: Archive retains all files. Cleanup refuses dirty, untracked or ignored files; reattach recreates from retained branch.

Memex protects dirty work by retaining disk usage; it does not implement the compared snapshot-and-prune lifecycle.

Recommendation: Consider explicit snapshot recovery before optional pruning. Preserve ignored-file safety guarantees.

- [Codex: worktrees-settings-page-21ceeff5dac4.js:2](/tmp/codex-parity-20261005/webview/assets/worktrees-settings-page-21ceeff5dac4.js:2) — Installed UI describes recoverable snapshot-before-prune semantics. Anchor/symbol: "ChatGPT snapshots worktrees before deleting".
- [Codex: worktrees-settings-page-21ceeff5dac4.js:2](/tmp/codex-parity-20261005/webview/assets/worktrees-settings-page-21ceeff5dac4.js:2) — Delete handler calls native worktree-delete after archiving linked conversations. Anchor/symbol: "await ne(`worktree-delete`".
- [Memex: ManagedWorkspaceLifecycle.swift:76](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceLifecycle.swift:76) — Archive only changes lifecycle metadata; cleanup requires empty status; reattach uses git worktree add from branch. Anchor/symbol: "setArchived / removeCleanCheckout / reattach".

Limits: Codex snapshot coverage and recovery correctness are UI/native-contract evidence, not runtime verification. This is an optional disk-management enhancement, not a data-loss defect.

## codex-workspace-03: Commit, push, draft PR and local diffs

**present; none; source inspection; no runtime/UI test.**

Codex: Git actions expose commit/push/create PR and branch actions.

Memex: Changes view has stage/unstage, split diffs and branch comparison; Git actions execute commit/push/gh draft PR.

Core Git workflow is present; lack of Git support is an incorrect gap claim.

Recommendation: Keep focused improvements scoped to concrete review or branch UX differences.

- [Codex: local-conversation-git-actions-290387202be8.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-git-actions-290387202be8.js:1) — Reachable checkout action menu. Anchor/symbol: "defaultMessage:`Git actions`".
- [Memex: WorkspaceGitActionsView.swift:19](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:19) — Commit, push and draft pull-request actions. Anchor/symbol: "Action".
- [Memex: WorkspaceGit.swift:54](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGit.swift:54) — Actual Git and gh execution implementation. Anchor/symbol: "commit / push / createPullRequest".
- [Memex: WorkspaceChangesView.swift:64](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceChangesView.swift:64) — Current/branch and split-diff controls. Anchor/symbol: "scope / splitDiff".

Limits:

## codex-workspace-04: GitHub PR review threads and richer diff controls

**partial; P2; source inspection; no runtime/UI test.**

Codex: PR renderer has reply/resolved threads and diff options: hide whitespace/imports, word diffs and full-file loading.

Memex: Workspace review is local current/branch/checkpoint comparison plus file-level comment context added to chat; PR action creates a draft using gh.

Local review is implemented, but the inspected native workspace surface has no equivalent first-class PR thread workflow.

Recommendation: Prioritize PR reading and linked review context before adding external posting actions.

- [Codex: pull-request-code-review-28722845bd32.js:2](/tmp/codex-parity-20261005/webview/assets/pull-request-code-review-28722845bd32.js:2) — PR review thread reply UI. Anchor/symbol: "defaultMessage:`Reply`".
- [Codex: pull-request-code-review-28722845bd32.js:1](/tmp/codex-parity-20261005/webview/assets/pull-request-code-review-28722845bd32.js:1) — PR diff filtering controls. Anchor/symbol: "defaultMessage:`Hide white space`".
- [Memex: WorkspaceReview.swift:3](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceReview.swift:3) — Local scopes and review context contract. Anchor/symbol: "WorkspaceDiffScope / WorkspaceReviewContext".
- [Memex: WorkspaceChangesView.swift:149](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceChangesView.swift:149) — Review comment adds local diff context to the active chat. Anchor/symbol: "Add review to chat".
- [Memex: WorkspaceGitActionsView.swift:19](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:19) — Git action boundary includes create PR but no open/reply PR workflow. Anchor/symbol: "Action".

Limits: No GitHub write action was performed. Provider shell/MCP access can offer PR operations without native product UI. Native workspace UI boundary, not a claim that all provider tools lack GitHub.

## codex-workspace-05: Local file browsing/editing

**present; none; source inspection; no runtime/UI test.**

Codex: Installed file-source editor component exists, including executor-aware file availability.

Memex: Mounted Files pane supports browsing, create, edit/save, conflict handling, previews and adding to chat.

Native file editing is already present.

Recommendation: Retain existing implementation; remote availability is tracked separately.

- [Codex: editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1](/tmp/codex-parity-20261005/webview/assets/editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1) — File editor handles environment-dependent file access. Anchor/symbol: "Files will be available when the task has an environment".
- [Memex: WorkspacePanelView.swift:38](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspacePanelView.swift:38) — Files pane is wired into workspace panel. Anchor/symbol: "WorkspaceFilesView".
- [Memex: WorkspaceFilesView.swift:62](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceFilesView.swift:62) — Editor and conflict UI. Anchor/symbol: "Save / Keep my edits / Use disk".
- [Memex: WorkspaceFilesView.swift:224](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceFilesView.swift:224) — Write/conflict path. Anchor/symbol: "save".

Limits:

## codex-workspace-06: Persistent local workspace terminals

**present; none; source inspection; no runtime/UI test.**

Codex: Terminal tab tracks session/environment, reconnect states and fresh-shell semantics.

Memex: Ghostty-backed per-worktree sessions survive pane/chat switches; tabs and split layouts, scrollback capture, save and context actions.

Basic integrated/persistent terminal is not a missing capability.

Recommendation: Do not reimplement local terminal; remote scope is separate.

- [Codex: terminal-tab-e527a3db0b81.js:2](/tmp/codex-parity-20261005/webview/assets/terminal-tab-e527a3db0b81.js:2) — Terminal UI distinguishes reconnection from replay. Anchor/symbol: "Opening a fresh shell. Previous commands won’t be rerun".
- [Memex: WorkspaceTerminalGroup.swift:5](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalGroup.swift:5) — Process ownership and retained sessions; snapshots are never replayed. Anchor/symbol: "WorkspaceTerminalGroup".
- [Memex: WorkspaceTerminalView.swift:86](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalView.swift:86) — New shell, layouts, selection/scrollback context, restart/close. Anchor/symbol: "WorkspaceTerminalGroupView".
- [Memex: WorkspaceTerminalSession.swift:91](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalSession.swift:91) — Actual Ghostty surface configured with working directory. Anchor/symbol: "TerminalSurfaceOptions".

Limits: Process retention within app lifetime differs from saved scrollback after restart; no claim of immortal shell sessions.

## codex-workspace-07: In-app tabs and bounded agent browser interaction

**present; none; source inspection; no runtime/UI test.**

Codex: Browser settings and native computer/browser bridge are shipped; policy/gate checks exist.

Memex: Chat-owned browser tabs explicitly grant snapshot/click/type/scroll and optional JS evaluation; revoke is exposed.

Memex has real app-owned browser control, not just static web previews.

Recommendation: Preserve per-chat explicit grants. Assess external-browser/native-app control separately.

- [Codex: browser-use-settings-visibility-a47f5656ea55.js:1](/tmp/codex-parity-20261005/webview/assets/browser-use-settings-visibility-a47f5656ea55.js:1) — Browser settings visibility is gated. Anchor/symbol: "enabled:p,isLoading:m".
- [Memex: WorkspaceBrowserTabView.swift:15](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserTabView.swift:15) — Tabs plus explicit agent automation grant/revoke UI. Anchor/symbol: "WorkspaceBrowserTabView".
- [Memex: WorkspaceBrowserAutomation.swift:85](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:85) — Dispatches allowed snapshot/click/type/scroll/evaluate operations. Anchor/symbol: "dispatch".

Limits: No signed-in browser interaction was performed; actual provider invocation depends on execution bridge and grant.

## codex-workspace-08: Native apps and external browser control

**partial; P2; source inspection; no runtime/UI test.**

Codex: Shipped Computer use settings include Any App, Chrome, Edge, Safari and per-app approvals; native settings bridge and policy checks are present.

Memex: Inspected desktop execution API exposes browser.describe/browser.dispatch into chat-owned tabs; grant text explicitly excludes other apps and conversations.

The native Memex control surface stops at its own browser tabs.

Recommendation: Treat broader desktop control as an explicitly permissioned integration, if desired.

- [Codex: computer-use-settings-8cfbd3f42052.js:1](/tmp/codex-parity-20261005/webview/assets/computer-use-settings-8cfbd3f42052.js:1) — Native/external app computer-use settings. Anchor/symbol: "defaultMessage:`Any App`".
- [Codex: computer-use-app-approvals-query-a8b3c61a321c.js:1](/tmp/codex-parity-20261005/webview/assets/computer-use-app-approvals-query-a8b3c61a321c.js:1) — Native bridge reads app approval policy. Anchor/symbol: "n.computerUseSettings.getAppApprovals()".
- [Codex: computer-use-app-approvals-query-a8b3c61a321c.js:1](/tmp/codex-parity-20261005/webview/assets/computer-use-app-approvals-query-a8b3c61a321c.js:1) — Computer-use requirements restrict locked-computer capability. Anchor/symbol: "allowLockedComputerUse!==!1".
- [Memex: WorkspaceBrowserTabView.swift:35](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceBrowserTabView.swift:35) — Control is explicitly limited to app-owned tabs. Anchor/symbol: "automation grant message".
- [Memex: ExecutionHost.swift:137](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:137) — Native execution mutation boundary exposes browser.dispatch, not desktop-app control. Anchor/symbol: "allowed".

Limits: Codex feature visibility is policy/plugin/platform dependent; no live enablement asserted. External provider-installed MCP tools are outside the native inventory boundary. Runtime generated computer_use protocol types alone do not implement this UI.

## codex-workspace-09: Remote execution and integrated SSH onboarding

**partial; P2; source inspection; no runtime/UI test.**

Codex: Connections UI includes manual SSH host/port/identity and remote CLI sign-in; account-device connection paths are also bundled.

Memex: Paired execution hosts are implemented via explicit HTTPS or loopback-tunnel URL plus token; verify host identity, list/create/resume conversations and manage remote worktrees.

Multi-host execution already exists; the difference is integrated SSH/account discovery and setup ergonomics.

Recommendation: Improve onboarding if required without replacing the execution-host ownership protocol.

- [Codex: remote-connection-editor-dialog-1d8f37e9e898.js:1](/tmp/codex-parity-20261005/webview/assets/remote-connection-editor-dialog-1d8f37e9e898.js:1) — SSH configuration form. Anchor/symbol: "defaultMessage:`SSH authentication method`".
- [Codex: remote-connections-settings-d2dabff157cf.js:1](/tmp/codex-parity-20261005/webview/assets/remote-connections-settings-d2dabff157cf.js:1) — Remote CLI authentication workflow. Anchor/symbol: "Authenticate the Codex CLI on this remote machine to continue".
- [Memex: ExecutionHostConnectionsView.swift:42](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:42) — Explicit endpoint/token pairing workflow. Anchor/symbol: "Execution hosts".
- [Memex: ExecutionHostConnection.swift:67](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnection.swift:67) — HTTPS or loopback HTTP endpoint validation. Anchor/symbol: "validatedEndpoint".
- [Memex: RemoteConversationRuntime.swift:20](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RemoteConversationRuntime.swift:20) — Imports/resumes exact host-native conversation identity. Anchor/symbol: "connect".

Limits: Codex device discovery availability is gate/account dependent; no connectivity test performed.

## codex-workspace-10: Files, review and terminals for remote conversations

**missing; P1; source inspection; no runtime/UI test.**

Codex: Non-durable terminal and editable-file branches pass explicit hostId to their implementations; local-conversation Git action queries include hostConfig/cwd. Durable cloud-specific branches are separate and are not the evidence for SSH host parity.

Memex: selectedWorkspace is nil for any paired-host or server-owned conversation. Files/changes/terminal panes require selectedWorkspace and terminal implementation requires a local directory.

A remote chat can execute, but native workspace inspection and shell UX disappear rather than switching to host-aware APIs.

Recommendation: Add host-scoped files/diff/terminal transport and bind the existing panels to verified remote workspace identity.

- [Codex: terminal-tab-e527a3db0b81.js:2](/tmp/codex-parity-20261005/webview/assets/terminal-tab-e527a3db0b81.js:2) — Non-durable terminal qe passes explicit hostId through to terminal component; durable-specific je is a separate branch. Anchor/symbol: "conversationTitle:i,cwd:a,hostId:o,initialPromptHint:k".
- [Codex: editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1](/tmp/codex-parity-20261005/webview/assets/editable-review-file-source-tab-content.electron-3af1b5dc3d31.js:1) — Non-durable file/editor branch De passes explicit hostId to editor; cloud environment branch is separate. Anchor/symbol: "commentProps:N,conversationId:k,cwd:n,editor:L,headerActions:r,hideFileNavigation:a,hostId:o".
- [Codex: local-conversation-git-actions-290387202be8.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-git-actions-290387202be8.js:1) — Local-conversation Git action state requests include hostConfig with cwd, including unstaged inspection. Anchor/symbol: "hostConfig:g,includeUnstaged:!0".
- [Memex: Store.swift:216](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Store.swift:216) — Explicitly excludes server-owned and paired-host sessions from local file access. Anchor/symbol: "selectedWorkspace / canAccessLocalFiles".
- [Memex: WorkspacePanelView.swift:17](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspacePanelView.swift:17) — Changes and files require selectedWorkspace. Anchor/symbol: "WorkspacePanelView".
- [Memex: WorkspaceTerminalView.swift:20](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/WorkspaceTerminalView.swift:20) — Terminal requires selectedWorkspace; otherwise unavailable. Anchor/symbol: "WorkspaceTerminalView".

Limits: Codex backend terminal/file operation correctness is not runtime-tested. Memex absence is established at the exact UI gating boundary, not inferred from search.

## codex-workspace-11: Move an existing chat and Git state between checkout/worktree/host

**missing; P2; source inspection; no runtime/UI test.**

Codex: Local-thread environment menu exposes Move to main checkout/Move to worktree; handoff dialog has remote destinations and dirty-workspace checks.

Memex: Execution API has context fork/delegate and worktree create/archive/reattach/cleanup; no same-chat workspace/host migration mutation. Native forks explicitly create context handoffs.

Context sharing and worktree management do not provide identity-preserving task-and-Git migration.

Recommendation: If needed, design explicit handoff preserving native session, repo state and host ownership; do not label context forks equivalent.

- [Codex: local-conversation-thread-604c26084272.js:8](/tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8) — Reachable thread environment action. Anchor/symbol: "defaultMessage:`Move to main checkout`".
- [Codex: local-conversation-thread-604c26084272.js:8](/tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8) — Cross-host handoff state in local thread UI. Anchor/symbol: "to-host-worktree".
- [Codex: thread-handoff-modal-63cd776f0ead.js:1](/tmp/codex-parity-20261005/webview/assets/thread-handoff-modal-63cd776f0ead.js:1) — Handoff checks destination cleanliness. Anchor/symbol: "Stash or commit your local changes to hand off".
- [Memex: ExecutionHost.swift:137](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:137) — Complete mutation inventory lacks move/transfer operation. Anchor/symbol: "allowed".
- [Memex: ExecutionHost.swift:218](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:218) — Fork/delegate creates new conversation with explicit context handoff rather than moving same task. Anchor/symbol: "conversation.fork / context_handoff".

Limits: Cross-host Codex handler backend is not included in renderer inventory; UI/workflow wiring is observed, not end-to-end migration.

## codex-workspace-12: Hosted cloud task and browser surface

**unknown; none; source inspection; no runtime/UI test.**

Codex: Bundled cloud browser preview has screenshot timeline, live panel and ChatGPT Work labels.

Memex: Native app has server-owned/paired execution. Separate runtime repository contains hosted infrastructure, but account provisioning and this native app’s cloud workflow were not fully traced here.

Do not treat bundled ChatGPT Work cloud UI as universal Codex capability or infer hosted Memex absence.

Recommendation: Keep as an explicit unresolved deployment/product-boundary comparison.

- [Codex: cloud-browser-preview-6a5c057f3897.js:1](/tmp/codex-parity-20261005/webview/assets/cloud-browser-preview-6a5c057f3897.js:1) — Cloud browser replay/live surface in installed bundle. Anchor/symbol: "Cloud browser activity timeline".
- [Memex: Store.swift:216](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Store.swift:216) — Native app distinguishes server-owned execution. Anchor/symbol: "canAccessLocalFiles".

Limits: ChatGPT Work-specific chunk; gate/entitlement and Codex applicability unverified. No cloud runtime observation.
