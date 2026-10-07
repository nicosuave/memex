# platform facet evidence

## CX-P01: Install, configure, authenticate and remove plugins/custom MCP servers

**partial; P1; inspected source; no runtime action or UI verification.**

Codex: Codex-local/work-local settings route exposes plugin management subject to capability gates; custom MCP editor supports stdio and streamable HTTP, saves/uninstalls, and dispatches OAuth login. Plugin installation dispatch reaches app-server plugin/install.

Memex: Memex discovers local skill/command files and uses native provider runtimes, but has no plugin/MCP management UI. Optional runtime plugin startup support is not passed by Memex resume connection. This does not mean native provider-configured tools cannot run.

Users must manage tools through provider configuration or other apps even though local skills can be selected in Memex.

Recommendation: Prioritize a bounded per-provider MCP inventory/configuration/OAuth surface; do not invent one universal provider plugin contract.

- [Codex: use-visible-settings-sections-dc3bbd6366c8.js:1](/tmp/codex-parity-20261005/webview/assets/use-visible-settings-sections-dc3bbd6366c8.js:1) — Routes bind MCP management to Codex or local Work; skills-settings standalone route explicitly hidden. Anchor/symbol: "\"mcp-settings\":`codexOrWorkLocal`".
- [Codex: plugins-settings-146885f88b18.js:1](/tmp/codex-parity-20261005/webview/assets/plugins-settings-146885f88b18.js:1) — Custom MCP form Ui validates transport fields and delegates onSave/onUninstall; includes STDIO/streamable_http fields. Anchor/symbol: "Ui".
- [Codex: plugins-settings-146885f88b18.js:1](/tmp/codex-parity-20261005/webview/assets/plugins-settings-146885f88b18.js:1) — Per-host authenticate action executes MCP OAuth login, validates URL then opens external browser. Anchor/symbol: "sendRequest(`mcpServer/oauth/login`".
- [Codex: app-initial-f9b16fbf8fc7.js:1592](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1592) — Installation helper pun sends plugin/install to host app-server. Anchor/symbol: "pun".
- [Memex: ConversationComposerCatalog.swift:56](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposerCatalog.swift:56) — Catalog implements local skills/commands discovery, not installation/authentication. Anchor/symbol: "let skills = [".
- [Memex: InAppAgentRuntime.swift:131](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/InAppAgentRuntime.swift:131) — Memex existing Codex connection passes executable/environment only. Anchor/symbol: "try service.connectCodex(binding".
- [Memex runtime: AgentConversationService.swift:391](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:391) — Runtime capability exists but defaults to nil; no native Memex management caller. Anchor/symbol: "pluginStartupOptions: AcpClient.Configuration.StartupOptions? = nil".
- [Memex: MemexApp.swift:9](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:9) — Native Settings scene is empty; native menus route execution hosts/schedules and workspace controls. App source inventory has no plugin manager. Anchor/symbol: "Settings { EmptyView() }".

Limits: Codex plugin marketplace access depends on host/account capabilities. Installed UI confirmation belongs to root. Absence is native Memex management UI, not absence of MCP execution in its providers.

## CX-P02: Create recurring work and inspect its runs

**partial; P2; inspected source; no runtime action or UI verification.**

Codex: Codex automation source supports heartbeat linked to a thread, cron project targets, run-now/pause, run history and per-automation notification policy.

Memex: Memex already supports durable host schedules on existing conversations, interval or timezone+weekday wall-clock recurrence, edit/pause/run/delete and held-queue recovery. It lacks the standalone new-chat-per-run target and dedicated run inbox/per-schedule notification policy in inspected host model/UI.

Basic scheduling is present. Codex offers more workflow separation and run triage, especially independent recurring tasks.

Recommendation: If wanted, extend existing durable host scheduler with explicit new-conversation targets and observable run records before building a separate scheduler.

- [Codex: automations-page-abd560996342.js:1](/tmp/codex-parity-20261005/webview/assets/automations-page-abd560996342.js:1) — Automation page handles thread-linked heartbeats distinctly; later create/run-now dispatches and history actions are wired. Anchor/symbol: "s.kind===`heartbeat`".
- [Codex: automation-detail-panel-59709a145853.js:1](/tmp/codex-parity-20261005/webview/assets/automation-detail-panel-59709a145853.js:1) — Draft editor exposes per-automation notification policy including failed_runs_only; kind heartbeat and model/reasoning are separate draft fields. Anchor/symbol: "notificationPolicy".
- [Codex: automations-page-abd560996342.js:1](/tmp/codex-parity-20261005/webview/assets/automations-page-abd560996342.js:1) — Cron-specific target/project and template state separated from heartbeat. Anchor/symbol: "t.pluginTemplateId:null,target:r.projectI".
- [Memex: ExecutionHostConnectionsView.swift:114](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:114) — Reachable host sheet supports schedules and explicit recurrence, run, edit, pause and delete controls. Anchor/symbol: "Section(\"Schedules\")".
- [Memex: ExecutionHost.swift:328](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:328) — Schedule schema validates conversation ID and interval/wall-clock recurrence; saves metadata. Anchor/symbol: "case \"schedule.upsert\":".
- [Memex: ExecutionHost.swift:422](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:422) — Runs enqueue prompt into same schedule.conversationID; no new-thread target or per-run inbox target in this path. Anchor/symbol: "private func enqueueSchedule".

Limits: Codex cloud automation and ChatGPT scheduled tasks are excluded from the parity claim. Heartbeat naming alone is not a gap: Memex can recur on an existing conversation.

## CX-P03: Organize conversations with custom sections and read/unread state

**partial; P2; inspected source; no runtime action or UI verification.**

Codex: Codex sidebar custom-section create/move and local-thread read/unread actions are wired; command palette includes markThreadUnread.

Memex: Memex has pinned, archive, removed/restore, rename, manual reorder and project grouping. Its persistent Entry schema and sidebar actions have no custom section or read/unread state.

Memex covers the baseline organization workflow but cannot form cross-project custom sections or manually triage unread chats.

Recommendation: Extend the existing library metadata and sidebar rather than replacing organization.

- [Codex: app-initial-f9b16fbf8fc7.js:3788](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3788) — Custom-section submenu maps selected items to section membership. Anchor/symbol: "id:`move-to-custom-section`".
- [Codex: app-initial-f9b16fbf8fc7.js:3939](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3939) — Local-thread read/unread action dispatches conversationId/hostId, not just label text. Anchor/symbol: "case`mark-thread-read`:case`mark-thread-unread`:return".
- [Memex: ConversationLibrary.swift:21](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationLibrary.swift:21) — Persistent library Entry stores session/title/pinned/archived/removed/order only. Anchor/symbol: "struct Entry: Codable".
- [Memex: SidebarConversations.swift:206](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/SidebarConversations.swift:206) — Native management actions expose rename/pin/archive/remove/restore; no custom-section/read-state action. Anchor/symbol: "private func managementActions".
- [Memex: SidebarConversations.swift:15](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/SidebarConversations.swift:15) — Project grouping and sort already implemented. Anchor/symbol: "var sidebarGroups:".

Limits: Sidebar snooze is intentionally excluded.

## CX-P04: Customize keyboard shortcuts and application appearance

**partial; P2; inspected source; no runtime action or UI verification.**

Codex: Codex keyboard editor captures/replaces/removes bindings with conflict checks; appearance settings expose fonts, UI/code sizing, contrast and reduced motion.

Memex: Memex supplies native keyboard shortcuts and accessibility labels; Settings scene is empty, so no equivalent app-level keymap editor. Native system behavior must not be confused with absence of accessibility.

Advanced users cannot remap shortcuts inside Memex; broader accessibility quality requires runtime checks.

Recommendation: Prioritize a small keymap editor if demand warrants. Do not claim general accessibility or performance inferiority from source.

- [Codex: use-visible-settings-sections-dc3bbd6366c8.js:1](/tmp/codex-parity-20261005/webview/assets/use-visible-settings-sections-dc3bbd6366c8.js:1) — Keyboard-shortcuts settings are in Codex applicability map. Anchor/symbol: "\"keyboard-shortcuts\":`codex`".
- [Codex: keyboard-shortcuts-settings-ef4c455aeec6.js:1](/tmp/codex-parity-20261005/webview/assets/keyboard-shortcuts-settings-ef4c455aeec6.js:1) — Editor capture persists mutation through B; neighboring handlers remove/reset and conflict validation. Anchor/symbol: "onCapture:t=>{B(t,e)}".
- [Codex: general-settings-4f1402fc1fbd.js:1](/tmp/codex-parity-20261005/webview/assets/general-settings-4f1402fc1fbd.js:1) — Appearance controls include reduced motion, fonts and UI/code sizes; source presence only for unobserved controls. Anchor/symbol: "settings.general.appearance.reducedMotion.label".
- [Memex: MemexApp.swift:9](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:9) — No native settings preference editor. Anchor/symbol: "Settings { EmptyView() }".
- [Memex: MemexApp.swift:32](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/MemexApp.swift:32) — Application commands have fixed shortcuts including Cmd-J terminal. Anchor/symbol: ".keyboardShortcut(\"j\")".
- [Memex: SidebarConversations.swift:306](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/SidebarConversations.swift:306) — Native row accessibility implemented; no accessibility quality deficit inferred. Anchor/symbol: ".accessibilityElement(children: .combine)".

Limits: Appearance controls may inherit OS settings; app-specific overrides are the bounded comparison. No VoiceOver, reduced-motion, keyboard traversal, responsiveness or memory benchmark was run.

## CX-P05: Receive completion, approval and question notifications

**present; none; inspected source; no runtime action or UI verification.**

Codex: Codex settings include agent turn, permission and question notifications.

Memex: Memex has persistent per-event notification/sound preferences, suppresses viewed conversations unless opted in, deduplicates approval/question requests, and opens the selected conversation from notification.

Notification support is not a missing capability.

Recommendation: Preserve current behavior; only compare finer policy differences after UI/runtime inspection.

- [Codex: notifications-settings-9ccd12da7ea7.js:1](/tmp/codex-parity-20261005/webview/assets/notifications-settings-9ccd12da7ea7.js:1) — Codex local agent permissions and question notification settings live with agent controls. Anchor/symbol: "notifications.permissions.label".
- [Memex: ConversationNotifications.swift:110](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationNotifications.swift:110) — State transition logic emits completion/approval/question events and applies preferences. Anchor/symbol: "func receive(_ session: Session".
- [Memex: ConversationNotifications.swift:182](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationNotifications.swift:182) — User-facing controls configure delivery, completion/input and while-viewing behavior. Anchor/symbol: "struct ConversationNotificationSettingsView".

Limits: Codex labels alone do not prove notifications enabled or OS authorization granted.

## CX-P06: Choose models, reasoning and provider-specific permissions

**present; none; inspected source; no runtime action or UI verification.**

Codex: Codex model picker derives choices from supportedReasoningEfforts and handles configured/custom model providers and hidden/gated models.

Memex: Memex supports native Codex, Claude Code and explicitly configured ACP executables. Model/configuration controls are negotiated and applied through the runtime; ACP resume/configuration are negotiated and native steer unavailable.

Basic model and reasoning selection is present with provider-specific limits; do not treat provider history import as executable provider support.

Recommendation: Preserve negotiated capability boundaries and distinguish native Codex from ACP transport behavior.

- [Codex: app-initial-f9b16fbf8fc7.js:1679](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1679) — Model/effort menu derives choices from runtime supportedReasoningEfforts. Anchor/symbol: "V9r".
- [Codex: app-initial-f9b16fbf8fc7.js:1679](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1679) — Picker handles custom provider catalog flags; no claim that Codex is restricted to OpenAI-compatible defaults. Anchor/symbol: "isCustomModelProvider:s=!1".
- [Memex: ConversationProviderCatalog.swift:62](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProviderCatalog.swift:62) — Codex/Claude builtin descriptors; configured ACP catalogs appended. Anchor/symbol: "static let builtins:".
- [Memex: ConversationProviderSetupView.swift:17](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProviderSetupView.swift:17) — Native view adds/removes ACP executables with explicit home identity and negotiated capability explanation. Anchor/symbol: "Text(\"Conversation providers\")".
- [Memex: ConversationComposer.swift:149](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:149) — Composer exposes negotiated configuration fields with nonempty choices. Anchor/symbol: "ForEach(settings.configurations.filter".
- [Memex: ConversationControls.swift:37](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationControls.swift:37) — Reasoning is identified from thought_level/reasoning_effort fields. Anchor/symbol: "var reasoning: Configuration?".

Limits: Exact available models and authentication state were not queried.

## CX-P07: Let agents organize/open/manage desktop chats and panels

**partial; P2; inspected source; no runtime action or UI verification.**

Codex: Installed Codex source has desktop tool schemas for thread create/read/list/wait/send, sidebar sections and open_in_codex; tools are host/context gated.

Memex: Memex already supplies a separate opt-in memex-control stdio MCP server forwarding durable conversation/queue/schedule/worktree/browser operations to the execution host. The explicit host allowlist lacks sidebar organization/read state and desktop panel/preferences operations. The gap is desktop-shell integration, not lack of an app-control MCP.

Memex already exposes execution control as MCP; agents have narrower access to desktop shell organization and panel presentation.

Recommendation: If needed, expose a small authenticated app-control adapter over existing native organization/panel operations, with explicit user authorization for cross-chat messaging.

- [Codex: app-initial-f9b16fbf8fc7.js:4604](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:4604) — Desktop sidebar tool schemas and siblings are declared in B0s. Anchor/symbol: "B0s".
- [Codex: app-initial-f9b16fbf8fc7.js:4604](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:4604) — Desktop thread tools and schema declarations include list/wait/create/read/send. Anchor/symbol: "D2s".
- [Codex: app-initial-f9b16fbf8fc7.js:3695](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:3695) — Panel opening tool schema supports files, browser, terminal, review and pages; Pages-specific capabilities excluded from recommendation. Anchor/symbol: "xda=`open_in_codex`".
- [Memex: ExecutionHost.swift:137](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:137) — Explicit mutation allowlist shows supported Memex host controls and bounds negative claim. Anchor/symbol: "let allowed: Set<String>".
- [Memex: ExecutionHost.swift:101](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:101) — Capability announcement covers conversations, queues, schedules, workspace/worktree/context fork and browser availability. Anchor/symbol: "case \"host.info\":".
- [Memex: control_mcp.rs:44](/Users/nico/Code/memex-chat-capabilities/src/control_mcp.rs:44) — Opt-in MCP forwards method+params to execution_host::request; existing tool describes full execution controls. Anchor/symbol: "ControlServer.control".

Limits: Codex schema presence is source evidence, not proof every tool is exposed in every thread or account. No claim that Pages/Work/ChatGPT-only integrations must be copied into a coding client.
