# Installed Codex versus Memex: source inventory

Installed ChatGPT **26.930.51102 (13100)**, bundle **com.openai.codex**. Whole app.asar SHA-256 `a159b8f5b78ed1ba89fc70d5c8448d822a46c4fc2a4a9f18ec348f3cc2f6b8c9`; header hash matches plist. Memex `17e958778a7398db8238cc9c8415ca12649067e5`; optional runtime `7818518b094f44911aecfec7dbeb905c57ea24a9`. Installed archive hash remained unchanged.

Research only: source extraction to owned scratch and source reads; no source product edits, app starts/stops, prompts, tests, builds or publication. Actual installed-account UI and runtime success are not inferred from bundled source.

Memex already has substantial composer, queue, recovery, plan-step, question, provider, organization, notification, local workspace and remote execution support. The clearest defect is content-bearing MCP form replies: the Codex adapter accepts with null content. Other significant gaps are native interactive MCP results, plugin/MCP management, and remote files/diffs/terminals. Document-plan editing, richer PR review, SSH onboarding, host handoff, custom sidebar sections and shortcut remapping are narrower workflow enhancements.

Scope excludes sidebar snooze and does not import ChatGPT Work/Pages/consumer-only features into Codex parity. Gated native-computer control and cloud-specific chunks are explicitly qualified. Memex's opt-in `memex-control` MCP already exists.

| Capability | Status | Priority | ID |
|---|---|---|---|
| Files, images, mentions, captured context | present | none | codex-conversation-01 |
| Annotation-aware attachment editing | partial | P2 | codex-conversation-02 |
| Integrated dictation and dictation recovery | missing | P2 | codex-conversation-03 |
| Queue, reorder, edit, steer, stop | present | none | codex-conversation-04 |
| Queued message to side chat | missing | P3 | codex-conversation-05 |
| Uncertain delivery and recovery | present | none | codex-conversation-06 |
| Editable plan document workflow | partial | P2 | codex-conversation-07 |
| Subagent overview depth | partial | P2 | codex-conversation-08 |
| Native fork, rewind and context branch | present | none | codex-conversation-09 |
| Ordinary structured questions | present | none | codex-conversation-10 |
| MCP structured form elicitation | defect | P1 | codex-conversation-11 |
| Policy amendment and reviewer detail UI | partial | P2 | codex-conversation-12 |
| Core readable transcript, search and navigation | present | none | codex-conversation-13 |
| Interactive MCP app results | missing | P1 | codex-conversation-14 |
| Projects and managed checkout creation | present | none | codex-workspace-01 |
| Automatic pruning with recoverable snapshots | partial | P2 | codex-workspace-02 |
| Commit, push, draft PR and local diffs | present | none | codex-workspace-03 |
| GitHub PR review threads and richer diff controls | partial | P2 | codex-workspace-04 |
| Local file browsing/editing | present | none | codex-workspace-05 |
| Persistent local workspace terminals | present | none | codex-workspace-06 |
| In-app tabs and bounded agent browser interaction | present | none | codex-workspace-07 |
| Native apps and external browser control | partial | P2 | codex-workspace-08 |
| Remote execution and integrated SSH onboarding | partial | P2 | codex-workspace-09 |
| Files, review and terminals for remote conversations | missing | P1 | codex-workspace-10 |
| Move an existing chat and Git state between checkout/worktree/host | missing | P2 | codex-workspace-11 |
| Hosted cloud task and browser surface | unknown | none | codex-workspace-12 |
| Install, configure, authenticate and remove plugins/custom MCP servers | partial | P1 | CX-P01 |
| Create recurring work and inspect its runs | partial | P2 | CX-P02 |
| Organize conversations with custom sections and read/unread state | partial | P2 | CX-P03 |
| Customize keyboard shortcuts and application appearance | partial | P2 | CX-P04 |
| Receive completion, approval and question notifications | present | none | CX-P05 |
| Choose models, reasoning and provider-specific permissions | present | none | CX-P06 |
| Let agents organize/open/manage desktop chats and panels | partial | P2 | CX-P07 |

## codex-conversation-01: Files, images, mentions, captured context

**present; none; source inspection; no runtime/UI execution.**

Codex: Reachable composer context model carries image/file/pasted-text/selection/MCP/annotation attachment types.

Memex: Native composer has file chooser, image/file paste/drop, file/chat mentions, slash commands, prompt stash and recall; captured contents are immutable and provider validated.

The ordinary context-composition workflow is already implemented. Richer structured contexts are assessed separately.

Recommendation: Retain existing composer; do not label attachments or mentions missing.

- [Codex: app-initial-f9b16fbf8fc7.js:1654](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1654) — Composer state has typed context collections. Anchor/symbol: "mcpAppModelContextAttachments:[],selectedTextAttachments:".
- [Memex: ConversationComposer.swift:75](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:75) — Reachable native composer wires attachments, mentions, queue and steer. Anchor/symbol: "AcpComposerView / onPasteCommand / onDrop".
- [Memex: ConversationComposer.swift:186](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:186) — Prompt stash, recall, mentions and commands are exposed. Anchor/symbol: "inputMenu".
- [Memex: ConversationComposer.swift:413](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:413) — Chat excerpt and file mentions enter attached context. Anchor/symbol: "mentionSuggestions / selectMention".
- [Memex: ConversationPromptContext.swift:10](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptContext.swift:10) — Captured contents become real blocks and enforce capability/size limits. Anchor/symbol: "ConversationAttachment.text / validate".

Limits: Provider capability and live account availability were not exercised.

## codex-conversation-02: Annotation-aware attachment editing

**partial; P2; source inspection; no runtime/UI execution.**

Codex: Composer renders response annotations and image/diff/browser comment attachment collections with edit/navigation/removal callbacks.

Memex: Memex provides text/image/file context and source labels; native composer and attachment APIs do not model response annotation identities or image comment drafts.

Users can supply the underlying text or screenshot, but cannot preserve and revise Codex-style contextual annotations in the composer.

Recommendation: Add annotation identity/location only for concrete supported Memex source surfaces; preserve current immutable capture behavior.

- [Codex: app-primary-c0280d43ce72.js:116](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:116) — Composer context UI exposes annotation edit and navigation callbacks. Anchor/symbol: "onEditResponseTextAnnotation:".
- [Codex: queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — Queued messages retain image comment collections and annotation counts. Anchor/symbol: "imageCommentDrafts?.reduce".
- [Memex: ConversationComposer.swift:70](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:70) — Memex attachment UI maps only title/path/id and removal. Anchor/symbol: "contextItems / mentionConfiguration".
- [Memex: ConversationPromptContext.swift:10](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptContext.swift:10) — Native capture boundary stores generic text or image blocks, not editable annotation structures. Anchor/symbol: "ConversationAttachment.text / image / validate".

Limits: Bounded absence: native ConversationComposer, ConversationPromptContext, ConversationAttachment and their public context item mapping. This does not claim no selection capture: terminal/browser selections have existing capture paths.

## codex-conversation-03: Integrated dictation and dictation recovery

**missing; P2; source inspection; no runtime/UI execution.**

Codex: Shared local composer footer accepts dictation controls; implementation has audio history read/retry/download and transcription.

Memex: Native Memex composer and SQACPUI input surface expose text, mentions, file/image context and send/stop; no app-owned recording/transcription flow is wired.

System text-field dictation is different from an integrated recording/retry workflow.

Recommendation: Consider a dedicated native dictation path if voice input is a product goal.

- [Codex: app-primary-c0280d43ce72.js:118](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:118) — Local composer footer receives dictation state and controls. Anchor/symbol: "composerInput:i,dictation:hn,dictationEnabled:n".
- [Codex: app-primary-c0280d43ce72.js:7](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:7) — Dictation history retry reads audio and transcribes it. Anchor/symbol: "async function Yze(e,t)".
- [Memex: ConversationComposer.swift:42](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:42) — Full composer input/callback inventory contains no dictation integration. Anchor/symbol: "AcpComposerView".
- [Memex runtime: AcpComposerView.swift:1](/Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:1) — Shared native composer types and implementation inspected; no dictation/speech recording APIs found. Anchor/symbol: "AcpComposerPendingApprovalItem / input types".

Limits: Codex dictation is feature/platform/service dependent; installed source proves integration, not that this account currently has it. No claim is made that macOS system dictation cannot type into Memex.

## codex-conversation-04: Queue, reorder, edit, steer, stop

**present; none; source inspection; no runtime/UI execution.**

Codex: Reachable queue has reorder, edit, delete, steer, default queue/steer toggle and pause after interruption.

Memex: Same core operations exist in ConversationQueueView and LiveConversation, including persisted queue and explicit pause/resume.

This is substantial current parity, with provider support checked before live steering.

Recommendation: Preserve the current queue state machine and tests; focus any polish on interaction details.

- [Codex: queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — Queue pauses and can resume after interruption. Anchor/symbol: "Queue paused because you interrupted".
- [Codex: queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — Send-now action steers without interrupting. Anchor/symbol: "Submit without interrupting the model".
- [Memex: ConversationQueueView.swift:12](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQueueView.swift:12) — Edit, move earlier/later, promote, remove and pause/resume are reachable. Anchor/symbol: "ConversationQueueView.body".
- [Memex: LiveConversation.swift:587](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:587) — Queue persists, checks capability, and dispatches prompt versus steer. Anchor/symbol: "enqueueDraft / steerDraft / dispatchQueued".
- [Memex: ConversationQueueTests.swift:79](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:79) — Existing tests cover persistence, stop settlement, steering identity and uncertain recovery; inspected, not run. Anchor/symbol: "queueEditsAndOrderSurviveRelaunchWithoutDispatch / recoveryDoesNotReplayAnUnconfirmedDequeuedCommand".

Limits: Codex uses drag reorder; Memex exposes explicit earlier/later commands. Memex steering is gated by snapshot.canSteer and request/settings state.

## codex-conversation-05: Queued message to side chat

**missing; P3; source inspection; no runtime/UI execution.**

Codex: Queue action Open in side chat is supplied conditionally by the composer callback.

Memex: Queue action inventory supports edit/reorder/steer/send/remove/pause, but has no branch/side-chat action.

Memex users must manually move that prompt to a new conversation.

Recommendation: If adopting side chats, reuse queue attachment identity and perform a reviewed transfer rather than duplicate dispatch.

- [Codex: queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — Queue menu optionally starts a queued message as a side chat. Anchor/symbol: "Open in side chat".
- [Codex: app-primary-c0280d43ce72.js:169](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:169) — Caller gates this action; it is not universal. Anchor/symbol: "onOpenInSideChatMessage:oc?xl:void 0".
- [Memex: ConversationQueueView.swift:28](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQueueView.swift:28) — Complete native queue menu has no side-chat operation. Anchor/symbol: "Menu".

Limits: Codex gate oc was not resolved to live entitlement; record as gated shipped source, not observed universal availability.

## codex-conversation-06: Uncertain delivery and recovery

**present; none; source inspection; no runtime/UI execution.**

Codex: Queue distinguishes sending/pending/outcome-unknown; warns to check conversation before deleting an unconfirmed message.

Memex: Memex holds uncertain prompts and queue, preserves drafts and exposes reconnect/inspection; never auto-replays unconfirmed commands.

Do not count recovery as absent; Memex deliberately makes uncertainty visible.

Recommendation: Keep recovery messages and provider identity checks.

- [Codex: queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — User-facing unknown-outcome state. Anchor/symbol: "Delivery could not be confirmed. Check the conversation before deleting this saved message".
- [Memex: ConversationRecoveryView.swift:4](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationRecoveryView.swift:4) — Distinct archive, authentication, uncertain delivery and reconnect states. Anchor/symbol: "ConversationRecoveryKind / detail".
- [Memex: LiveConversation.swift:484](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:484) — Explicit pending-prompt reconciliation. Anchor/symbol: "restoreUnsentPrompt / restorePendingDraft / reconcilePendingPrompt".
- [Memex: ConversationQueueTests.swift:192](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:192) — Existing no-replay regression test. Anchor/symbol: "recoveryDoesNotReplayAnUnconfirmedDequeuedCommand".

Limits: Provider capability and live account availability were not exercised.

## codex-conversation-07: Editable plan document workflow

**partial; P2; source inspection; no runtime/UI execution.**

Codex: Completed plan text can open an editable PLAN.md under the host Codex home; saves existing edits and falls back to a read-only panel when file writing is unavailable.

Memex: Memex projects structured plan steps from update_plan/TodoWrite/native plan arrays and offers Refine/Implement/Implement in new conversation. Markdown plan text can be shown as tool detail, but does not become an editable plan document.

Planning exists, but document-oriented planning and direct editing are not equivalent to the step summary.

Recommendation: Preserve full plan identity/text and add an editable plan view with save/implementation provenance.

- [Codex: local-conversation-plan-model-5c6fbe48a2a4.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-plan-model-5c6fbe48a2a4.js:1) — Completed plan text is selected from conversation items. Anchor/symbol: "r.type!==`plan`".
- [Codex: local-conversation-thread-604c26084272.js:8](/tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8) — Thread summary opens selected plan. Anchor/symbol: "Pv(t,{content:r.content".
- [Codex: plan-side-panel-2f427e3e9737.js:1](/tmp/codex-parity-20261005/webview/assets/plan-side-panel-2f427e3e9737.js:1) — Plan panel creates/opens editable file with conflict and save handling; fallback read-only. Anchor/symbol: "m=l(p,`PLAN.md`)".
- [Memex: ConversationWork.swift:27](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:27) — Only structured steps become the plan summary. Anchor/symbol: "ConversationWork.project".
- [Memex: ConversationWork.swift:117](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:117) — Refine/Implement buttons attach step text to a draft. Anchor/symbol: "ConversationWorkView".
- [Memex runtime: CodexAppServerTransport.swift:2405](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2405) — Markdown text is retained in details; structured payload is derived from arrays. Anchor/symbol: "case \"plan\"".

Limits: Editable Codex plan requires workspaceFiles.write and a supported host. Do not claim the existing Memex planning mode or implementation actions are missing. Acceptance review: categorized as a workflow enhancement (P2), not a correctness defect; existing Memex structured planning and generic file editor remain present.

## codex-conversation-08: Subagent overview depth

**partial; P2; source inspection; no runtime/UI execution.**

Codex: Dedicated subagent panel splits active/done and opens selected child details including model/reasoning; local thread summary links into it.

Memex: Memex derives child identity/prompt/status/parent, offers Open and Parent for indexed sessions, and displays unavailable status honestly. It has no dedicated active/done side panel or per-child model/effort fields in this view.

Child work is visible today, but navigation and metadata presentation are shallower.

Recommendation: Extend the existing child summary using authoritative provider state; avoid inferring completion from tool return.

- [Codex: local-conversation-subagents-panel-tab-797735e5898b.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1) — Active/completed sections in a dedicated panel. Anchor/symbol: "localConversation.subagentsPanel.active".
- [Codex: local-conversation-subagents-panel-tab-797735e5898b.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1) — Child detail header shows model and effort. Anchor/symbol: "localConversation.subagentsPanel.modelAndReasoningEffort".
- [Memex: ConversationWork.swift:17](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:17) — Memex child summary data consists of id, prompt, status, parentID. Anchor/symbol: "Agent / State".
- [Memex: ConversationWork.swift:137](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:137) — Open/Parent require matching indexed child sessions. Anchor/symbol: "ForEach(state.agents)".
- [Memex: InAppConversationTarget.swift:22](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/InAppConversationTarget.swift:22) — Standalone resume of subagents is disallowed; parent is the control surface. Anchor/symbol: "unavailableReason".

Limits: Codex child interactivity is explicitly canInteract-gated. No claim of universal direct child messaging is made. Memex parent-owned transport can handle child requests.

## codex-conversation-09: Native fork, rewind and context branch

**present; none; source inspection; no runtime/UI execution.**

Codex: Fork service retains history, supports same-directory and asynchronous worktree fork, and distinguishes incomplete setup.

Memex: Memex has native fork/rewind at valid boundaries, context branch/provider transition/new worktree, persisted relationships and recovery journal.

History operations are not a Memex gap, though native worktree fork and context-with-worktree are distinct workflows.

Recommendation: Keep the explicit native-history versus captured-context distinction.

- [Codex: threads-fork-d7fba758d383.js:1](/tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1) — Same-directory native fork. Anchor/symbol: "case`same-directory`".
- [Codex: threads-fork-d7fba758d383.js:1](/tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1) — Asynchronous worktree fork retains working-tree starting state. Anchor/symbol: "case`worktree`".
- [Memex: ConversationHistoryActions.swift:19](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationHistoryActions.swift:19) — Reachable branch/fork/rewind actions and recovery notice. Anchor/symbol: "ConversationHistoryActions.body".
- [Memex: StoreConversationHistory.swift:42](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/StoreConversationHistory.swift:42) — Context branch can request new worktree; native mutation records relationship. Anchor/symbol: "branchWithContext / mutateNativeHistory".
- [Memex runtime: CodexAppServerTransport.swift:532](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:532) — Native fork/revert requires compatibility and verifies chosen cutoff. Anchor/symbol: "mutateHistory / fork cutoff verification".

Limits: Parity is at workflow family level. Memex native mutation requires idle state, empty queue and provider capability; this is not a claim that its context branch is an identical native-history worktree fork.

## codex-conversation-10: Ordinary structured questions

**present; none; source inspection; no runtime/UI execution.**

Codex: Question picker supports selected options, freeform Other, multi-select and secret fields; request panel dispatches userInput.

Memex: Memex projects native question identity/choice values/multiselect/secret flags; retains drafts while navigating; answers can include captured UTF-8 files.

Ordinary questions already have a substantive native implementation.

Recommendation: Retain grouped reply identity and draft preservation.

- [Codex: pending-request-item-panel-f0363294db8b.js:1](/tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1) — Pending-request dispatcher exposes structured questions. Anchor/symbol: "case`userInput`".
- [Codex: app-primary-c0280d43ce72.js:11](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:11) — Question picker handles multi-selection and secret input. Anchor/symbol: "isMultiSelect:he,isSecret:ge".
- [Memex: ConversationQuestionsView.swift:20](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQuestionsView.swift:20) — Back/Next retains per-question drafts; submission preserves exact choice values and text context. Anchor/symbol: "ConversationQuestionsView".
- [Memex runtime: CodexAppServerTransport.swift:1590](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1590) — Answers accumulate by native question ID until the group can respond. Anchor/symbol: "respondUserInput".

Limits: This does not imply parity for MCP JSON-schema elicitation or Codex async/autoresolved questions; those use different protocols.

## codex-conversation-11: MCP structured form elicitation

**defect; P1; source inspection; no runtime/UI execution.**

Codex: MCP elicitation dispatcher recognizes formElicitation/openaiForm/legacyOpenAIForm and opens a dedicated form component.

Memex: Codex transport maps mcpServer/elicitation/request to generic approval options and respondPermission always sends content null, including accept.

A form that requires entered values cannot be completed correctly through this path; clicking Allow does not supply them.

Recommendation: Add a typed elicitation form/reply path or explicitly reject unsupported required forms instead of offering a misleading empty accept.

- [Codex: mcp-server-elicitation-request-panel-15e9f832a59b.js:1](/tmp/codex-parity-20261005/webview/assets/mcp-server-elicitation-request-panel-15e9f832a59b.js:1) — Dedicated form elicitation dispatch, separate from simple approvals. Anchor/symbol: "case`formElicitation`".
- [Memex runtime: CodexAppServerTransport.swift:2094](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2094) — Elicitation is projected as approval, not a schema-backed question form. Anchor/symbol: "handleServerRequest".
- [Memex runtime: CodexAppServerTransport.swift:1575](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1575) — Accepted response explicitly contains NSNull content. Anchor/symbol: "respondPermission / mcpServer/elicitation/request".
- [Memex: ConversationComposer.swift:50](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:50) — Memex supplies only title/detail/options to approval UI. Anchor/symbol: "pendingApprovals".
- [Codex: request-panel-49ba9fe401fd.js:2](/tmp/codex-parity-20261005/webview/assets/request-panel-49ba9fe401fd.js:2) — Actual form submit serializes collected form content t into elicitation response; not just a dedicated label. Anchor/symbol: "replyWithMcpServerElicitationResponse(a,c,pe(e,null,t??{}))".

Limits: Source-proven limitation of Memex Codex transport, not a claim that every MCP elicitation requires content or that every ACP provider has the same behavior. The Codex adapter advertises mcpServerOpenaiFormElicitation:false at line282. The defect is the generic MCP elicitation request path when a content-bearing form reaches it, not a promise that proprietary OpenAI forms are negotiated.

## codex-conversation-12: Policy amendment and reviewer detail UI

**partial; P2; source inspection; no runtime/UI execution.**

Codex: Approval panel exposes exec/network policy amendments; automatic-review details show denied/high-risk rationale and review state.

Memex: Generic Memex approval panel supports allow once/session/deny and raw request detail. Codex adapter filters command decisions to string accept/acceptForSession, and its notification switch lacks dedicated guardian review start/completion projection.

Approval works, but users do not get the same scoped rule controls or review explanation UI.

Recommendation: Project only supported typed decisions and review events; keep unknown data inspectable.

- [Codex: pending-request-item-panel-f0363294db8b.js:1](/tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1) — Native network policy amendment action. Anchor/symbol: "applyNetworkPolicyAmendment".
- [Codex: pending-request-item-panel-f0363294db8b.js:1](/tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1) — Exec amendment is part of specialized approval UI. Anchor/symbol: "proposedExecpolicyAmendment".
- [Codex: automatic-approval-review-details-4ade28f5c96a.js:1](/tmp/codex-parity-20261005/webview/assets/automatic-approval-review-details-4ade28f5c96a.js:1) — Review detail presentation distinguishes high risk denials. Anchor/symbol: "Requires explicit authorization because this action is considered high risk".
- [Memex runtime: CodexAppServerTransport.swift:2613](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2613) — Only accept, session, decline/cancel decisions are surfaced/serialized. Anchor/symbol: "approvalOptions / commandDecision".
- [Memex runtime: CodexAppServerTransport.swift:2023](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2023) — Generic guardianWarning is handled; dedicated review lifecycle is not projected here. Anchor/symbol: "notification switch / default".
- [Memex: ConversationProjection.swift:21](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProjection.swift:21) — UI projection carries generic title, raw detail and options. Anchor/symbol: "approval projection".

Limits: Memex already configures auto_review for noninteractive approval policy. The gap is specialized decision/presentation, not lack of auto-review backend selection.

## codex-conversation-13: Core readable transcript, search and navigation

**present; none; source inspection; no runtime/UI execution.**

Codex: Local turn renderer groups user/assistant/activity entries; thread find command and history search hydration exist.

Memex: Memex native transcript provides Markdown tables/code/links/images, structured tool output with raw disclosure, completed-work folding, paginated find and prompt outline.

A broad missing-transcript/search claim would be inaccurate.

Recommendation: Retain native rendering and provenance; extend only concrete richer content types.

- [Codex: local-conversation-turn-ccc9d0fc20e5.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1) — Local turn rendering separates user content from activity groups. Anchor/symbol: "case`user-message`".
- [Codex: app-initial-f9b16fbf8fc7.js:1522](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1522) — Native find command is dispatched. Anchor/symbol: "type:`find-in-thread`".
- [Codex: app-shared-9d148924be0b.js:487](/tmp/codex-parity-20261005/webview/assets/app-shared-9d148924be0b.js:487) — History search results can be hydrated. Anchor/symbol: "hydrateConversationSearchMatch(e)".
- [Memex: Reader.swift:31](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Reader.swift:31) — Reachable reader includes recovery, work view and find state. Anchor/symbol: "Reader.body".
- [Memex: ConversationFind.swift:85](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationFind.swift:85) — Cancellable paged whole-conversation literal find. Anchor/symbol: "search(in:)".
- [Memex: TranscriptPresentation.swift:178](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/TranscriptPresentation.swift:178) — Completed routine work folds while failures remain visible. Anchor/symbol: "group".
- [Memex: RichTextRenderer.swift:28](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RichTextRenderer.swift:28) — Native Markdown code, quotes, headings, lists and tables. Anchor/symbol: "appendBlock / appendTable".
- [Memex: ToolContentRenderer.swift:80](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ToolContentRenderer.swift:80) — Raw tool content remains inspectable beside structured rendering. Anchor/symbol: "render".

Limits: Source inspection does not measure runtime performance or accessibility quality. Codex find reachability was traced through command dispatch/hydration, not exercised. Memex literal find is not regex search.

## codex-conversation-14: Interactive MCP app results

**missing; P1; source inspection; no runtime/UI execution.**

Codex: Local turn renderer passes renderMcpApps into tool activity; MCP tool component resolves app metadata/resource, open/reopen widget activity, and side-panel app representation.

Memex: Runtime has AgentMcpAppProjection, but native Memex renderer/projection does not consume mcpApp; its rich-content cases cover text/code/images/attachments and tool results are generic.

Runtime protocol support alone does not make interactive tool apps usable inside Memex.

Recommendation: Wire typed runtime app metadata to a native host with scoped messaging and restore behavior, retaining generic fallback.

- [Codex: local-conversation-turn-ccc9d0fc20e5.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1) — Local thread turn forwards MCP app rendering controls. Anchor/symbol: "renderMcpApps:S,shouldAutoExpandMcpApps:C".
- [Codex: mcp-tool-item-content-a7c9d903249d.js:1](/tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1) — Tool result offers app opening/open/error/reopen states. Anchor/symbol: "mcpApp.activity.status".
- [Codex: mcp-tool-item-content-a7c9d903249d.js:1](/tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1) — App resource and metadata are resolved for MCP result. Anchor/symbol: "invocationResourceUri:n".
- [Memex runtime: CodexAppServerTransport.swift:2412](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2412) — Underlying runtime retains typed MCP app projection. Anchor/symbol: "mcpAppProjection".
- [Memex: TranscriptController.swift:700](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/TranscriptController.swift:700) — Reachable native reader uses SourceContent and ToolContentRenderer. Anchor/symbol: "measurement".
- [Memex: RichContentView.swift:46](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RichContentView.swift:46) — Complete native rich block inventory has no interactive app host. Anchor/symbol: "RichContentBlock / RichContentDocument".
- [Memex: ToolContentRenderer.swift:19](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ToolContentRenderer.swift:19) — Typed media/text/tool blocks do not create MCP app hosts. Anchor/symbol: "blocks".

Limits: Absence boundary: searched apps/macos/Sources/Memex and MemexExecutionHostCore for mcpApp/MCPApp/McpApp/uiResource; no consumers, then traced Reader→TranscriptController→RichContentView/ToolContentRenderer. Codex rendering remains metadata/feature gated; not every tool result is an app.

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

## Coverage and unresolved questions

- composer/input/attachments/context: Codex composer state/footer and annotation contexts; Memex Reader→ConversationComposer→AcpComposerView plus attachment/catalog/context helpers.
- queue/steer/stop/recovery: Codex queued-message-list and caller; Memex queue view, LiveConversation state machine, recovery view and existing tests.
- plan/tasks/subagents/forks: Codex completed plan selection, editable/read-only panel, child summary/details, native fork service; Memex Work projection, history UI/store/runtime and child resume boundaries.
- approval/questions: Codex pending request dispatcher, question picker, specialized permissions/review and elicitation; Memex pending projection/UI and exact Codex transport request/reply handling.
- transcript/rendering/search/history: Codex local turn grouping, MCP tool result, find dispatch/history hydration; Memex Reader, NSTableView transcript, raw/structured tool content, rich blocks, literal paged find and outline.
- projects/worktrees/Git/review/files; terminal/browser/computer use; remote/cloud/multi-host: Memex WorkspacePanelView and Store.canAccessLocalFiles; Memex LocalProjects / ProjectCreation / ManagedWorkspaceLifecycle; Memex WorkspaceGit / WorkspaceReview / WorkspaceChangesView / WorkspaceFilesView; Memex WorkspaceTerminalSession / Group / View; Memex WorkspaceBrowserTabView / Automation; Memex ExecutionHostConnection / ConnectionsView / RemoteConversationRuntime; MemexExecutionHostCore ExecutionHost complete operation dispatch; Codex local thread, handoff, worktree settings/query, Git actions, PR review, file editor, terminal, remote connections and computer-use settings chunks
- providers/models/configuration; schedules/automation; plugins/MCP/skills; app-control tools; organization/notifications/settings; accessibility/keybindings/performance: Codex settings applicability map -> settings implementations/app-server calls and desktop tool schemas. Memex native Settings/menus/sidebar/library, composer catalogs/runtime connection, host request allowlist/schedule runtime, notification state machine. Read-only; no runtime claims.

- All findings are source inspection. No app launch, UI interaction, provider prompts, tests, or runtime observations were performed.
- Live feature flags/account entitlements for Codex dictation, queue-to-side-chat, editable plan on each host, MCP app embedding and child interactivity were not queried.
- Advanced math/Mermaid/audio/video rendering equivalence was not claimed: libraries coexist with ChatGPT assets and need additional route-specific tracing before a Codex claim.
- Codex async/automatic question-resolution behavior is present in source but not compared as ordinary requestUserInput parity; provider/version availability requires a separate bounded check.
- No universal ACP-provider claims: concrete elicitation/policy gaps apply to the traced Codex transport; other provider capabilities can differ.
- Actual Codex account/feature-gate visibility and remote native backend behavior were not runtime-tested.
- Codex worktree snapshot retention/exclusion details are asserted by UI/native request contract; native implementation was not extracted.
- Cloud Work versus Codex and Memex server/runtime provisioning are not equivalent product boundaries; leave cloud parity unknown.
- Codex account/feature-gate resolution and actual UI reachability remain root UI work.
- No performance benchmarks or accessibility assistive-technology verification.
- Memex native provider configuration may supply tools outside native management UI; not enumerated and no credential/state files inspected.

## Source manifest and review

[Version manifest](./version-manifest.json) records source roots, exact archive/version identity, extraction boundaries and every referenced Codex asset SHA-256. [Structured findings](./findings.json) contain short exact minified anchors and byte offsets; line numbers alone are not the locator. Source is retained only in local scratch.

Lead inspected consequential return-path/schema and native UI inventory evidence. Corrected remote workspace comparator anchors from durable/cloud branches to non-durable terminal/file/Git paths. Explicitly credited Memex Rust memex-control MCP. Lowered editable-plan priority to P2. Added actual Codex form response sender and adapter capability caveat.

33 findings, referenced files/lines/exact Codex anchors validated. Installed app.asar SHA256 rechecked unchanged after source inventory. No tests/builds/app interactions performed.
