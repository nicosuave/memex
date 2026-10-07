# Codex conversation capability evidence

Source-only comparison of installed Codex 26.930.51102 (13100) with Memex 17e958778a73 and runtime 7818518b094f. No app/UI mutation, build or provider prompt was performed. Exact JSON evidence contains absolute paths, line ranges and minified-source anchors.

The current Memex baseline already implements core composition, editable/reorderable queues, live steering, uncertainty recovery, structured questions, native fork/rewind, structured plan/child summaries and paginated transcript find. The strongest remaining gaps are MCP form replies, interactive MCP app result hosts and full editable plan documents.

| ID | Capability | Disposition | Priority |
|---|---|---|---|
| codex-conversation-01 | Files, images, mentions, captured context | present | none |
| codex-conversation-02 | Annotation-aware attachment editing | partial | P2 |
| codex-conversation-03 | Integrated dictation and dictation recovery | missing | P2 |
| codex-conversation-04 | Queue, reorder, edit, steer, stop | present | none |
| codex-conversation-05 | Queued message to side chat | missing | P3 |
| codex-conversation-06 | Uncertain delivery and recovery | present | none |
| codex-conversation-07 | Editable plan document workflow | partial | P1 |
| codex-conversation-08 | Subagent overview depth | partial | P2 |
| codex-conversation-09 | Native fork, rewind and context branch | present | none |
| codex-conversation-10 | Ordinary structured questions | present | none |
| codex-conversation-11 | MCP structured form elicitation | defect | P1 |
| codex-conversation-12 | Policy amendment and reviewer detail UI | partial | P2 |
| codex-conversation-13 | Core readable transcript, search and navigation | present | none |
| codex-conversation-14 | Interactive MCP app results | missing | P1 |

## codex-conversation-01 — Files, images, mentions, captured context

**Codex:** Reachable composer context model carries image/file/pasted-text/selection/MCP/annotation attachment types.

**Memex:** Native composer has file chooser, image/file paste/drop, file/chat mentions, slash commands, prompt stash and recall; captured contents are immutable and provider validated.

The ordinary context-composition workflow is already implemented. Richer structured contexts are assessed separately. Retain existing composer; do not label attachments or mentions missing.

Boundary: Provider capability and live account availability were not exercised.

Evidence:
- Codex: [app-initial-f9b16fbf8fc7.js:1654](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1654) — `mcpAppModelContextAttachments:[],selectedTextAttachments:`. Composer state has typed context collections.
- Memex: [ConversationComposer.swift:75](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:75) — `AcpComposerView / onPasteCommand / onDrop`. Reachable native composer wires attachments, mentions, queue and steer.
- Memex: [ConversationComposer.swift:186](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:186) — `inputMenu`. Prompt stash, recall, mentions and commands are exposed.
- Memex: [ConversationComposer.swift:413](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:413) — `mentionSuggestions / selectMention`. Chat excerpt and file mentions enter attached context.
- Memex: [ConversationPromptContext.swift:10](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptContext.swift:10) — `ConversationAttachment.text / validate`. Captured contents become real blocks and enforce capability/size limits.

## codex-conversation-02 — Annotation-aware attachment editing

**Codex:** Composer renders response annotations and image/diff/browser comment attachment collections with edit/navigation/removal callbacks.

**Memex:** Memex provides text/image/file context and source labels; native composer and attachment APIs do not model response annotation identities or image comment drafts.

Users can supply the underlying text or screenshot, but cannot preserve and revise Codex-style contextual annotations in the composer. Add annotation identity/location only for concrete supported Memex source surfaces; preserve current immutable capture behavior.

Boundary: Bounded absence: native ConversationComposer, ConversationPromptContext, ConversationAttachment and their public context item mapping. This does not claim no selection capture: terminal/browser selections have existing capture paths.

Evidence:
- Codex: [app-primary-c0280d43ce72.js:116](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:116) — `onEditResponseTextAnnotation:`. Composer context UI exposes annotation edit and navigation callbacks.
- Codex: [queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — `imageCommentDrafts?.reduce`. Queued messages retain image comment collections and annotation counts.
- Memex: [ConversationComposer.swift:70](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:70) — `contextItems / mentionConfiguration`. Memex attachment UI maps only title/path/id and removal.
- Memex: [ConversationPromptContext.swift:10](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptContext.swift:10) — `ConversationAttachment.text / image / validate`. Native capture boundary stores generic text or image blocks, not editable annotation structures.

## codex-conversation-03 — Integrated dictation and dictation recovery

**Codex:** Shared local composer footer accepts dictation controls; implementation has audio history read/retry/download and transcription.

**Memex:** Native Memex composer and SQACPUI input surface expose text, mentions, file/image context and send/stop; no app-owned recording/transcription flow is wired.

System text-field dictation is different from an integrated recording/retry workflow. Consider a dedicated native dictation path if voice input is a product goal.

Boundary: Codex dictation is feature/platform/service dependent; installed source proves integration, not that this account currently has it. No claim is made that macOS system dictation cannot type into Memex.

Evidence:
- Codex: [app-primary-c0280d43ce72.js:118](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:118) — `composerInput:i,dictation:hn,dictationEnabled:n`. Local composer footer receives dictation state and controls.
- Codex: [app-primary-c0280d43ce72.js:7](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:7) — `async function Yze(e,t)`. Dictation history retry reads audio and transcribes it.
- Memex: [ConversationComposer.swift:42](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:42) — `AcpComposerView`. Full composer input/callback inventory contains no dictation integration.
- Memex runtime: [AcpComposerView.swift:1](/Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:1) — `AcpComposerPendingApprovalItem / input types`. Shared native composer types and implementation inspected; no dictation/speech recording APIs found.

## codex-conversation-04 — Queue, reorder, edit, steer, stop

**Codex:** Reachable queue has reorder, edit, delete, steer, default queue/steer toggle and pause after interruption.

**Memex:** Same core operations exist in ConversationQueueView and LiveConversation, including persisted queue and explicit pause/resume.

This is substantial current parity, with provider support checked before live steering. Preserve the current queue state machine and tests; focus any polish on interaction details.

Boundary: Codex uses drag reorder; Memex exposes explicit earlier/later commands. Memex steering is gated by snapshot.canSteer and request/settings state.

Evidence:
- Codex: [queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — `Queue paused because you interrupted`. Queue pauses and can resume after interruption.
- Codex: [queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — `Submit without interrupting the model`. Send-now action steers without interrupting.
- Memex: [ConversationQueueView.swift:12](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQueueView.swift:12) — `ConversationQueueView.body`. Edit, move earlier/later, promote, remove and pause/resume are reachable.
- Memex: [LiveConversation.swift:587](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:587) — `enqueueDraft / steerDraft / dispatchQueued`. Queue persists, checks capability, and dispatches prompt versus steer.
- Memex: [ConversationQueueTests.swift:79](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:79) — `queueEditsAndOrderSurviveRelaunchWithoutDispatch / recoveryDoesNotReplayAnUnconfirmedDequeuedCommand`. Existing tests cover persistence, stop settlement, steering identity and uncertain recovery; inspected, not run.

## codex-conversation-05 — Queued message to side chat

**Codex:** Queue action Open in side chat is supplied conditionally by the composer callback.

**Memex:** Queue action inventory supports edit/reorder/steer/send/remove/pause, but has no branch/side-chat action.

Memex users must manually move that prompt to a new conversation. If adopting side chats, reuse queue attachment identity and perform a reviewed transfer rather than duplicate dispatch.

Boundary: Codex gate oc was not resolved to live entitlement; record as gated shipped source, not observed universal availability.

Evidence:
- Codex: [queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — `Open in side chat`. Queue menu optionally starts a queued message as a side chat.
- Codex: [app-primary-c0280d43ce72.js:169](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:169) — `onOpenInSideChatMessage:oc?xl:void 0`. Caller gates this action; it is not universal.
- Memex: [ConversationQueueView.swift:28](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQueueView.swift:28) — `Menu`. Complete native queue menu has no side-chat operation.

## codex-conversation-06 — Uncertain delivery and recovery

**Codex:** Queue distinguishes sending/pending/outcome-unknown; warns to check conversation before deleting an unconfirmed message.

**Memex:** Memex holds uncertain prompts and queue, preserves drafts and exposes reconnect/inspection; never auto-replays unconfirmed commands.

Do not count recovery as absent; Memex deliberately makes uncertainty visible. Keep recovery messages and provider identity checks.

Boundary: Provider capability and live account availability were not exercised.

Evidence:
- Codex: [queued-message-list-e1e037c82f46.js:1](/tmp/codex-parity-20261005/webview/assets/queued-message-list-e1e037c82f46.js:1) — `Delivery could not be confirmed. Check the conversation before deleting this saved message`. User-facing unknown-outcome state.
- Memex: [ConversationRecoveryView.swift:4](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationRecoveryView.swift:4) — `ConversationRecoveryKind / detail`. Distinct archive, authentication, uncertain delivery and reconnect states.
- Memex: [LiveConversation.swift:484](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:484) — `restoreUnsentPrompt / restorePendingDraft / reconcilePendingPrompt`. Explicit pending-prompt reconciliation.
- Memex: [ConversationQueueTests.swift:192](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:192) — `recoveryDoesNotReplayAnUnconfirmedDequeuedCommand`. Existing no-replay regression test.

## codex-conversation-07 — Editable plan document workflow

**Codex:** Completed plan text can open an editable PLAN.md under the host Codex home; saves existing edits and falls back to a read-only panel when file writing is unavailable.

**Memex:** Memex projects structured plan steps from update_plan/TodoWrite/native plan arrays and offers Refine/Implement/Implement in new conversation. Markdown plan text can be shown as tool detail, but does not become an editable plan document.

Planning exists, but document-oriented planning and direct editing are not equivalent to the step summary. Preserve full plan identity/text and add an editable plan view with save/implementation provenance.

Boundary: Editable Codex plan requires workspaceFiles.write and a supported host. Do not claim the existing Memex planning mode or implementation actions are missing.

Evidence:
- Codex: [local-conversation-plan-model-5c6fbe48a2a4.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-plan-model-5c6fbe48a2a4.js:1) — `r.type!==`plan``. Completed plan text is selected from conversation items.
- Codex: [local-conversation-thread-604c26084272.js:8](/tmp/codex-parity-20261005/webview/assets/local-conversation-thread-604c26084272.js:8) — `Pv(t,{content:r.content`. Thread summary opens selected plan.
- Codex: [plan-side-panel-2f427e3e9737.js:1](/tmp/codex-parity-20261005/webview/assets/plan-side-panel-2f427e3e9737.js:1) — `m=l(p,`PLAN.md`)`. Plan panel creates/opens editable file with conflict and save handling; fallback read-only.
- Memex: [ConversationWork.swift:27](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:27) — `ConversationWork.project`. Only structured steps become the plan summary.
- Memex: [ConversationWork.swift:117](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:117) — `ConversationWorkView`. Refine/Implement buttons attach step text to a draft.
- Memex runtime: [CodexAppServerTransport.swift:2405](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2405) — `case "plan"`. Markdown text is retained in details; structured payload is derived from arrays.

## codex-conversation-08 — Subagent overview depth

**Codex:** Dedicated subagent panel splits active/done and opens selected child details including model/reasoning; local thread summary links into it.

**Memex:** Memex derives child identity/prompt/status/parent, offers Open and Parent for indexed sessions, and displays unavailable status honestly. It has no dedicated active/done side panel or per-child model/effort fields in this view.

Child work is visible today, but navigation and metadata presentation are shallower. Extend the existing child summary using authoritative provider state; avoid inferring completion from tool return.

Boundary: Codex child interactivity is explicitly canInteract-gated. No claim of universal direct child messaging is made. Memex parent-owned transport can handle child requests.

Evidence:
- Codex: [local-conversation-subagents-panel-tab-797735e5898b.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1) — `localConversation.subagentsPanel.active`. Active/completed sections in a dedicated panel.
- Codex: [local-conversation-subagents-panel-tab-797735e5898b.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-subagents-panel-tab-797735e5898b.js:1) — `localConversation.subagentsPanel.modelAndReasoningEffort`. Child detail header shows model and effort.
- Memex: [ConversationWork.swift:17](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:17) — `Agent / State`. Memex child summary data consists of id, prompt, status, parentID.
- Memex: [ConversationWork.swift:137](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationWork.swift:137) — `ForEach(state.agents)`. Open/Parent require matching indexed child sessions.
- Memex: [InAppConversationTarget.swift:22](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/InAppConversationTarget.swift:22) — `unavailableReason`. Standalone resume of subagents is disallowed; parent is the control surface.

## codex-conversation-09 — Native fork, rewind and context branch

**Codex:** Fork service retains history, supports same-directory and asynchronous worktree fork, and distinguishes incomplete setup.

**Memex:** Memex has native fork/rewind at valid boundaries, context branch/provider transition/new worktree, persisted relationships and recovery journal.

History operations are not a Memex gap, though native worktree fork and context-with-worktree are distinct workflows. Keep the explicit native-history versus captured-context distinction.

Boundary: Parity is at workflow family level. Memex native mutation requires idle state, empty queue and provider capability; this is not a claim that its context branch is an identical native-history worktree fork.

Evidence:
- Codex: [threads-fork-d7fba758d383.js:1](/tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1) — `case`same-directory``. Same-directory native fork.
- Codex: [threads-fork-d7fba758d383.js:1](/tmp/codex-parity-20261005/webview/assets/threads-fork-d7fba758d383.js:1) — `case`worktree``. Asynchronous worktree fork retains working-tree starting state.
- Memex: [ConversationHistoryActions.swift:19](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationHistoryActions.swift:19) — `ConversationHistoryActions.body`. Reachable branch/fork/rewind actions and recovery notice.
- Memex: [StoreConversationHistory.swift:42](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/StoreConversationHistory.swift:42) — `branchWithContext / mutateNativeHistory`. Context branch can request new worktree; native mutation records relationship.
- Memex runtime: [CodexAppServerTransport.swift:532](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:532) — `mutateHistory / fork cutoff verification`. Native fork/revert requires compatibility and verifies chosen cutoff.

## codex-conversation-10 — Ordinary structured questions

**Codex:** Question picker supports selected options, freeform Other, multi-select and secret fields; request panel dispatches userInput.

**Memex:** Memex projects native question identity/choice values/multiselect/secret flags; retains drafts while navigating; answers can include captured UTF-8 files.

Ordinary questions already have a substantive native implementation. Retain grouped reply identity and draft preservation.

Boundary: This does not imply parity for MCP JSON-schema elicitation or Codex async/autoresolved questions; those use different protocols.

Evidence:
- Codex: [pending-request-item-panel-f0363294db8b.js:1](/tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1) — `case`userInput``. Pending-request dispatcher exposes structured questions.
- Codex: [app-primary-c0280d43ce72.js:11](/tmp/codex-parity-20261005/webview/assets/app-primary-c0280d43ce72.js:11) — `isMultiSelect:he,isSecret:ge`. Question picker handles multi-selection and secret input.
- Memex: [ConversationQuestionsView.swift:20](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQuestionsView.swift:20) — `ConversationQuestionsView`. Back/Next retains per-question drafts; submission preserves exact choice values and text context.
- Memex runtime: [CodexAppServerTransport.swift:1590](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1590) — `respondUserInput`. Answers accumulate by native question ID until the group can respond.

## codex-conversation-11 — MCP structured form elicitation

**Codex:** MCP elicitation dispatcher recognizes formElicitation/openaiForm/legacyOpenAIForm and opens a dedicated form component.

**Memex:** Codex transport maps mcpServer/elicitation/request to generic approval options and respondPermission always sends content null, including accept.

A form that requires entered values cannot be completed correctly through this path; clicking Allow does not supply them. Add a typed elicitation form/reply path or explicitly reject unsupported required forms instead of offering a misleading empty accept.

Boundary: Source-proven limitation of Memex Codex transport, not a claim that every MCP elicitation requires content or that every ACP provider has the same behavior.

Evidence:
- Codex: [mcp-server-elicitation-request-panel-15e9f832a59b.js:1](/tmp/codex-parity-20261005/webview/assets/mcp-server-elicitation-request-panel-15e9f832a59b.js:1) — `case`formElicitation``. Dedicated form elicitation dispatch, separate from simple approvals.
- Memex runtime: [CodexAppServerTransport.swift:2094](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2094) — `handleServerRequest`. Elicitation is projected as approval, not a schema-backed question form.
- Memex runtime: [CodexAppServerTransport.swift:1575](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1575) — `respondPermission / mcpServer/elicitation/request`. Accepted response explicitly contains NSNull content.
- Memex: [ConversationComposer.swift:50](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:50) — `pendingApprovals`. Memex supplies only title/detail/options to approval UI.

## codex-conversation-12 — Policy amendment and reviewer detail UI

**Codex:** Approval panel exposes exec/network policy amendments; automatic-review details show denied/high-risk rationale and review state.

**Memex:** Generic Memex approval panel supports allow once/session/deny and raw request detail. Codex adapter filters command decisions to string accept/acceptForSession, and its notification switch lacks dedicated guardian review start/completion projection.

Approval works, but users do not get the same scoped rule controls or review explanation UI. Project only supported typed decisions and review events; keep unknown data inspectable.

Boundary: Memex already configures auto_review for noninteractive approval policy. The gap is specialized decision/presentation, not lack of auto-review backend selection.

Evidence:
- Codex: [pending-request-item-panel-f0363294db8b.js:1](/tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1) — `applyNetworkPolicyAmendment`. Native network policy amendment action.
- Codex: [pending-request-item-panel-f0363294db8b.js:1](/tmp/codex-parity-20261005/webview/assets/pending-request-item-panel-f0363294db8b.js:1) — `proposedExecpolicyAmendment`. Exec amendment is part of specialized approval UI.
- Codex: [automatic-approval-review-details-4ade28f5c96a.js:1](/tmp/codex-parity-20261005/webview/assets/automatic-approval-review-details-4ade28f5c96a.js:1) — `Requires explicit authorization because this action is considered high risk`. Review detail presentation distinguishes high risk denials.
- Memex runtime: [CodexAppServerTransport.swift:2613](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2613) — `approvalOptions / commandDecision`. Only accept, session, decline/cancel decisions are surfaced/serialized.
- Memex runtime: [CodexAppServerTransport.swift:2023](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2023) — `notification switch / default`. Generic guardianWarning is handled; dedicated review lifecycle is not projected here.
- Memex: [ConversationProjection.swift:21](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProjection.swift:21) — `approval projection`. UI projection carries generic title, raw detail and options.

## codex-conversation-13 — Core readable transcript, search and navigation

**Codex:** Local turn renderer groups user/assistant/activity entries; thread find command and history search hydration exist.

**Memex:** Memex native transcript provides Markdown tables/code/links/images, structured tool output with raw disclosure, completed-work folding, paginated find and prompt outline.

A broad missing-transcript/search claim would be inaccurate. Retain native rendering and provenance; extend only concrete richer content types.

Boundary: Source inspection does not measure runtime performance or accessibility quality. Codex find reachability was traced through command dispatch/hydration, not exercised. Memex literal find is not regex search.

Evidence:
- Codex: [local-conversation-turn-ccc9d0fc20e5.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1) — `case`user-message``. Local turn rendering separates user content from activity groups.
- Codex: [app-initial-f9b16fbf8fc7.js:1522](/tmp/codex-parity-20261005/webview/assets/app-initial-f9b16fbf8fc7.js:1522) — `type:`find-in-thread``. Native find command is dispatched.
- Codex: [app-shared-9d148924be0b.js:487](/tmp/codex-parity-20261005/webview/assets/app-shared-9d148924be0b.js:487) — `hydrateConversationSearchMatch(e)`. History search results can be hydrated.
- Memex: [Reader.swift:31](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/Reader.swift:31) — `Reader.body`. Reachable reader includes recovery, work view and find state.
- Memex: [ConversationFind.swift:85](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationFind.swift:85) — `search(in:)`. Cancellable paged whole-conversation literal find.
- Memex: [TranscriptPresentation.swift:178](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/TranscriptPresentation.swift:178) — `group`. Completed routine work folds while failures remain visible.
- Memex: [RichTextRenderer.swift:28](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RichTextRenderer.swift:28) — `appendBlock / appendTable`. Native Markdown code, quotes, headings, lists and tables.
- Memex: [ToolContentRenderer.swift:80](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ToolContentRenderer.swift:80) — `render`. Raw tool content remains inspectable beside structured rendering.

## codex-conversation-14 — Interactive MCP app results

**Codex:** Local turn renderer passes renderMcpApps into tool activity; MCP tool component resolves app metadata/resource, open/reopen widget activity, and side-panel app representation.

**Memex:** Runtime has AgentMcpAppProjection, but native Memex renderer/projection does not consume mcpApp; its rich-content cases cover text/code/images/attachments and tool results are generic.

Runtime protocol support alone does not make interactive tool apps usable inside Memex. Wire typed runtime app metadata to a native host with scoped messaging and restore behavior, retaining generic fallback.

Boundary: Absence boundary: searched apps/macos/Sources/Memex and MemexExecutionHostCore for mcpApp/MCPApp/McpApp/uiResource; no consumers, then traced Reader→TranscriptController→RichContentView/ToolContentRenderer. Codex rendering remains metadata/feature gated; not every tool result is an app.

Evidence:
- Codex: [local-conversation-turn-ccc9d0fc20e5.js:1](/tmp/codex-parity-20261005/webview/assets/local-conversation-turn-ccc9d0fc20e5.js:1) — `renderMcpApps:S,shouldAutoExpandMcpApps:C`. Local thread turn forwards MCP app rendering controls.
- Codex: [mcp-tool-item-content-a7c9d903249d.js:1](/tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1) — `mcpApp.activity.status`. Tool result offers app opening/open/error/reopen states.
- Codex: [mcp-tool-item-content-a7c9d903249d.js:1](/tmp/codex-parity-20261005/webview/assets/mcp-tool-item-content-a7c9d903249d.js:1) — `invocationResourceUri:n`. App resource and metadata are resolved for MCP result.
- Memex runtime: [CodexAppServerTransport.swift:2412](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:2412) — `mcpAppProjection`. Underlying runtime retains typed MCP app projection.
- Memex: [TranscriptController.swift:700](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/TranscriptController.swift:700) — `measurement`. Reachable native reader uses SourceContent and ToolContentRenderer.
- Memex: [RichContentView.swift:46](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/RichContentView.swift:46) — `RichContentBlock / RichContentDocument`. Complete native rich block inventory has no interactive app host.
- Memex: [ToolContentRenderer.swift:19](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ToolContentRenderer.swift:19) — `blocks`. Typed media/text/tool blocks do not create MCP app hosts.

## Unresolved / validation boundary

- All findings are source inspection. No app launch, UI interaction, provider prompts, tests, or runtime observations were performed.
- Live feature flags/account entitlements for Codex dictation, queue-to-side-chat, editable plan on each host, MCP app embedding and child interactivity were not queried.
- Advanced math/Mermaid/audio/video rendering equivalence was not claimed: libraries coexist with ChatGPT assets and need additional route-specific tracing before a Codex claim.
- Codex async/automatic question-resolution behavior is present in source but not compared as ordinary requestUserInput parity; provider/version availability requires a separate bounded check.
- No universal ACP-provider claims: concrete elicitation/policy gaps apply to the traced Codex transport; other provider capabilities can differ.
