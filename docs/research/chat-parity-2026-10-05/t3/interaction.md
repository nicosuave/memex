# Active interaction inventory: current T3 vs Memex

Source-only comparison, 5 October 2026. No builds, test execution, provider prompts, UI sessions or source changes. All three baseline HEADs were independently verified.

- T3: `ecfdda5fa804582066c1aa62afea419677c24883` at /private/tmp/t3-chat-inventory-ecfdda5
- Memex: `17e958778a7398db8238cc9c8415ca12649067e5` at /Users/nico/Code/memex-chat-capabilities
- Runtime: `7818518b094f44911aecfec7dbeb905c57ea24a9` at /Users/nico/Code/memex-chat-runtime

## Main conclusions

Current Memex closes the older review's basic interaction gaps: first-turn preparation with settings and attachments, paste/drop, mentions, local command/skill capture, stash/recall, editable queues and native steering, structured multi-select questions, and stop acknowledgement/timeout recovery are wired.

Remaining differences are narrower: T3 has eight driver types and some interrupt/restart steering, richer image normalization, and question media/dismissibility. Memex has two native providers plus configured ACP; local-only file/skill catalogs and text-only question context are deliberate boundaries.

Do not copy T3's ordinary send recovery: the saved draft is cleared before asynchronous dispatch and a new retry uses new identities. This remains distinct from server-side receipt deduplication. T3 also has a reload/crash image-loss window during stash and enabled-but-inert overflow actions on non-live approvals. Memex's durable outbound intent and atomic captured-byte stash are strengths.

The optional runtime is included by default when its configured local root exists; history-only mode explicitly disables it. This is a provenance qualifier, not an identified missing capability of the supplied runtime-enabled Memex baseline. Installed binary behavior was not measured.

## Workflow matrix

| ID | Workflow | Disposition | Priority |
|---|---|---|---|
| T3-I01 | First and follow-up prompts with pre-send configuration | present | none |
| T3-I02 | Files, pasted images, and media ergonomics | partial | P2 |
| T3-I03 | File/chat mentions and slash commands/skills | present | none |
| T3-I04 | Queue, edit, reorder, cancel and promote | present | none |
| T3-I05 | Provider-specific steering fallback | partial | P2 |
| T3-I06 | Stop acknowledgement and safe send recovery | present | none |
| T3-I07 | Structured choices and question attachments | partial | P2 |
| T3-I08 | Provider-specific approval options | present | none |
| T3-I09 | T3 non-live approval overflow actions remain enabled | defect | P2 |
| T3-I10 | Prompt stash and recall | present | none |
| T3-I11 | T3 stash image loss window | defect | P2 |
| T3-I12 | Provider catalog, model/effort and permission configuration | partial | P2 |
| T3-I13 | Build/runtime availability boundary | not-applicable | none |
| T3-I14 | T3 ordinary send clears durable draft before acknowledgement and retries with new identity | defect | P1 |

## Detailed evidence

### T3-I01: First and follow-up prompts with pre-send configuration

**Workflow:** Start from Home; select model/permissions or attachments before sending the first prompt; continue in the chat.

**T3:** T3 uses the same rich composer and provider/model state for draft and existing thread targets.

**Memex:** Home supports Attach files, paste/drop, Model and permissions preparation without sending, then opens the full composer. Explicit preferences are applied and observed before any immediate first send.

**Disposition:** present; none; source-confirmed; existing tests inspected where cited, not executed. The old claim that Memex first prompts cannot use attachments or explicit settings is stale.

**Recommendation:** Keep the prepared-session/no-send boundary and inherited-versus-explicit settings distinction.

- [T3: ChatComposer.tsx:2133–2155](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:2133) — Provider-filtered runtime modes and model state apply to composerDraftTarget. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2133-L2155).
- [Memex: HomeConversationComposer.swift:98–119](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/HomeConversationComposer.swift:98) — Creates a prepared conversation without sending and captures attachments.
- [Memex: HomeConversationComposer.swift:125–144](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/HomeConversationComposer.swift:125) — Reachable Home attachment and Model and permissions actions.
- [Memex: StoreNewConversation.swift:71–97](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/StoreNewConversation.swift:71) — Apply settings, attach, transfer durable Home draft, optionally send.
- [Memex: ConversationQueueTests.swift:217–239](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:217) — Existing test exercises settings acknowledgement before returning.

**Limits:** Memex preparation creates a real native session; it is not a purely local settings preview. Runtime-enabled build is required.


### T3-I02: Files, pasted images, and media ergonomics

**Workflow:** Attach files or paste/drop an image into a new or existing prompt.

**T3:** Image/generic-file classification, HEIC-to-JPEG conversion, video preview eligibility and server capability/size validation are implemented.

**Memex:** Explicit file panel and paste/drop capture are implemented; runtime supports capability-checked images/audio/file contents. Images are limited to PNG/JPEG/GIF/WebP and 3 MiB each, with ten attachments and 20 MiB total. File-URL HEIC receives no conversion before validation. Clipboard TIFF/PNG/JPEG bytes are converted to PNG.

**Disposition:** partial; P2; source-confirmed; existing tests inspected where cited, not executed. Basic attachment parity exists; large screenshots and phone-photo files can require manual conversion/resizing in Memex.

**Recommendation:** Add a bounded image-normalization path only if these common inputs are a product priority; preserve captured-byte durability.

- [T3: composerAttachmentFiles.ts:65–136](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/composerAttachmentFiles.ts:65) — Classifies HEIC, generic files, previewable video; blocks unsupported server upload state. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/composerAttachmentFiles.ts#L65-L136).
- [T3: imageCompression.ts:482–507](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/lib/imageCompression.ts:482) — Converts HEIC/HEIF to compatible JPEG. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/lib/imageCompression.ts#L482-L507).
- [T3: composerAttachmentFiles.test.ts:45–60](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/composerAttachmentFiles.test.ts:45) — Existing tests cover image classification. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/composerAttachmentFiles.test.ts#L45-L60).
- [Memex: ConversationComposer.swift:85–92](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:85) — Follow-up composer wires clipboard/drop capture.
- [Memex: ConversationAttachment.swift:9–20](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationAttachment.swift:9) — File attachments go directly through runtime loadFile and validation.
- [Memex: ConversationPromptContext.swift:80–107](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptContext.swift:80) — File URL branch loads original bytes; bitmap paste converts to PNG.
- [Runtime: AgentPromptAttachments.swift:62–95](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:62) — Original file bytes and runtime limits.
- [Runtime: AgentPromptAttachments.swift:115–151](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:115) — Provider capability gates, image MIME validation and byte limits.

**Limits:** T3 accepting a generic video file is not proof every model can understand video. Attachment capability remains provider/model-specific.


### T3-I03: File/chat mentions and slash commands/skills

**Workflow:** Type @ to capture file or conversation context; select a command or skill.

**T3:** T3 combines thread and path suggestions; built-in /model, /plan, /default, provider slash commands, and skill invocation insertion are wired.

**Memex:** Memex wires mention suggestions into AcpComposerView. File choices capture bytes; chat choices capture a bounded excerpt. Built-in slash controls change settings; provider commands are suggestions; local skill/command files are captured as attachments, with $ARGUMENTS substitution for commands.

**Disposition:** present; none; source-confirmed; existing tests inspected where cited, not executed. This is a semantic difference, not a missing composer feature. Memex skill selection includes file contents; T3's inspected skill-selection handler inserts $name for downstream resolution.

**Recommendation:** Preserve explicit capture semantics and label bounded catalogs/excerpts; do not advertise provider CLI slash behavior beyond reported commands.

- [T3: ChatComposer.tsx:2616–2700](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:2616) — Thread/file suggestions and provider-aware slash/skill menu. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2616-L2700).
- [T3: ChatComposer.tsx:3939–4005](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:3939) — Built-in settings controls, provider token insertion, $skill insertion. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L3939-L4005).
- [Memex: ConversationComposer.swift:300–338](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:300) — Built-in/provider/local prompt suggestions.
- [Memex: ConversationComposer.swift:352–405](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:352) — Command dispatch, content capture and argument substitution.
- [Memex: ConversationComposer.swift:409–436](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:409) — File and conversation context wiring.
- [Memex: ConversationComposerCatalog.swift:21–78](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposerCatalog.swift:21) — Local-only bounded 5000-file/1000-prompt catalog; known skills and command roots.
- [Memex: ConversationComposerTests.swift:74–105](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationComposerTests.swift:74) — Existing catalog coverage.

**Limits:** Local file/skill catalog returns empty for server-owned or non-local sessions; remote inventory belongs to another facet owner. Memex conversation mentions are excerpts, not a permanent live link to all chat context.


### T3-I04: Queue, edit, reorder, cancel and promote

**Workflow:** Write follow-up instructions during a running turn, reorder pending messages or steer immediately.

**T3:** T3 routes running submissions through configurable queue/steer/restart policy and provides queued-run edit/reorder/cancel/promotion controls.

**Memex:** Memex routes Enter through selected queue/steer behavior, exposes explicit Queue message/Steer now buttons, persists queued prompt bytes/order, supports edits, movement, removal, hold/resume and promotion. Stop holds queue.

**Disposition:** present; none; source-confirmed; existing tests inspected where cited, not executed. The earlier no-queue/no-steer gap is closed for supported native providers.

**Recommendation:** Keep capability-gated steering and explicit queue hold after uncertainty or stop.

- [T3: composerDispatch.ts:1–31](/private/tmp/t3-chat-inventory-ecfdda5/packages/client-runtime/src/state/composerDispatch.ts:1) — Default/alternate queue-versus-steer dispatch. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/state/composerDispatch.ts#L1-L31).
- [T3: QueuedRunsControl.tsx:168–243](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/QueuedRunsControl.tsx:168) — Reachable queue actions. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/QueuedRunsControl.tsx#L168-L243).
- [T3: QueuedRunsControl.test.tsx:78–171](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/QueuedRunsControl.test.tsx:78) — Existing tests cover thumbnails, optimistic acknowledgement and editing visibility. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/QueuedRunsControl.test.tsx#L78-L171).
- [Memex: ConversationComposer.swift:94–105](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:94) — Explicit queue and steer buttons.
- [Memex: ConversationComposer.swift:352–377](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:352) — Keyboard submission dispatches working follow-up policy.
- [Runtime: AcpComposerView.swift:726–759](/Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:726) — Text submission is wired independently of idle Send button disabled state.
- [Memex: LiveConversation.swift:587–680](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:587) — Durable enqueue and edit/reorder/cancel/hold/resume/promote/drain.
- [Memex: ConversationQueueTests.swift:79–107](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:79) — Existing persisted-queue contract test.

**Limits:** Queue implementations have different ownership/lifetimes; this finding covers active input controls rather than claiming identical server recovery.


### T3-I05: Provider-specific steering fallback

**Workflow:** Steer a running configured ACP provider that has no native steering.

**T3:** T3 policy chooses native active steering or interrupt/restart when explicitly supported. Common ACP adapter advertises interrupt/restart and app queues; Cursor also uses restart, while Pi and Claude advertise native steering without generic restart.

**Memex:** Memex native Codex/Claude advertise steering. Configured ACP descriptors explicitly mark steering unavailable; UI gates on observed actions. Unsupported steer leaves draft and queue intact.

**Disposition:** partial; P2; source-confirmed; existing tests inspected where cited, not executed. T3 covers more active-instruction strategies; Memex does not promise generic ACP steering merely because ACP is configurable.

**Recommendation:** Consider explicit interrupt-and-restart as a separately labeled future capability; never silently equate it to native steering.

- [T3: CommandPolicy.ts:238–269](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/orchestration-v2/CommandPolicy.ts:238) — Capability-checked native versus restart policy. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/CommandPolicy.ts#L238-L269).
- [T3: AcpAdapterV2.ts:553–565](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.ts:553) — Common ACP interrupt/restart and app queue capabilities. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.ts#L553-L565).
- [T3: CursorAdapterV2.ts:102–109](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/orchestration-v2/Adapters/CursorAdapterV2.ts:102) — No native steering; restart support. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/Adapters/CursorAdapterV2.ts#L102-L109).
- [Memex: ConversationProviderCatalog.swift:28–32](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProviderCatalog.swift:28) — Configured ACP steering unavailable.
- [Memex: LiveConversation.swift:214–218](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:214) — Observed capability and interaction gates.
- [Memex: InAppAgentRuntime.swift:207–213](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/InAppAgentRuntime.swift:207) — Steer comes from actual available action.
- [Memex: ConversationQueueTests.swift:174–191](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:174) — Existing no-loss unsupported-steer test.

**Limits:** Adapter declarations plus orchestration policy establish implementation intent, not live compatibility with every installed ACP agent version.


### T3-I06: Stop acknowledgement and safe send recovery

**Workflow:** Stop work; recover after send acknowledgement is lost without duplicated execution.

**T3:** T3 has durable command receipts/event/effect recording, capability-checked interrupt dispatch and adapter error propagation.

**Memex:** Memex persists outgoing prompt/steer before dispatch, marks ambiguous failures uncertain and requires explicit reconnect/review. Stop awaits terminal snapshot and has a timeout warning with queued work held. Runtime Codex interrupt has an acknowledged completion path.

**Disposition:** present; none; source-confirmed; existing tests inspected where cited, not executed. The old discarded-interrupt-response and indefinitely stuck stopping findings do not apply to the inspected current baseline.

**Recommendation:** Preserve exact command/native identity checks and never auto-resend an uncertain message.

- [T3: EventSink.ts:518–576](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/orchestration-v2/EventSink.ts:518) — Command receipt deduplication, event/effect recording and post-commit publication. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/EventSink.ts#L518-L576).
- [T3: ProviderTurnControlService.ts:171–197](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/orchestration-v2/ProviderTurnControlService.ts:171) — Native interrupt and surfaced errors. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ProviderTurnControlService.ts#L171-L197).
- [Memex: LiveConversation.swift:366–368](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:366) — Terminal state clears stopping.
- [Memex: LiveConversation.swift:717–754](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:717) — Holds queue, handles pre-dispatch cancellation and missing terminal timeout.
- [Memex: LiveConversation.swift:779–818](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:779) — Persist-before-provider, uncertain failure and explicit reconnect.
- [Runtime: CodexAppServerTransport.swift:1119–1127](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1119) — Propagates turn/interrupt completion.
- [Memex: ConversationQueueTests.swift:108–148](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:108) — Terminal-state, rejected-stop and timeout tests.
- [Memex: ConversationQueueTests.swift:192–216](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQueueTests.swift:192) — Existing recovery contract.

**Limits:** No failure was induced in a live provider; source and tests were inspected only. T3 process/server restart semantics belong to the lifecycle facet, not this active-send comparison. Server command receipts do not establish a durable ordinary web-client send intent; see T3-I14 for the separate client recovery defect.


### T3-I07: Structured choices and question attachments

**Workflow:** Answer several pending questions with multi-select/custom text and context.

**T3:** T3 supports choice progress, multi-select, keyboard selection, per-question image/file uploads, non-resumable state and dismissible requests; message-mode requests can remain answerable after their turn ends.

**Memex:** Memex now supplies multi-select, descriptions, secret/default fields, per-question drafts and Back/Next navigation. Answers preserve native choice values and request IDs. Attachments deliberately accept captured UTF-8 only, up to 10 files/1 MiB, and Cancel stops the conversation rather than dismissing one request.

**Disposition:** partial; P2; source-confirmed; existing tests inspected where cited, not executed. Multi-question UX is present; media answers and asynchronous dismissal are still meaningful semantic differences.

**Recommendation:** Keep text-only labeling accurate. Add media/dismissal only through provider contracts that can actually represent them.

- [T3: ComposerPendingUserInputPanel.tsx:67–70](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:67) — Message-mode versus non-resumable response state. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L67-L70).
- [T3: ComposerPendingUserInputPanel.tsx:121–169](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:121) — Multi-select and number keys. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L121-L169).
- [T3: ComposerPendingUserInputPanel.tsx:203–225](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:203) — Dismissibility is request-dependent. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L203-L225).
- [T3: ChatView.tsx:9783–9816](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:9783) — Per-question uploaded image/file payload collection. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9783-L9816).
- [Memex: ConversationQuestionsView.swift:21–78](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQuestionsView.swift:21) — Navigation, structured draft, Stop cancellation and answer encoding.
- [Memex: ConversationQuestionsView.swift:105–111](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQuestionsView.swift:105) — Passes descriptions, multiSelect and secret/default fields.
- [Memex: ConversationQuestionContext.swift:13–46](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationQuestionContext.swift:13) — Bounded text-only attachment support.
- [Memex: ConversationQuestionTests.swift:6–47](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationQuestionTests.swift:6) — Captured bytes, media rejection, stable native identity navigation.

**Limits:** Question media is a T3 orchestration feature; no universal assertion that every provider accepts equivalent native media replies. Memex question drafts are view-local state, distinct from durable prompt drafts.


### T3-I08: Provider-specific approval options

**Workflow:** Inspect a provider approval and choose its exact decision.

**T3:** T3 renders provider-supplied labels, warnings and primary/secondary options, with live response capability supplied by parent.

**Memex:** Memex maps exact option IDs/titles/kinds and approval detail to SQACPUI; replies preserve request/option IDs and are guarded against stale requests. Cancel invokes Stop.

**Disposition:** present; none; source-confirmed; existing tests inspected where cited, not executed. Basic approval interaction is implemented in both. Warning/detail semantics differ by provider and projection.

**Recommendation:** Keep native option identity and explicit response gating; avoid reducing all providers to one universal allow/deny policy.

- [T3: ComposerPendingApprovalActions.tsx:24–59](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx:24) — Provider options plus default primary decisions and warnings. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx#L24-L59).
- [Memex: ConversationComposer.swift:52–67](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:52) — Exact options, details and response callbacks.
- [Memex: LiveConversation.swift:757–760](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:757) — Validates active approval/option and sends exact IDs.
- [Memex: LiveConversationTests.swift:508–536](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/LiveConversationTests.swift:508) — Existing exact-ID response contract.


### T3-I09: T3 non-live approval overflow actions remain enabled

**Workflow:** An approval is visible but its responseCapability is not live; open More approval options and select Cancel or session-wide approval.

**T3:** Primary buttons disable on !canRespond, but More trigger and its menu items only disable on isResponding. The parent callback immediately returns for a non-live request, so these enabled controls silently do nothing.

**Memex:** Memex supplies canRespond to the shared approval component and validates the current request/option in its response method. No equivalent T3 split primary/overflow gate is introduced by the Memex integration.

**Disposition:** defect; P2; source-confirmed; existing tests inspected where cited, not executed. This is a concrete misleading/dead-control defect in T3, not an authorization bypass.

**Recommendation:** Do not copy the inconsistent gate: apply response availability to all decisions and explain expired requests.

- [T3: ChatComposer.tsx:6651–6658](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:6651) — Reachable component receives live-only canRespond. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L6651-L6658).
- [T3: ComposerPendingApprovalActions.tsx:49–85](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx:49) — Primary respects canRespond; menu does not. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx#L49-L85).
- [T3: ChatView.tsx:9750–9757](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:9750) — Non-live callbacks silently return. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9750-L9757).
- [T3: ComposerPendingApprovalActions.test.tsx:7–63](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ComposerPendingApprovalActions.test.tsx:7) — Tests all use canRespond=true; inspected suite does not cover expired overflow actions. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.test.tsx#L7-L63).
- [Memex: ConversationComposer.swift:52–67](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:52) — Shared pending-item canRespond is supplied.
- [Memex: LiveConversation.swift:757–760](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:757) — Current request and option guards.
- [Runtime: AcpComposerView.swift:895–909](/Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:895) — Shared approval control disables options using approval.canRespond.

**Limits:** Source-established UI defect; no interactive reproduction performed. Memex connected-state gating is not identical to T3 native responseCapability; no universal superiority claim.


### T3-I10: Prompt stash and recall

**Workflow:** Save a prompt for later and recover the unsent draft after navigating prompt history.

**T3:** T3 provides prompt history navigation and a 20-entry stash with rich records/files/images. Capacity evicts the oldest with a warning; restore consumes an entry.

**Memex:** Memex provides previous/next/recent prompt recall, a 20-entry stash that refuses overflow and copies on restore; captured attachment bytes are saved atomically before clearing unchanged input.

**Disposition:** present; none; source-confirmed; existing tests inspected where cited, not executed. Parity exists and Memex's explicit copy/delete policy avoids destructive stash eviction.

**Recommendation:** Keep the non-destructive Memex stash policy rather than adopting every T3 interaction literally.

- [T3: ChatComposer.tsx:4299–4367](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:4299) — History handling is wired to composer. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4299-L4367).
- [T3: ChatComposer.tsx:4961–4973](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:4961) — Oldest entry evicted on overflow. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4961-L4973).
- [Memex: ConversationComposer.swift:191–269](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:191) — Reachable stash/recall and unchanged-input clearing.
- [Memex: ConversationPromptLibrary.swift:15–74](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptLibrary.swift:15) — Copy-on-restore policy, full rejection and atomic captured-payload save.
- [Memex: ConversationComposerTests.swift:12–61](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationComposerTests.swift:12) — Roundtrip, full/unreadable store and original draft recall.


### T3-I11: T3 stash image loss window

**Workflow:** Stash an image-bearing prompt, then close/reload the tab before asynchronous image compression finishes.

**T3:** T3 writes a text-only entry with pendingImageCount, clears the composer and releases image uploads, then asynchronously encodes and persists images. Hydration explicitly reports those pending images lost if reload interrupts the encode. Storage quota or encoding limits can also omit images; warnings disclose those outcomes.

**Memex:** Memex saves text plus captured attachment content atomically before clearing the unchanged draft and refuses a failed save.

**Disposition:** defect; P2; source-confirmed; existing tests inspected where cited, not executed. T3 has a source-established data-loss window for unsent image context. Its warning/recovery design preserves text but cannot recover those image bytes.

**Recommendation:** Do not copy the two-phase destructive image stash. Retain the original draft or durably stage image bytes before clearing.

- [T3: ChatComposer.tsx:4898–4956](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:4898) — Text-only stash followed by composer clear and upload release. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4898-L4956).
- [T3: ChatComposer.tsx:4984–5025](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:4984) — Image encoding occurs after clear and may be non-durable. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4984-L5025).
- [T3: promptStashStore.ts:75–101](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/promptStashStore.ts:75) — Reload explicitly converts interrupted encodes to missing-image notices. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/promptStashStore.ts#L75-L101).
- [Memex: ConversationPromptLibrary.swift:40–74](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPromptLibrary.swift:40) — Whole captured entry saved atomically.
- [Memex: ConversationComposer.swift:233–241](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:233) — Clears only after awaited successful save and only if input is unchanged.

**Limits:** This is a bounded crash/reload defect, not a claim that normal completed stash operations lose images. T3 intentionally compresses stash images and warns about lossy/omitted content; preserve that distinction from accidental silent loss.


### T3-I12: Provider catalog, model/effort and permission configuration

**Workflow:** Choose an executable provider and its reported model, effort and permission options.

**T3:** T3 ships eight driver types: Codex, Claude, Cursor, Grok, OpenCode, Antigravity, Pi and ACP Registry. Model picker is instance-aware; traits and runtime modes are provider-filtered.

**Memex:** Memex has Codex and Claude native integrations plus explicitly configured ACP executables/homes/arguments. It discovers model and nonempty configuration options, preserves unknown inherited values, remembers explicit provider-scoped choices, and waits for acknowledgement. It does not ship the remaining dedicated T3 driver integrations merely because it ingests their history.

**Disposition:** partial; P2; source-confirmed; existing tests inspected where cited, not executed. Provider breadth remains a real gap, but describing Memex as Codex/Claude-only is also incomplete.

**Recommendation:** Add dedicated adapters only for demonstrated workflow demand; retain negotiated ACP capability boundaries and explicit installation identity.

- [T3: builtInDrivers.ts:52–62](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/provider/builtInDrivers.ts:52) — Authoritative shipped driver list. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/provider/builtInDrivers.ts#L52-L62).
- [T3: ProviderModelPicker.tsx:75–96](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ProviderModelPicker.tsx:75) — Account/instance-aware selected model state. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ProviderModelPicker.tsx#L75-L96).
- [T3: ChatComposer.tsx:2131–2155](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/chat/ChatComposer.tsx:2131) — Provider-supported mode filtering and model state. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2131-L2155).
- [Memex: ConversationProviderCatalog.swift:28–67](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationProviderCatalog.swift:28) — Configured ACP and two native built-ins.
- [Memex: InAppAgentRuntime.swift:124–135](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/InAppAgentRuntime.swift:124) — Actual configured ACP/Codex/Claude connection paths.
- [Memex: ConversationComposer.swift:139–182](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:139) — Reported model/config selectors and inherited defaults.
- [Memex: LiveConversation.swift:540–569](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:540) — Observed setting acknowledgement and timeout.
- [Memex: ConversationProviderCatalogTests.swift:6–55](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/ConversationProviderCatalogTests.swift:6) — Executable-vs-ingestion identity and resume capability tests.

**Limits:** Driver presence is not proof that the required binary/account is configured or that every provider supports every interaction. Existing chat model/effort changes are blocked while working in Memex; settings are applied when idle.


### T3-I13: Build/runtime availability boundary

**Workflow:** Install a build and expect active chat functionality to exist.

**T3:** T3 server's shipped provider driver list is built into its normal provider architecture; external provider readiness still controls usability.

**Memex:** Runtime dependencies are included by default when MEMEX_AGENT_RUNTIME_ROOT or .local-runtime-root resolves to a runtime checkout. Package.swift imports SQACP/SQACPHost/SQACPUI from that local root and links its archive. MEMEX_HISTORY_ONLY=1 explicitly removes these dependencies; without a resolved root the conditional Home fallback is EmptyView.

**Disposition:** not-applicable; none; source-confirmed; existing tests inspected where cited, not executed. This is an intentional packaging/provenance qualifier, not a demonstrated feature gap in the supplied runtime-enabled build. Installed-binary runtime provenance was not measured.

**Recommendation:** Make release/runtime provenance explicit in final synthesis; do not count dormant dependency APIs as shipped app capability.

- [T3: builtInDrivers.ts:1–18](/private/tmp/t3-chat-inventory-ecfdda5/apps/server/src/provider/builtInDrivers.ts:1) — Configured providers resolve through shipped drivers. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/provider/builtInDrivers.ts#L1-L18).
- [Memex: Package.swift:4–38](/Users/nico/Code/memex-chat-capabilities/apps/macos/Package.swift:4) — Machine-local optional runtime and archive linkage.
- [Memex: ConversationComposer.swift:4–8](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationComposer.swift:4) — Full composer requires SQACPUI.
- [Memex: HomeConversationComposer.swift:226–231](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/HomeConversationComposer.swift:226) — Home composer fallback is empty.

**Limits:** No app was built, launched or provider prompted in this task; installed-binary behavior remains unverified. The supplied Memex and runtime commits are separate baselines rather than an assertion that Package.swift pins the latter.


### T3-I14: T3 ordinary send clears durable draft before acknowledgement and retries with new identity

**Workflow:** Send an ordinary single-target prompt, then close/reload before dispatch completes, or retry after a failure whose original server acceptance is uncertain.

**T3:** The web path snapshots content into local function variables and an optimistic React state row, clears composer content, then awaits settings/upload preparation and startThreadTurn. Failure restores from that closure only when the current draft is empty. A fresh onSend allocates a new messageId; startThreadTurn allocates a fresh commandId because the call supplies no retained commandId. The ordinary path has no durable uncertain-intent record or same-identity retry guard.

**Memex:** Memex persists the outgoing command identity and captured attachments before provider dispatch; ambiguous errors become uncertain. Recovery requires reconnect/review, and restoring the pending prompt does not send it. Queue recovery also retains unconfirmed command identity.

**Disposition:** defect; P1; source-confirmed failure window and identity allocation; no crash or duplicate-provider execution induced. A reload/close during a pre-dispatch await can discard the only recoverable prompt snapshot. After accepted-then-lost acknowledgement, a manually retried ordinary send has new identities, so server receipt deduplication cannot identify it as the original command. Duplicate execution is a supported risk, not an observed runtime result.

**Recommendation:** Do not copy this client recovery design. Persist the complete outbound intent and original command/message IDs before clearing, reconcile receipt/native state after reconnect, and distinguish rejected from ambiguous outcomes.

- [T3: ChatView.tsx:1816–1816](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:1816) — Ordinary optimistic input is React useState. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L1816-L1816).
- [T3: ChatView.tsx:8911–8944](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:8911) — Content remains in closure; new message ID allocated for each submission. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L8911-L8944).
- [T3: ChatView.tsx:9449–9451](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:9449) — Clears composer before later asynchronous work. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9449-L9451).
- [T3: ChatView.tsx:9485–9507](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:9485) — Pre-dispatch awaits occur after durable composer clear. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9485-L9507).
- [T3: ChatView.tsx:9549–9593](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:9549) — New ordinary send passes messageId but no retained commandId. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9549-L9593).
- [T3: ChatView.tsx:9665–9723](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:9665) — Restores local snapshots conditionally and reports generic failure. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9665-L9723).
- [T3: commands.ts:261–270](/private/tmp/t3-chat-inventory-ecfdda5/packages/client-runtime/src/operations/commands.ts:261) — Missing caller ID creates a random UUID. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L261-L270).
- [T3: commands.ts:626–639](/private/tmp/t3-chat-inventory-ecfdda5/packages/client-runtime/src/operations/commands.ts:626) — Allocates command identity before preparing attachment command payload. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L626-L639).
- [T3: commands.ts:755–772](/private/tmp/t3-chat-inventory-ecfdda5/packages/client-runtime/src/operations/commands.ts:755) — Dispatch uses newly allocated command and message identity. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L755-L772).
- [T3: ChatView.tsx:9156–9168](/private/tmp/t3-chat-inventory-ecfdda5/apps/web/src/components/ChatView.tsx:9156) — Separate multi-model branch has a guard; it is excluded from this finding. [Pinned upstream](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9156-L9168).
- [Memex: LiveConversation.swift:779–818](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/LiveConversation.swift:779) — Persistent intent precedes provider boundary, ambiguous result requires reconnect.
- [Memex: ConversationPendingView.swift:26–34](/Users/nico/Code/memex-chat-capabilities/apps/macos/Sources/Memex/ConversationPendingView.swift:26) — Restore draft warns review first and does not resend.
- [Memex: LiveConversationTests.swift:159–208](/Users/nico/Code/memex-chat-capabilities/apps/macos/Tests/MemexTests/LiveConversationTests.swift:159) — Existing tests cover no replay and native identity confirmation.

**Limits:** Bounded to the ordinary single-target web send branch. Multi-model fanout has a separate in-memory uncertain-submission map; it was not audited here for persistence. Server-side receipts remain useful for retries of the same command ID; the defect is the UI's loss of original outbound intent/identity. No test claiming an induced reload or duplicate execution was run.

## Boundaries and unresolved observations

- **Installed/runtime validation — not performed by contract:** No builds/tests/provider prompts/app launches. Source-reachable implementation and read-only existing test evidence only.
- **T3 arbitrary provider parity — provider/version-dependent:** Eight shipped drivers do not imply universal native capabilities. Configured binaries/accounts and agent versions unverified.
- **Workspace/transcript/history/remote/schedules/organization — owned elsewhere:** Only mentions and prompt recall touched as active-input integration boundaries; no independent facet inventory performed.

Existing tests were read, not run. Source-reachable implementation and existing test contracts are not runtime acceptance. No arbitrary completeness score is assigned.
