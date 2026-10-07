# Current T3 versus Memex chat inventory

T3 `ecfdda5fa804582066c1aa62afea419677c24883` (fresh upstream main; 57 commits beyond the old guide), Memex `17e958778a7398db8238cc9c8415ca12649067e5`, runtime `7818518b094f44911aecfec7dbeb905c57ea24a9`. Research-only source comparison. 39 heterogeneous dispositions and 228 checked source ranges are an audit trail, not a completeness score.

Current Memex closes most broad gaps in the earlier review: first-turn preparation, attachments/context/skills, stash/recall, queues/steering, plans and agents, forks/lineage/context return, organization/notifications/outline, local Git/files/rewind/terminal/browser automation, remote conversations, schedules, and opt-in app-control MCP all have reachable implementations.

The strongest remaining differences are remote workspace files/Git/terminal surfaces, dedicated provider breadth, inline PR review, finer browser/app-tool operations, event/fresh-thread automation, child navigation before indexing, image/question media ergonomics, and customizable keyboard bindings. None imply these broad categories are wholly absent.

Memex advantages to preserve include indexed filtered history and exact paged Find/raw evidence; durable captured prompt bytes and explicit uncertainty recovery; refusal to destructively evict stash entries; separate checkpoint index/worktree restoration; file fingerprint conflict detection; reversible local conversation removal; and review-before-send context transfers.

T3 shortcomings not to copy: ordinary single-target send loses durable draft state before acknowledgement and manual retry allocates new identity; image stash clears before image bytes are durable; editor writes lack a disk-version precondition; checkpoint restore cannot retain the staged/unstaged split; stale approval overflow controls remain enabled but the handler refuses them. These are scoped source findings, not runtime incident claims.

Runtime-enabled Memex is the comparison target. Optional dependency packaging is an applicability qualifier. No builds, tests, apps, provider prompts or UI actions were performed by this research tree. No speed ranking, universal provider support, arbitrary desktop computer-use support or managed cloud product is inferred. Sidebar snooze is intentionally excluded.

## Coverage

| Facet | Evidence rows |
|---|---|
| composer and input/attachments/context | T3-I01, T3-I02, T3-I03, T3-I10, T3-I11 |
| queue/steer/stop/recovery | T3-I04, T3-I05, T3-I06, T3-I14 |
| plan/tasks/subagents/forks | T3-TR-01, T3-TR-02, T3-TR-03 |
| transcript/rendering/search/history | T3-TR-04, T3-TR-05, T3-TR-06, T3-TR-07 |
| approval/questions | T3-I07, T3-I08, T3-I09 |
| providers/models/configuration | T3-I12, T3-I13 |
| projects/worktrees/Git/review/files | T3-W01, T3-W02, T3-W03, T3-W04, T3-W05, T3-W06 |
| terminal/browser/computer use | T3-W07, T3-W08, T3-W12 |
| remote/cloud/multi-host | T3-W09, T3-W12 |
| schedules/automation | T3-W10 |
| app-control tools/plugins/skills | T3-W11, T3-W12, T3-I03 |
| organization/notifications/settings | T3-TR-08, T3-TR-09, T3-TR-11 |
| performance/accessibility/keybindings | T3-TR-10, T3-TR-12, T3-TR-13 |

## Findings

| ID | Capability | Disposition | Priority |
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
| T3-TR-01 | Structured plans and implementation | present | none |
| T3-TR-02 | Live subagent visibility and navigation | partial | P2 |
| T3-TR-03 | Forks, lineage and context return | present | none |
| T3-TR-04 | Typed transcript fidelity | present | none |
| T3-TR-05 | Historical search scope | present | none |
| T3-TR-06 | Exact in-conversation Find and raw evidence | present | none |
| T3-TR-07 | Conversation outline and reading position | present | none |
| T3-TR-08 | Rename, pin, archive, removal and bulk ordering | present | none |
| T3-TR-09 | Completion and input notifications | present | none |
| T3-TR-11 | Sidebar snooze | intentionally-excluded | none |
| T3-TR-10 | Customizable keyboard bindings | partial | P2 |
| T3-TR-12 | Long-history publish architecture | unknown | none |
| T3-TR-13 | Accessibility hooks | present | none |
| T3-W01 | Managed worktrees and lifecycle | present | none |
| T3-W02 | Git controls and inline pull-request workspace | partial | P2 |
| T3-W03 | Checkpoint and conversation rewind | present | none |
| T3-W04 | T3 checkpoint staging-state loss | defect | P1 |
| T3-W05 | File browsing, editing and previews | present | none |
| T3-W06 | T3 editor concurrent-write overwrite | defect | P1 |
| T3-W07 | Persistent embedded terminal | present | none |
| T3-W08 | Browser automation and app-owned preview | partial | P2 |
| T3-W09 | Remote execution versus remote workspace surfaces | partial | P1 |
| T3-W10 | Time-based and event-triggered recurring work | partial | P2 |
| T3-W11 | Agent-callable application toolkit breadth and registration | partial | P2 |
| T3-W12 | Unproven universal capabilities | unknown | none |

## Source-backed detail

### T3-I01 — First and follow-up prompts with pre-send configuration

**present; none; source-confirmed; existing tests inspected where cited, not executed.** Start from Home; select model/permissions or attachments before sending the first prompt; continue in the chat.

**T3:** T3 uses the same rich composer and provider/model state for draft and existing thread targets.

**Memex:** Home supports Attach files, paste/drop, Model and permissions preparation without sending, then opens the full composer. Explicit preferences are applied and observed before any immediate first send.

The old claim that Memex first prompts cannot use attachments or explicit settings is stale.

**Recommendation:** Keep the prepared-session/no-send boundary and inherited-versus-explicit settings distinction.

**Limits:** ['Memex preparation creates a real native session; it is not a purely local settings preview.', 'Runtime-enabled build is required.']

- [T3 apps/web/src/components/chat/ChatComposer.tsx:2133–2155](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2133-L2155): Provider-filtered runtime modes and model state apply to composerDraftTarget.
- [Memex apps/macos/Sources/Memex/HomeConversationComposer.swift:98–119](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/HomeConversationComposer.swift#L98-L119): Creates a prepared conversation without sending and captures attachments.
- [Memex apps/macos/Sources/Memex/HomeConversationComposer.swift:125–144](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/HomeConversationComposer.swift#L125-L144): Reachable Home attachment and Model and permissions actions.
- [Memex apps/macos/Sources/Memex/StoreNewConversation.swift:71–97](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreNewConversation.swift#L71-L97): Apply settings, attach, transfer durable Home draft, optionally send.
- [Memex apps/macos/Tests/MemexTests/ConversationQueueTests.swift:217–239](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L217-L239): Existing test exercises settings acknowledgement before returning.

### T3-I02 — Files, pasted images, and media ergonomics

**partial; P2; source-confirmed; existing tests inspected where cited, not executed.** Attach files or paste/drop an image into a new or existing prompt.

**T3:** Image/generic-file classification, HEIC-to-JPEG conversion, video preview eligibility and server capability/size validation are implemented.

**Memex:** Explicit file panel and paste/drop capture are implemented; runtime supports capability-checked images/audio/file contents. Images are limited to PNG/JPEG/GIF/WebP and 3 MiB each, with ten attachments and 20 MiB total. File-URL HEIC receives no conversion before validation. Clipboard TIFF/PNG/JPEG bytes are converted to PNG.

Basic attachment parity exists; large screenshots and phone-photo files can require manual conversion/resizing in Memex.

**Recommendation:** Add a bounded image-normalization path only if these common inputs are a product priority; preserve captured-byte durability.

**Limits:** ['T3 accepting a generic video file is not proof every model can understand video.', 'Attachment capability remains provider/model-specific.']

- [T3 apps/web/src/components/chat/composerAttachmentFiles.ts:65–136](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/composerAttachmentFiles.ts#L65-L136): Classifies HEIC, generic files, previewable video; blocks unsupported server upload state.
- [T3 apps/web/src/lib/imageCompression.ts:482–507](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/lib/imageCompression.ts#L482-L507): Converts HEIC/HEIF to compatible JPEG.
- [T3 apps/web/src/components/chat/composerAttachmentFiles.test.ts:45–60](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/composerAttachmentFiles.test.ts#L45-L60): Existing tests cover image classification.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:85–92](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L85-L92): Follow-up composer wires clipboard/drop capture.
- [Memex apps/macos/Sources/Memex/ConversationAttachment.swift:9–20](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationAttachment.swift#L9-L20): File attachments go directly through runtime loadFile and validation.
- [Memex apps/macos/Sources/Memex/ConversationPromptContext.swift:80–107](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptContext.swift#L80-L107): File URL branch loads original bytes; bitmap paste converts to PNG.
- [Memex runtime packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:62–95](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:62): Original file bytes and runtime limits.
- [Memex runtime packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:115–151](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentPromptAttachments.swift:115): Provider capability gates, image MIME validation and byte limits.

### T3-I03 — File/chat mentions and slash commands/skills

**present; none; source-confirmed; existing tests inspected where cited, not executed.** Type @ to capture file or conversation context; select a command or skill.

**T3:** T3 combines thread and path suggestions; built-in /model, /plan, /default, provider slash commands, and skill invocation insertion are wired.

**Memex:** Memex wires mention suggestions into AcpComposerView. File choices capture bytes; chat choices capture a bounded excerpt. Built-in slash controls change settings; provider commands are suggestions; local skill/command files are captured as attachments, with $ARGUMENTS substitution for commands.

This is a semantic difference, not a missing composer feature. Memex skill selection includes file contents; T3's inspected skill-selection handler inserts $name for downstream resolution.

**Recommendation:** Preserve explicit capture semantics and label bounded catalogs/excerpts; do not advertise provider CLI slash behavior beyond reported commands.

**Limits:** ['Local file/skill catalog returns empty for server-owned or non-local sessions; remote inventory belongs to another facet owner.', 'Memex conversation mentions are excerpts, not a permanent live link to all chat context.']

- [T3 apps/web/src/components/chat/ChatComposer.tsx:2616–2700](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2616-L2700): Thread/file suggestions and provider-aware slash/skill menu.
- [T3 apps/web/src/components/chat/ChatComposer.tsx:3939–4005](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L3939-L4005): Built-in settings controls, provider token insertion, $skill insertion.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:300–338](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L300-L338): Built-in/provider/local prompt suggestions.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:352–405](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L352-L405): Command dispatch, content capture and argument substitution.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:409–436](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L409-L436): File and conversation context wiring.
- [Memex apps/macos/Sources/Memex/ConversationComposerCatalog.swift:21–78](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposerCatalog.swift#L21-L78): Local-only bounded 5000-file/1000-prompt catalog; known skills and command roots.
- [Memex apps/macos/Tests/MemexTests/ConversationComposerTests.swift:74–105](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationComposerTests.swift#L74-L105): Existing catalog coverage.

### T3-I04 — Queue, edit, reorder, cancel and promote

**present; none; source-confirmed; existing tests inspected where cited, not executed.** Write follow-up instructions during a running turn, reorder pending messages or steer immediately.

**T3:** T3 routes running submissions through configurable queue/steer/restart policy and provides queued-run edit/reorder/cancel/promotion controls.

**Memex:** Memex routes Enter through selected queue/steer behavior, exposes explicit Queue message/Steer now buttons, persists queued prompt bytes/order, supports edits, movement, removal, hold/resume and promotion. Stop holds queue.

The earlier no-queue/no-steer gap is closed for supported native providers.

**Recommendation:** Keep capability-gated steering and explicit queue hold after uncertainty or stop.

**Limits:** ['Queue implementations have different ownership/lifetimes; this finding covers active input controls rather than claiming identical server recovery.']

- [T3 packages/client-runtime/src/state/composerDispatch.ts:1–31](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/state/composerDispatch.ts#L1-L31): Default/alternate queue-versus-steer dispatch.
- [T3 apps/web/src/components/chat/QueuedRunsControl.tsx:168–243](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/QueuedRunsControl.tsx#L168-L243): Reachable queue actions.
- [T3 apps/web/src/components/chat/QueuedRunsControl.test.tsx:78–171](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/QueuedRunsControl.test.tsx#L78-L171): Existing tests cover thumbnails, optimistic acknowledgement and editing visibility.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:94–105](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L94-L105): Explicit queue and steer buttons.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:352–377](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L352-L377): Keyboard submission dispatches working follow-up policy.
- [Memex runtime packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:726–759](/Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:726): Text submission is wired independently of idle Send button disabled state.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:587–680](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L587-L680): Durable enqueue and edit/reorder/cancel/hold/resume/promote/drain.
- [Memex apps/macos/Tests/MemexTests/ConversationQueueTests.swift:79–107](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L79-L107): Existing persisted-queue contract test.

### T3-I05 — Provider-specific steering fallback

**partial; P2; source-confirmed; existing tests inspected where cited, not executed.** Steer a running configured ACP provider that has no native steering.

**T3:** T3 policy chooses native active steering or interrupt/restart when explicitly supported. Common ACP adapter advertises interrupt/restart and app queues; Cursor also uses restart, while Pi and Claude advertise native steering without generic restart.

**Memex:** Memex native Codex/Claude advertise steering. Configured ACP descriptors explicitly mark steering unavailable; UI gates on observed actions. Unsupported steer leaves draft and queue intact.

T3 covers more active-instruction strategies; Memex does not promise generic ACP steering merely because ACP is configurable.

**Recommendation:** Consider explicit interrupt-and-restart as a separately labeled future capability; never silently equate it to native steering.

**Limits:** ['Adapter declarations plus orchestration policy establish implementation intent, not live compatibility with every installed ACP agent version.']

- [T3 apps/server/src/orchestration-v2/CommandPolicy.ts:238–269](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/CommandPolicy.ts#L238-L269): Capability-checked native versus restart policy.
- [T3 apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.ts:553–565](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/Adapters/AcpAdapterV2.ts#L553-L565): Common ACP interrupt/restart and app queue capabilities.
- [T3 apps/server/src/orchestration-v2/Adapters/CursorAdapterV2.ts:102–109](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/Adapters/CursorAdapterV2.ts#L102-L109): No native steering; restart support.
- [Memex apps/macos/Sources/Memex/ConversationProviderCatalog.swift:28–32](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProviderCatalog.swift#L28-L32): Configured ACP steering unavailable.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:214–218](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L214-L218): Observed capability and interaction gates.
- [Memex apps/macos/Sources/Memex/InAppAgentRuntime.swift:207–213](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L207-L213): Steer comes from actual available action.
- [Memex apps/macos/Tests/MemexTests/ConversationQueueTests.swift:174–191](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L174-L191): Existing no-loss unsupported-steer test.

### T3-I06 — Stop acknowledgement and safe send recovery

**present; none; source-confirmed; existing tests inspected where cited, not executed.** Stop work; recover after send acknowledgement is lost without duplicated execution.

**T3:** T3 has durable command receipts/event/effect recording, capability-checked interrupt dispatch and adapter error propagation.

**Memex:** Memex persists outgoing prompt/steer before dispatch, marks ambiguous failures uncertain and requires explicit reconnect/review. Stop awaits terminal snapshot and has a timeout warning with queued work held. Runtime Codex interrupt has an acknowledged completion path.

The old discarded-interrupt-response and indefinitely stuck stopping findings do not apply to the inspected current baseline.

**Recommendation:** Preserve exact command/native identity checks and never auto-resend an uncertain message.

**Limits:** ['No failure was induced in a live provider; source and tests were inspected only.', 'T3 process/server restart semantics belong to the lifecycle facet, not this active-send comparison.', 'Server command receipts do not establish a durable ordinary web-client send intent; see T3-I14 for the separate client recovery defect.']

- [T3 apps/server/src/orchestration-v2/EventSink.ts:518–576](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/EventSink.ts#L518-L576): Command receipt deduplication, event/effect recording and post-commit publication.
- [T3 apps/server/src/orchestration-v2/ProviderTurnControlService.ts:171–197](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ProviderTurnControlService.ts#L171-L197): Native interrupt and surfaced errors.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:366–368](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L366-L368): Terminal state clears stopping.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:717–754](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L717-L754): Holds queue, handles pre-dispatch cancellation and missing terminal timeout.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:779–818](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L779-L818): Persist-before-provider, uncertain failure and explicit reconnect.
- [Memex runtime packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1119–1127](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1119): Propagates turn/interrupt completion.
- [Memex apps/macos/Tests/MemexTests/ConversationQueueTests.swift:108–148](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L108-L148): Terminal-state, rejected-stop and timeout tests.
- [Memex apps/macos/Tests/MemexTests/ConversationQueueTests.swift:192–216](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQueueTests.swift#L192-L216): Existing recovery contract.

### T3-I07 — Structured choices and question attachments

**partial; P2; source-confirmed; existing tests inspected where cited, not executed.** Answer several pending questions with multi-select/custom text and context.

**T3:** T3 supports choice progress, multi-select, keyboard selection, per-question image/file uploads, non-resumable state and dismissible requests; message-mode requests can remain answerable after their turn ends.

**Memex:** Memex now supplies multi-select, descriptions, secret/default fields, per-question drafts and Back/Next navigation. Answers preserve native choice values and request IDs. Attachments deliberately accept captured UTF-8 only, up to 10 files/1 MiB, and Cancel stops the conversation rather than dismissing one request.

Multi-question UX is present; media answers and asynchronous dismissal are still meaningful semantic differences.

**Recommendation:** Keep text-only labeling accurate. Add media/dismissal only through provider contracts that can actually represent them.

**Limits:** ['Question media is a T3 orchestration feature; no universal assertion that every provider accepts equivalent native media replies.', 'Memex question drafts are view-local state, distinct from durable prompt drafts.']

- [T3 apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:67–70](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L67-L70): Message-mode versus non-resumable response state.
- [T3 apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:121–169](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L121-L169): Multi-select and number keys.
- [T3 apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx:203–225](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingUserInputPanel.tsx#L203-L225): Dismissibility is request-dependent.
- [T3 apps/web/src/components/ChatView.tsx:9783–9816](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9783-L9816): Per-question uploaded image/file payload collection.
- [Memex apps/macos/Sources/Memex/ConversationQuestionsView.swift:21–78](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQuestionsView.swift#L21-L78): Navigation, structured draft, Stop cancellation and answer encoding.
- [Memex apps/macos/Sources/Memex/ConversationQuestionsView.swift:105–111](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQuestionsView.swift#L105-L111): Passes descriptions, multiSelect and secret/default fields.
- [Memex apps/macos/Sources/Memex/ConversationQuestionContext.swift:13–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationQuestionContext.swift#L13-L46): Bounded text-only attachment support.
- [Memex apps/macos/Tests/MemexTests/ConversationQuestionTests.swift:6–47](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationQuestionTests.swift#L6-L47): Captured bytes, media rejection, stable native identity navigation.

### T3-I08 — Provider-specific approval options

**present; none; source-confirmed; existing tests inspected where cited, not executed.** Inspect a provider approval and choose its exact decision.

**T3:** T3 renders provider-supplied labels, warnings and primary/secondary options, with live response capability supplied by parent.

**Memex:** Memex maps exact option IDs/titles/kinds and approval detail to SQACPUI; replies preserve request/option IDs and are guarded against stale requests. Cancel invokes Stop.

Basic approval interaction is implemented in both. Warning/detail semantics differ by provider and projection.

**Recommendation:** Keep native option identity and explicit response gating; avoid reducing all providers to one universal allow/deny policy.

**Limits:** []

- [T3 apps/web/src/components/chat/ComposerPendingApprovalActions.tsx:24–59](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx#L24-L59): Provider options plus default primary decisions and warnings.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:52–67](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L52-L67): Exact options, details and response callbacks.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:757–760](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L757-L760): Validates active approval/option and sends exact IDs.
- [Memex apps/macos/Tests/MemexTests/LiveConversationTests.swift:508–536](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/LiveConversationTests.swift#L508-L536): Existing exact-ID response contract.

### T3-I09 — T3 non-live approval overflow actions remain enabled

**defect; P2; source-confirmed; existing tests inspected where cited, not executed.** An approval is visible but its responseCapability is not live; open More approval options and select Cancel or session-wide approval.

**T3:** Primary buttons disable on !canRespond, but More trigger and its menu items only disable on isResponding. The parent callback immediately returns for a non-live request, so these enabled controls silently do nothing.

**Memex:** Memex supplies canRespond to the shared approval component and validates the current request/option in its response method. No equivalent T3 split primary/overflow gate is introduced by the Memex integration.

This is a concrete misleading/dead-control defect in T3, not an authorization bypass.

**Recommendation:** Do not copy the inconsistent gate: apply response availability to all decisions and explain expired requests.

**Limits:** ['Source-established UI defect; no interactive reproduction performed.', 'Memex connected-state gating is not identical to T3 native responseCapability; no universal superiority claim.']

- [T3 apps/web/src/components/chat/ChatComposer.tsx:6651–6658](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L6651-L6658): Reachable component receives live-only canRespond.
- [T3 apps/web/src/components/chat/ComposerPendingApprovalActions.tsx:49–85](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.tsx#L49-L85): Primary respects canRespond; menu does not.
- [T3 apps/web/src/components/ChatView.tsx:9750–9757](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9750-L9757): Non-live callbacks silently return.
- [T3 apps/web/src/components/chat/ComposerPendingApprovalActions.test.tsx:7–63](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ComposerPendingApprovalActions.test.tsx#L7-L63): Tests all use canRespond=true; inspected suite does not cover expired overflow actions.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:52–67](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L52-L67): Shared pending-item canRespond is supplied.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:757–760](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L757-L760): Current request and option guards.
- [Memex runtime packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:895–909](/Users/nico/Code/memex-chat-runtime/packages/sq-ui/Sources/SQACPUI/AcpComposerView.swift:895): Shared approval control disables options using approval.canRespond.

### T3-I10 — Prompt stash and recall

**present; none; source-confirmed; existing tests inspected where cited, not executed.** Save a prompt for later and recover the unsent draft after navigating prompt history.

**T3:** T3 provides prompt history navigation and a 20-entry stash with rich records/files/images. Capacity evicts the oldest with a warning; restore consumes an entry.

**Memex:** Memex provides previous/next/recent prompt recall, a 20-entry stash that refuses overflow and copies on restore; captured attachment bytes are saved atomically before clearing unchanged input.

Parity exists and Memex's explicit copy/delete policy avoids destructive stash eviction.

**Recommendation:** Keep the non-destructive Memex stash policy rather than adopting every T3 interaction literally.

**Limits:** []

- [T3 apps/web/src/components/chat/ChatComposer.tsx:4299–4367](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4299-L4367): History handling is wired to composer.
- [T3 apps/web/src/components/chat/ChatComposer.tsx:4961–4973](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4961-L4973): Oldest entry evicted on overflow.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:191–269](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L191-L269): Reachable stash/recall and unchanged-input clearing.
- [Memex apps/macos/Sources/Memex/ConversationPromptLibrary.swift:15–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptLibrary.swift#L15-L74): Copy-on-restore policy, full rejection and atomic captured-payload save.
- [Memex apps/macos/Tests/MemexTests/ConversationComposerTests.swift:12–61](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationComposerTests.swift#L12-L61): Roundtrip, full/unreadable store and original draft recall.

### T3-I11 — T3 stash image loss window

**defect; P2; source-confirmed; existing tests inspected where cited, not executed.** Stash an image-bearing prompt, then close/reload the tab before asynchronous image compression finishes.

**T3:** T3 writes a text-only entry with pendingImageCount, clears the composer and releases image uploads, then asynchronously encodes and persists images. Hydration explicitly reports those pending images lost if reload interrupts the encode. Storage quota or encoding limits can also omit images; warnings disclose those outcomes.

**Memex:** Memex saves text plus captured attachment content atomically before clearing the unchanged draft and refuses a failed save.

T3 has a source-established data-loss window for unsent image context. Its warning/recovery design preserves text but cannot recover those image bytes.

**Recommendation:** Do not copy the two-phase destructive image stash. Retain the original draft or durably stage image bytes before clearing.

**Limits:** ['This is a bounded crash/reload defect, not a claim that normal completed stash operations lose images.', 'T3 intentionally compresses stash images and warns about lossy/omitted content; preserve that distinction from accidental silent loss.']

- [T3 apps/web/src/components/chat/ChatComposer.tsx:4898–4956](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4898-L4956): Text-only stash followed by composer clear and upload release.
- [T3 apps/web/src/components/chat/ChatComposer.tsx:4984–5025](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L4984-L5025): Image encoding occurs after clear and may be non-durable.
- [T3 apps/web/src/promptStashStore.ts:75–101](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/promptStashStore.ts#L75-L101): Reload explicitly converts interrupted encodes to missing-image notices.
- [Memex apps/macos/Sources/Memex/ConversationPromptLibrary.swift:40–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPromptLibrary.swift#L40-L74): Whole captured entry saved atomically.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:233–241](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L233-L241): Clears only after awaited successful save and only if input is unchanged.

### T3-I12 — Provider catalog, model/effort and permission configuration

**partial; P2; source-confirmed; existing tests inspected where cited, not executed.** Choose an executable provider and its reported model, effort and permission options.

**T3:** T3 ships eight driver types: Codex, Claude, Cursor, Grok, OpenCode, Antigravity, Pi and ACP Registry. Model picker is instance-aware; traits and runtime modes are provider-filtered.

**Memex:** Memex has Codex and Claude native integrations plus explicitly configured ACP executables/homes/arguments. It discovers model and nonempty configuration options, preserves unknown inherited values, remembers explicit provider-scoped choices, and waits for acknowledgement. It does not ship the remaining dedicated T3 driver integrations merely because it ingests their history.

Provider breadth remains a real gap, but describing Memex as Codex/Claude-only is also incomplete.

**Recommendation:** Add dedicated adapters only for demonstrated workflow demand; retain negotiated ACP capability boundaries and explicit installation identity.

**Limits:** ['Driver presence is not proof that the required binary/account is configured or that every provider supports every interaction.', 'Existing chat model/effort changes are blocked while working in Memex; settings are applied when idle.']

- [T3 apps/server/src/provider/builtInDrivers.ts:52–62](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/provider/builtInDrivers.ts#L52-L62): Authoritative shipped driver list.
- [T3 apps/web/src/components/chat/ProviderModelPicker.tsx:75–96](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ProviderModelPicker.tsx#L75-L96): Account/instance-aware selected model state.
- [T3 apps/web/src/components/chat/ChatComposer.tsx:2131–2155](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ChatComposer.tsx#L2131-L2155): Provider-supported mode filtering and model state.
- [Memex apps/macos/Sources/Memex/ConversationProviderCatalog.swift:28–67](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProviderCatalog.swift#L28-L67): Configured ACP and two native built-ins.
- [Memex apps/macos/Sources/Memex/InAppAgentRuntime.swift:124–135](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L124-L135): Actual configured ACP/Codex/Claude connection paths.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:139–182](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L139-L182): Reported model/config selectors and inherited defaults.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:540–569](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L540-L569): Observed setting acknowledgement and timeout.
- [Memex apps/macos/Tests/MemexTests/ConversationProviderCatalogTests.swift:6–55](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationProviderCatalogTests.swift#L6-L55): Executable-vs-ingestion identity and resume capability tests.

### T3-I13 — Build/runtime availability boundary

**not-applicable; none; source-confirmed; existing tests inspected where cited, not executed.** Install a build and expect active chat functionality to exist.

**T3:** T3 server's shipped provider driver list is built into its normal provider architecture; external provider readiness still controls usability.

**Memex:** Runtime dependencies are included by default when MEMEX_AGENT_RUNTIME_ROOT or .local-runtime-root resolves to a runtime checkout. Package.swift imports SQACP/SQACPHost/SQACPUI from that local root and links its archive. MEMEX_HISTORY_ONLY=1 explicitly removes these dependencies; without a resolved root the conditional Home fallback is EmptyView.

This is an intentional packaging/provenance qualifier, not a demonstrated feature gap in the supplied runtime-enabled build. Installed-binary runtime provenance was not measured.

**Recommendation:** Make release/runtime provenance explicit in final synthesis; do not count dormant dependency APIs as shipped app capability.

**Limits:** ['No app was built, launched or provider prompted in this task; installed-binary behavior remains unverified.', 'The supplied Memex and runtime commits are separate baselines rather than an assertion that Package.swift pins the latter.']

- [T3 apps/server/src/provider/builtInDrivers.ts:1–18](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/provider/builtInDrivers.ts#L1-L18): Configured providers resolve through shipped drivers.
- [Memex apps/macos/Package.swift:4–38](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Package.swift#L4-L38): Machine-local optional runtime and archive linkage.
- [Memex apps/macos/Sources/Memex/ConversationComposer.swift:4–8](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationComposer.swift#L4-L8): Full composer requires SQACPUI.
- [Memex apps/macos/Sources/Memex/HomeConversationComposer.swift:226–231](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/HomeConversationComposer.swift#L226-L231): Home composer fallback is empty.

### T3-I14 — T3 ordinary send clears durable draft before acknowledgement and retries with new identity

**defect; P1; source-confirmed failure window and identity allocation; no crash or duplicate-provider execution induced.** Send an ordinary single-target prompt, then close/reload before dispatch completes, or retry after a failure whose original server acceptance is uncertain.

**T3:** The web path snapshots content into local function variables and an optimistic React state row, clears composer content, then awaits settings/upload preparation and startThreadTurn. Failure restores from that closure only when the current draft is empty. A fresh onSend allocates a new messageId; startThreadTurn allocates a fresh commandId because the call supplies no retained commandId. The ordinary path has no durable uncertain-intent record or same-identity retry guard.

**Memex:** Memex persists the outgoing command identity and captured attachments before provider dispatch; ambiguous errors become uncertain. Recovery requires reconnect/review, and restoring the pending prompt does not send it. Queue recovery also retains unconfirmed command identity.

A reload/close during a pre-dispatch await can discard the only recoverable prompt snapshot. After accepted-then-lost acknowledgement, a manually retried ordinary send has new identities, so server receipt deduplication cannot identify it as the original command. Duplicate execution is a supported risk, not an observed runtime result.

**Recommendation:** Do not copy this client recovery design. Persist the complete outbound intent and original command/message IDs before clearing, reconcile receipt/native state after reconnect, and distinguish rejected from ambiguous outcomes.

**Limits:** ['Bounded to the ordinary single-target web send branch. Multi-model fanout has a separate in-memory uncertain-submission map; it was not audited here for persistence.', "Server-side receipts remain useful for retries of the same command ID; the defect is the UI's loss of original outbound intent/identity.", 'No test claiming an induced reload or duplicate execution was run.']

- [T3 apps/web/src/components/ChatView.tsx:1816–1816](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L1816-L1816): Ordinary optimistic input is React useState.
- [T3 apps/web/src/components/ChatView.tsx:8911–8944](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L8911-L8944): Content remains in closure; new message ID allocated for each submission.
- [T3 apps/web/src/components/ChatView.tsx:9449–9451](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9449-L9451): Clears composer before later asynchronous work.
- [T3 apps/web/src/components/ChatView.tsx:9485–9507](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9485-L9507): Pre-dispatch awaits occur after durable composer clear.
- [T3 apps/web/src/components/ChatView.tsx:9549–9593](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9549-L9593): New ordinary send passes messageId but no retained commandId.
- [T3 apps/web/src/components/ChatView.tsx:9665–9723](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9665-L9723): Restores local snapshots conditionally and reports generic failure.
- [T3 packages/client-runtime/src/operations/commands.ts:261–270](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L261-L270): Missing caller ID creates a random UUID.
- [T3 packages/client-runtime/src/operations/commands.ts:626–639](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L626-L639): Allocates command identity before preparing attachment command payload.
- [T3 packages/client-runtime/src/operations/commands.ts:755–772](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/operations/commands.ts#L755-L772): Dispatch uses newly allocated command and message identity.
- [T3 apps/web/src/components/ChatView.tsx:9156–9168](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L9156-L9168): Separate multi-model branch has a guard; it is excluded from this finding.
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:779–818](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L779-L818): Persistent intent precedes provider boundary, ambiguous result requires reconnect.
- [Memex apps/macos/Sources/Memex/ConversationPendingView.swift:26–34](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationPendingView.swift#L26-L34): Restore draft warns review first and does not resend.
- [Memex apps/macos/Tests/MemexTests/LiveConversationTests.swift:159–208](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/LiveConversationTests.swift#L159-L208): Existing tests cover no replay and native identity confirmation.

### T3-TR-01 — Structured plans and implementation

**present; none; source-and-existing-tests.** Inspect task steps, refine a plan, implement in this or a new conversation.

**T3:** Typed todo_list and proposed-plan cards; direct plan follow-up/new-thread dispatch.

**Memex:** Latest recognized native plan/update_plan/TodoWrite steps; Refine/Implement attach context to unsent draft; new-conversation preparation is wired.

The old no-plan-workflow finding is obsolete. The products choose different dispatch semantics.

**Recommendation:** Preserve review-before-send semantics; consider richer proposed-plan export only if wanted.

**Limits:** Source inspection only; tests mentioned were read, not executed.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:2688–2705](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2688-L2705): Renders proposed plan card
- [T3 apps/web/src/components/ChatView.tsx:10091–10140](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10091-L10140): Starts implementation follow-up
- [Memex apps/macos/Sources/Memex/ConversationWork.swift:44–66](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L44-L66): Recognizes structured steps, never prose
- [Memex apps/macos/Sources/Memex/ConversationWork.swift:114–132](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L114-L132): Reachable refine/implement/new-conversation actions
- [Memex apps/macos/Sources/Memex/Reader.swift:40–46](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L40-L46): Wires work panel
- [Memex apps/macos/Tests/MemexTests/ConversationHistoryTests.swift:98–110](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationHistoryTests.swift#L98-L110): Plan branch retains unsent draft

### T3-TR-02 — Live subagent visibility and navigation

**partial; P2; source-and-existing-tests.** Follow children and open their work.

**T3:** Live projected agent status, grouped timing and child-thread navigation.

**Memex:** Structured child rows and known spawn/wait schemas provide roster/status; Open resolves only an already indexed same-provider/same-machine session. Runtime child read API is not wired by this panel.

Memex has live roster presentation but provider child capabilities exceed the reachable native control surface.

**Recommendation:** Expose verified runtime child history/navigation when indexing has not caught up; keep unknown status explicit.

**Limits:** Source inspection only; tests mentioned were read, not executed.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:3037–3110](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L3037-L3110): Combines current projection statuses and timing
- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:5303–5326](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L5303-L5326): Opens child thread
- [Memex apps/macos/Sources/Memex/ConversationWork.swift:30–93](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L30-L93): Collects exact IDs/status; unknown remains unknown
- [Memex apps/macos/Sources/Memex/ConversationWork.swift:133–152](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L133-L152): Child Open gated on indexed sessions
- [Memex runtime packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1284–1296](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1284): Runtime API exists
- [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:105–114](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L105-L114): Does not confuse tool completion with child completion
- [Memex runtime packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:1030–1050](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:1030): Publishes native child status metadata to root conversation

### T3-TR-03 — Forks, lineage and context return

**present; none; source-and-existing-tests.** Branch a conversation and bring findings back.

**T3:** Response fork and relationship panel with merge-back.

**Memex:** Native history fork/rewind for capable connected idle provider, context branch across providers, parent/child navigation, captured context to parent draft, durable pending-operation inspection.

Old no-fork/lineage finding is obsolete. Context return is not a Git merge in either product.

**Recommendation:** Retain explicit native-history versus captured-context distinction.

**Limits:** Provider capability and idle-state gates apply; Memex context capture is capped at 4 MB and errors rather than silently truncating.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:2575–2586](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2575-L2586): Response fork control
- [T3 apps/web/src/components/chat/ThreadRelationshipsControl.tsx:282–300](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ThreadRelationshipsControl.tsx#L282-L300): Dispatches merge-back and opens parent
- [Memex apps/macos/Sources/Memex/ConversationHistoryActions.swift:19–44](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationHistoryActions.swift#L19-L44): Reachable native/context fork and parent return
- [Memex apps/macos/Sources/Memex/StoreConversationHistory.swift:42–81](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreConversationHistory.swift#L42-L81): Captures context, preserves draft and relationship
- [Memex apps/macos/Sources/Memex/StoreConversationHistory.swift:110–126](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreConversationHistory.swift#L110-L126): Adds captured context to parent draft
- [Memex apps/macos/Tests/MemexTests/ConversationHistoryTests.swift:50–66](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationHistoryTests.swift#L50-L66): Durable operation marker

### T3-TR-04 — Typed transcript fidelity

**present; none; source-and-existing-tests.** Read commentary, completed work, lifecycle and tool media after resume.

**T3:** Typed work, reasoning, lifecycle/error/compaction/todo rows and rich attachments.

**Memex:** Exact evidence metadata now restores phase/turn/lifecycle; opaque image tool content and full-output artifacts retained; unknown evidence stays raw.

The earlier completed-work and nested-Claude-image defects are addressed at the inspected baseline.

**Recommendation:** Avoid repeating the old defects; preserve these composition tests.

**Limits:** Not equal rendering of every provider field or media type; artifact references do not prove the underlying file still exists.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:2730–2802](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2730-L2802): Explicit error/interrupt/handoff/fork/compaction/todo semantics
- [Memex apps/macos/Sources/Memex/ConversationProjection.swift:104–125](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L104-L125): Exact retained evidence join
- [Memex apps/macos/Sources/Memex/ConversationProjection.swift:177–201](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L177-L201): Child/lifecycle projection
- [Memex apps/macos/Sources/Memex/ConversationProjection.swift:286–310](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L286-L310): Retains media/full artifact
- [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:6–57](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L6-L57): Tests phase folding and resumed Claude media/full output

### T3-TR-05 — Historical search scope

**present; none; source-only.** Find prior discussions across providers and machines.

**T3:** Literal SQL LIKE over finished user/assistant messages in active V2 threads/projects, one best result per thread; archived/tool/system/unimported V1 content excluded.

**Memex:** Native UI uses indexed lexical search with provider/project/time/origin/machine filters and record anchors.

Memex remains a history-retrieval product; T3 search is active-thread discovery. Archiving a T3 thread removes its message content from this search.

**Recommendation:** Do not copy T3 search exclusions into Memex; keep retrieval scope clear.

**Limits:** Native macOS search is lexical, not the MCP hybrid/semantic surface. The T3 search limitation is an implemented boundary, not an alleged data-loss defect.

- [T3 apps/server/src/orchestration-v2/ThreadSearch.ts:64–68](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L64-L68): Defines active V2 corpus
- [T3 apps/server/src/orchestration-v2/ThreadSearch.ts:88–143](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L88-L143): Explicit exclusions and one-best-match ranking
- [Memex apps/macos/Sources/Memex/Client.swift:126–143](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Client.swift#L126-L143): Filtered native lexical search
- [Memex apps/macos/Sources/Memex/Client.swift:235–249](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Client.swift#L235-L249): Exact source-record anchor lookup

### T3-TR-06 — Exact in-conversation Find and raw evidence

**present; none; source-and-existing-tests.** Find each literal occurrence, including hidden raw source, across a long chat.

**T3:** Reviewed MessagesTimeline/ThreadDetailsPanel and shared state expose rich normalized items; no all-history exact-occurrence Find or raw-provider-transcript toggle located.

**Memex:** Paged literal case-insensitive Find with previous/next and exact raw fallback; live mode searches projected records.

Memex retains an evidence-navigation advantage; browser Find cannot search rows absent from the virtual DOM.

**Recommendation:** Preserve exact Find and raw access when adding richer rendering.

**Limits:** T3 absence claim is bounded to reviewed web transcript/shared runtime, not every external browser extension; live Memex raw evidence is the projected record, not a full provider dump.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1342–1385](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1385): Virtualized bounded rendered rows, not full-history browser Find
- [T3 apps/server/src/orchestration-v2/ThreadSearch.ts:88–143](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L88-L143): Separate cross-thread search is one match per thread
- [Memex apps/macos/Sources/Memex/ConversationFind.swift:12–25](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationFind.swift#L12-L25): Literal occurrence ranges
- [Memex apps/macos/Sources/Memex/ConversationFind.swift:83–136](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationFind.swift#L83-L136): Reads successive pages; preserves live selection
- [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:92–102](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L92-L102): Raw hidden match reveals exact source

### T3-TR-07 — Conversation outline and reading position

**present; none; source-and-existing-tests.** Jump to earlier prompts and return to a reading position.

**T3:** Timeline minimap, citation targeting, remembered position and anchor preservation.

**Memex:** Wired prompt-outline popover, previous/next selection and exact record reveal, reading/disclosure restoration.

Earlier dormant-outline finding is obsolete.

**Recommendation:** Treat layout differences as design choices, not missing navigation.

**Limits:** Source inspection only; tests mentioned were read, not executed.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1342–1385](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1385): Anchoring plus minimap
- [Memex apps/macos/Sources/Memex/Reader.swift:67–73](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L67-L73): Loads historical/live outline and follows visible record
- [Memex apps/macos/Sources/Memex/Reader.swift:197–223](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L197-L223): Previous/next and prompt selection
- [Memex apps/macos/Sources/Memex/Reader.swift:268–269](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L268-L269): Reachable outline control
- [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:80–89](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L80-L89): Retains selection

### T3-TR-08 — Rename, pin, archive, removal and bulk ordering

**present; none; source-and-existing-tests.** Organize chats without altering native provider history.

**T3:** Thread action menu supports rename/pin/archive/delete, with archive distinct from deletion.

**Memex:** Reachable rename/pin/archive/restore/remove and multi-selection; locked atomic local metadata persists and preserves provider history. Removal is reversible local hiding, not destructive deletion.

Old basic-lifecycle absence is obsolete. Memex deliberately protects ingested provider records.

**Recommendation:** Preserve reversible removal semantics.

**Limits:** Source inspection only; tests mentioned were read, not executed.

- [T3 apps/web/src/components/threadActionMenu.logic.ts:121–125](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L121-L125): Pin/unpin
- [T3 apps/web/src/components/threadActionMenu.logic.ts:219–235](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L219-L235): Archive versus permanent deletion
- [Memex apps/macos/Sources/Memex/ConversationLibrary.swift:83–108](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationLibrary.swift#L83-L108): Reversible metadata actions
- [Memex apps/macos/Sources/Memex/SidebarConversations.swift:211–232](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/SidebarConversations.swift#L211-L232): Reachable pin/archive/restore/remove
- [Memex apps/macos/Tests/MemexTests/ConversationLibraryTests.swift:11–39](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationLibraryTests.swift#L11-L39): Native source untouched
- [Memex apps/macos/Tests/MemexTests/ConversationLibraryTests.swift:64–106](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationLibraryTests.swift#L64-L106): Concurrent metadata and filtered order

### T3-TR-09 — Completion and input notifications

**present; none; source-and-existing-tests.** Get alerts when background work completes or needs input.

**T3:** Opt-in per-client sound/desktop/in-app notification path with browser permission gate.

**Memex:** Native opt-in completion/approval/question notifications, sound-only mode, per-conversation visibility suppression, click opens conversation; packaged-app permission gate.

Notification delivery is now implemented in Memex, not merely sidebar indicators.

**Recommendation:** Do not carry the old missing-notifications claim forward.

**Limits:** OS/browser permissions, running client and user opt-in constrain delivery. No actual notification was emitted during research.

- [T3 apps/web/src/components/ThreadNotificationCoordinator.tsx:124–179](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ThreadNotificationCoordinator.tsx#L124-L179): Transition-driven alerts and sound
- [T3 apps/web/src/components/ThreadNotificationCoordinator.tsx:209–231](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ThreadNotificationCoordinator.tsx#L209-L231): Permission-gated delivery
- [Memex apps/macos/Sources/Memex/ConversationNotifications.swift:87–103](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L87-L103): Packaged app and OS authorization
- [Memex apps/macos/Sources/Memex/ConversationNotifications.swift:111–145](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L111-L145): Identity dedupe and visibility suppression
- [Memex apps/macos/Sources/Memex/MemexApp.swift:82–90](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L82-L90): Activates and navigates notifications
- [Memex apps/macos/Tests/MemexTests/ConversationNotificationTests.swift:14–71](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationNotificationTests.swift#L14-L71): Avoids historical replay and dedupes pending requests

### T3-TR-11 — Sidebar snooze

**intentionally-excluded; none; source-only.** Temporarily hide a chat until later.

**T3:** T3 supports snooze in thread lifecycle actions.

**Memex:** Not considered for implementation by explicit research contract.

This is an explicit exclusion, not a backlog recommendation.

**Recommendation:** Do not propose sidebar snooze.

**Limits:** Source inspection only; tests mentioned were read, not executed.

- [T3 apps/web/src/components/threadActionMenu.logic.ts:128–140](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L128-L140): Snooze is distinct from pin and settle

### T3-TR-10 — Customizable keyboard bindings

**partial; P2; source-only.** Change shortcuts and detect conflicts.

**T3:** Editable keybindings with recording/conflict handling; browser warns when native browser shortcuts intercept.

**Memex:** Built-in native commands and composer shortcuts; no equivalent native customization surface located in command/settings inventory.

Keyboard customization is a real feature difference, independent of rendering performance.

**Recommendation:** Consider customization if fixed shortcuts conflict with user workflows.

**Limits:** Source inspection only; tests mentioned were read, not executed.

- [T3 apps/web/src/components/settings/KeybindingsSettings.tsx:751–778](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/KeybindingsSettings.tsx#L751-L778): Conflict checks and recording
- [T3 apps/web/src/components/settings/KeybindingsSettings.tsx:1314–1322](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/KeybindingsSettings.tsx#L1314-L1322): Browser may intercept shortcuts
- [Memex apps/macos/Sources/Memex/MemexApp.swift:12–33](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L12-L33): Statically declared native keyboard shortcuts

### T3-TR-12 — Long-history publish architecture

**unknown; none; source-only.** Read long live conversations efficiently.

**T3:** LegendList virtualization and cursor-based progressive history loads.

**Memex:** NSTableView reuse and bounded visible window; native publish reads/reprojects complete runtime conversation before slicing.

Architecture differs, but source cannot determine latency, CPU or memory cost at actual transcript sizes.

**Recommendation:** Benchmark representative long conversations before deciding whether this path needs optimization.

**Limits:** Validation need, not established performance defect; no benchmark or runtime measurement was run.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1342–1372](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1372): Virtualized rows
- [T3 packages/client-runtime/src/state/threads.ts:636–691](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/state/threads.ts#L636-L691): Cursor-based progressive history request
- [Memex apps/macos/Sources/Memex/InAppAgentRuntime.swift:191–218](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L191-L218): Reads/projects full conversation
- [Memex apps/macos/Sources/Memex/LiveConversation.swift:238–245](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L238-L245): Window after snapshot
- [Memex apps/macos/Sources/Memex/TranscriptController.swift:633–640](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptController.swift#L633-L640): Reusable native rows

### T3-TR-13 — Accessibility hooks

**present; none; source-only.** Navigate transcript controls using assistive technology.

**T3:** ARIA labels/expanded state and reduced-motion-aware following.

**Memex:** Native copy/disclosure accessibility labels and expanded state.

Both implement observable accessibility hooks; source does not establish comprehensive accessibility.

**Recommendation:** Keep these hooks when changing presentation; audit with assistive technology for claims beyond source presence.

**Limits:** No screen-reader, keyboard-only usability or compliance audit performed.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1356–1372](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1356-L1372): Reduced-motion scrolling
- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:3070–3075](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L3070-L3075): ARIA label/description
- [Memex apps/macos/Sources/Memex/TranscriptController.swift:874–921](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptController.swift#L874-L921): Copy/disclosure labels and values

### T3-W01 — Managed worktrees and lifecycle

**present; none; source-only.** Create an isolated checkout, run setup, retain or remove it.

**T3:** Reachable worktree setup UI and authenticated current-thread handoff service.

**Memex:** Existing/new-worktree preparation and archive, reattach, safe clean removal UI.

Do not count worktrees as a missing Memex capability.

**Recommendation:** Preserve Memex isolation and retained-chat cleanup guards; compare handoff UX separately.

**Limits:** Source inspection only; no runtime or test execution.

- [T3 apps/web/src/components/chat/WorktreeSetupCard.tsx:284–297](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/WorktreeSetupCard.tsx#L284-L297): Setup script and settled outcome presentation.
- [T3 apps/server/src/mcp/WorktreeMcpService.ts:248–269](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/WorktreeMcpService.ts#L248-L269): Creates the worktree under cancellation/rollback protection.
- [Memex apps/macos/Sources/Memex/ConversationWorkspace.swift:22–28](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWorkspace.swift#L22-L28): Delegates existing/new checkout preparation.
- [Memex apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift:102–110](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift#L102-L110): Existing workspace branch of preparation.
- [Memex apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift:155–170](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ManagedWorkspaceStore.swift#L155-L170): Creates managed branch and worktree.
- [Memex apps/macos/Sources/Memex/WorkspaceLifecycleView.swift:23–60](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceLifecycleView.swift#L23-L60): Archive, reattach and guarded removal.

### T3-W02 — Git controls and inline pull-request workspace

**partial; P2; source-only.** Review changes, commit, push and inspect the PR.

**T3:** Git workflow controls plus an environment-scoped PullRequestDetailPanel inside the chat.

**Memex:** Staging, commit, push and GitHub draft PR creation; draft PR opens externally. Local branch diff and review context feed chat.

Memex already handles local Git, but its inspected native workflow ends at an external PR URL.

**Recommendation:** If PR review parity is desired, add a first-class PR detail/review surface rather than duplicating local Git controls.

**Limits:** Source inspection only; no runtime or test execution.

- [T3 apps/web/src/components/ChatView.tsx:10621–10665](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10621-L10665): Diff and PR detail surfaces are mounted from selected panel.
- [T3 apps/web/src/components/GitActionsControl.tsx:1730–1748](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/GitActionsControl.tsx#L1730-L1748): Git initialization and quick action UI.
- [Memex apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:19–28](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift#L19-L28): Complete native Git menu inventory.
- [Memex apps/macos/Sources/Memex/WorkspaceGitActionsView.swift:84–100](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceGitActionsView.swift#L84-L100): Draft PR success opens its URL externally.
- [Memex apps/macos/Sources/Memex/WorkspaceChangesView.swift:63–80](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceChangesView.swift#L63-L80): Current and branch comparison plus Git actions.
- [Memex apps/macos/Sources/Memex/WorkspaceChangesView.swift:149–154](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceChangesView.swift#L149-L154): Adds review context to chat.

### T3-W03 — Checkpoint and conversation rewind

**present; none; source-only.** Restore files and rewind provider context to an earlier turn.

**T3:** Timeline revert invokes provider rollback then optional checkpoint restore; provider capability errors are explicit.

**Memex:** Workspace pane wires checkpoint controls and conversation rewind; restore protects managed ownership and captures recovery.

Both have rewind, with provider and checkout constraints.

**Recommendation:** Retain separate file/context intent and provider capability gating; do not infer all providers support rollback.

**Limits:** Source inspection only; no runtime or test execution.

- [T3 apps/web/src/components/chat/MessagesTimeline.tsx:2285–2287](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2285-L2287): Timeline revert entry point.
- [T3 apps/server/src/orchestration-v2/CheckpointRollbackService.ts:257–265](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/CheckpointRollbackService.ts#L257-L265): Provider rollback followed by optional file restore.
- [Memex apps/macos/Sources/Memex/WorkspacePanelView.swift:18–25](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L18-L25): Rewind handler supplied to workspace changes.
- [Memex apps/macos/Sources/Memex/WorkspaceCheckpoints.swift:69–101](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceCheckpoints.swift#L69-L101): Isolation validation, recovery snapshot and index restore.

### T3-W04 — T3 checkpoint staging-state loss

**defect; P1; source-only.** Restore a checkpoint captured while a file had different staged and unstaged contents.

**T3:** Capture stores one final workspace tree; restore writes that tree to both worktree and staging index.

**Memex:** Captures a separate index tree and restores it after raw worktree restoration.

T3 cannot reconstruct the original staged/unstaged split from this checkpoint shape; copying it regresses Memex.

**Recommendation:** Keep separate index/worktree snapshots. Add an upstream regression test if pursuing a T3 fix.

**Limits:** Deterministic source-level defect, not reproduced by executing Git or tests in this inventory.

- [T3 apps/server/src/vcs/GitVcsDriver.ts:1030–1069](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/vcs/GitVcsDriver.ts#L1030-L1069): Single staged temporary-index tree becomes checkpoint commit.
- [T3 apps/server/src/vcs/GitVcsDriver.ts:1091–1102](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/vcs/GitVcsDriver.ts#L1091-L1102): git restore passes both --worktree and --staged with the same source.
- [Memex apps/macos/Sources/Memex/WorkspaceCheckpoints.swift:98–101](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceCheckpoints.swift#L98-L101): Restores checkpoint.indexTree separately.
- [Memex apps/macos/Tests/MemexTests/WorkspaceGitWorkflowTests.swift:47–76](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/WorkspaceGitWorkflowTests.swift#L47-L76): Existing test distinguishes staged contents and verifies recovery snapshot.

### T3-W05 — File browsing, editing and previews

**present; none; source-only.** Browse a workspace file, edit it, save, or attach context.

**T3:** Chat file preview mounts editable CodeMirror-like source surface and rendered markdown/media/table previews; save uses environment writeFile.

**Memex:** File/folder creation, text edits, save, disk conflict UI, previews and add-to-chat are reachable in workspace Files.

Memex files/editor is implemented; visual editor depth is a separate UX question.

**Recommendation:** Retain existing capability and evaluate syntax/navigation refinements only with specific user needs.

**Limits:** Source inspection only; no runtime or test execution.

- [T3 apps/web/src/components/files/FilePreviewPanel.tsx:567–590](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/FilePreviewPanel.tsx#L567-L590): Editable file component.
- [T3 apps/web/src/components/files/useFileSaveCoordinator.ts:19–40](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/useFileSaveCoordinator.ts#L19-L40): Editor persistence dispatches environment writeFile.
- [Memex apps/macos/Sources/Memex/WorkspacePanelView.swift:38–45](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L38-L45): Files view reachable for local workspace.
- [Memex apps/macos/Sources/Memex/WorkspaceFilesView.swift:31–87](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFilesView.swift#L31-L87): Create, open, add context, save and conflict resolution controls.

### T3-W06 — T3 editor concurrent-write overwrite

**defect; P1; source-only.** An agent or external editor changes disk after the app loads a file but before its save.

**T3:** Save sends only cwd, path and new contents; server unconditionally writes contents with no expected revision.

**Memex:** Save passes loaded fingerprint and throws a conflict if the disk version differs; draft remains available.

T3 autosave can silently overwrite concurrent agent edits; this is a concrete design to avoid.

**Recommendation:** Preserve Memex optimistic concurrency and explicit disk-versus-draft reconciliation.

**Limits:** Source-level race scenario; no runtime reproduction performed.

- [T3 apps/web/src/components/files/useFileSaveCoordinator.ts:31–40](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/useFileSaveCoordinator.ts#L31-L40): No expected content revision in write request.
- [T3 apps/server/src/workspace/WorkspaceFileSystem.ts:305–340](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/workspace/WorkspaceFileSystem.ts#L305-L340): Path resolution followed by unconditional file write.
- [Memex apps/macos/Sources/Memex/WorkspaceFilesView.swift:224–240](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFilesView.swift#L224-L240): Passes fingerprint and loads disk version after conflict.
- [Memex apps/macos/Sources/Memex/WorkspaceFiles.swift:58–78](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceFiles.swift#L58-L78): Checks expectedFingerprint before write.

### T3-W07 — Persistent embedded terminal

**present; none; source-only.** Run shells, keep state across pane switches, split terminals and attach output.

**T3:** Persistent panel/drawer terminals with split and context callbacks; server PTY manager owns execution.

**Memex:** Native Ghostty terminals keyed to workspace, multiple shells, right pane/drawer, context capture; existing tests check persistence and independent shells.

Terminal itself is not a parity gap. Remote terminals are a distinct boundary.

**Recommendation:** Reuse the native terminal; do not replace it solely for comparator parity.

**Limits:** Source inspection only; no runtime or test execution.

- [T3 apps/web/src/components/ChatView.tsx:10602–10619](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10602-L10619): Persistent terminal, split directions, new/close and context callbacks.
- [Memex apps/macos/Sources/Memex/WorkspacePanelView.swift:56–57](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspacePanelView.swift#L56-L57): Native terminal pane entry.
- [Memex apps/macos/Sources/Memex/WorkspaceTerminalView.swift:40–52](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalView.swift#L40-L52): Retained workspace terminal group selection.
- [Memex apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift:45–49](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift#L45-L49): Persistence test exists.
- [Memex apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift:127–131](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/WorkspaceTerminalTests.swift#L127-L131): Independent-shell test exists.

### T3-W08 — Browser automation and app-owned preview

**partial; P2; source-only.** Open a page and let an agent inspect/interact with it.

**T3:** Preview panel plus MCP open/navigate/resize/snapshot/click/type/press/scroll/evaluate/wait/recording tools.

**Memex:** App-owned WKWebView tabs; explicit per-conversation capability grant; snapshot/click/type/scroll/evaluate dispatch through execution host and desktop bridge. The opt-in memex-control stdio MCP exposes this path via its generic control tool.

Both products expose agent-callable browser control. The narrower Memex action inventory, not absence of a tool bridge, is the gap.

**Recommendation:** Consider first-class navigation/wait/keyboard/recording actions and clearer discovery; preserve opt-in registration and app-owned tab grants.

**Limits:** Does not establish general OS computer-use parity. T3 device tools are separate device surfaces, not evidence of arbitrary macOS app control.

- [T3 apps/web/src/components/ChatView.tsx:10588–10600](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10588-L10600): Browser preview mounts in chat.
- [T3 apps/server/src/mcp/toolkits/preview/tools.ts:67–91](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/toolkits/preview/tools.ts#L67-L91): Agent navigation controls.
- [T3 apps/server/src/mcp/toolkits/preview/tools.ts:201–244](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/toolkits/preview/tools.ts#L201-L244): Evaluate, wait and recording tools.
- [Memex apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:68–100](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift#L68-L100): Grant and tab/action validation.
- [Memex apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:5–6](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift#L5-L6): Five supported action cases.
- [Memex apps/macos/Sources/Memex/MemexApp.swift:78–81](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L78-L81): Starts browser execution bridge.
- [Memex src/control_mcp.rs:44–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/control_mcp.rs#L44-L74): Registered agent-callable control MCP forwards method and params to the execution host; includes conversation, worktree, schedules and browser operations.
- [Memex src/bin/memex-control.rs:5–26](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/bin/memex-control.rs#L5-L26): Separate opt-in binary invokes control_mcp::run with optional root.
- [Memex docs/execution-host.md:92–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/docs/execution-host.md#L92-L96): Documents opt-in same-user authority and scoped browser describe/dispatch bridge.

### T3-W09 — Remote execution versus remote workspace surfaces

**partial; P1; source-only.** Pair another machine, resume its agent and use its files/Git/terminal.

**T3:** Environment-scoped clients and SSH/connect/direct routes feed the same workspace panels.

**Memex:** Paired execution hosts create/resume durable remote conversations, but selectedWorkspace explicitly excludes host-owned conversations; Git/files/terminal panes require local workspace.

The high-value gap is remote workspace tooling, not remote conversation execution.

**Recommendation:** Route read/review/terminal operations through authenticated execution-host APIs; preserve host-local path identity.

**Limits:** No claim that T3 or Memex provides arbitrary managed cloud compute. Deployment availability was not tested.

- [T3 apps/web/src/components/settings/EnvironmentRow.tsx:20–44](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/EnvironmentRow.tsx#L20-L44): SSH, WSL, T3 Connect and direct remote routes.
- [T3 apps/web/src/components/settings/EnvironmentRoutesList.tsx:39–68](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/EnvironmentRoutesList.tsx#L39-L68): Prioritized connection routes and active route.
- [T3 apps/web/src/components/files/useFileSaveCoordinator.ts:19–38](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/files/useFileSaveCoordinator.ts#L19-L38): File operations route by environmentId.
- [Memex apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:42–77](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L42-L77): Pair host with URL/token.
- [Memex apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:80–111](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L80-L111): Open and create host conversations.
- [Memex apps/macos/Sources/Memex/Store.swift:216–224](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Store.swift#L216-L224): Explicit local-only workspace boundary.
- [Memex apps/macos/Sources/Memex/WorkspaceTerminalView.swift:20–45](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceTerminalView.swift#L20-L45): Terminal unavailable without local working folder.

### T3-W10 — Time-based and event-triggered recurring work

**partial; P2; source-only.** Run prompts on a schedule or webhook, targeting a chat or creating a fresh task.

**T3:** Reachable settings supports scheduled/webhook tasks; dispatch either launches a new thread or queues an existing one.

**Memex:** Execution-host UI supports interval and weekday/local-time/timezone schedules for an existing hosted conversation, plus pause/run/delete.

Do not classify automation as absent. Event triggers and fresh-chat-per-run are the inspected gaps.

**Recommendation:** Extend the durable host scheduler only if event-driven/fresh-task workflows are required; keep stopped queue semantics.

**Limits:** Source inspection only; no runtime or test execution.

- [T3 apps/web/src/routes/settings.scheduled-tasks.tsx:1–8](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/routes/settings.scheduled-tasks.tsx#L1-L8): Mounted scheduled task settings.
- [T3 apps/server/src/scheduledTasks/ScheduledTaskService.ts:238–252](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/scheduledTasks/ScheduledTaskService.ts#L238-L252): Webhook rotation and verification/dispatch API.
- [T3 apps/server/src/scheduledTasks/ScheduledTaskService.ts:795–830](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/scheduledTasks/ScheduledTaskService.ts#L795-L830): New-thread or existing-thread queued prompt path.
- [Memex apps/macos/Sources/Memex/MemexApp.swift:35–35](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L35-L35): Execution Hosts and Schedules entry.
- [Memex apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift:114–174](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ExecutionHostConnectionsView.swift#L114-L174): Interval/wall-clock editor and actions.
- [Memex apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:328–376](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L328-L376): Existing conversation schedule model and actions.
- [Memex apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:383–397](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L383-L397): Recurrence and skipped wall-clock handling.

### T3-W11 — Agent-callable application toolkit breadth and registration

**partial; P2; source-only.** Ask the running agent to manage projects, worktrees, previews or app resources.

**T3:** Authenticated MCP /mcp mounts orchestrator/thread/project/environment/attachment/worktree/PR/device/HTML/preview toolkits into server.

**Memex:** Implemented opt-in memex-control stdio MCP registers a generic control(method, params) tool and forwards to the execution host. Supports conversation lifecycle/control/queue/fork/delegate, schedules, workspace listing, worktree lifecycle and granted browser operations. Ordinary memex mcp stays retrieval-only.

Memex has a real agent-callable app-control server. The remaining differences are registration/discovery ergonomics and the narrower explicit operation inventory, including no matching PR/device/HTML app tools in the inspected host dispatcher.

**Recommendation:** Credit existing control MCP. Reuse its host owners and explicit authority boundary if adding PR/device/HTML operations or friendlier schema discovery; manual opt-in registration is not a missing API.

**Limits:** Source-only acceptance correction: Rust MCP entry point and forwarding path verified. Parent reports packaging/smoke evidence separately; this worker did not rerun it. Does not claim provider configuration auto-registers the opt-in server.

- [T3 apps/server/src/mcp/McpHttpServer.ts:748–766](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/McpHttpServer.ts#L748-L766): Authenticated transport and registered toolkits.
- [T3 apps/server/src/server.ts:681–685](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/server.ts#L681-L685): Server mounts the MCP layer.
- [Memex apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:104–142](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L104-L142): Declared host capabilities and method dispatch boundary.
- [Memex apps/macos/Sources/Memex/WorkspaceBrowserExecutionBridge.swift:24–43](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserExecutionBridge.swift#L24-L43): Browser-specific app bridge dispatch.
- [Memex src/control_mcp.rs:44–74](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/control_mcp.rs#L44-L74): Registered agent-callable control MCP forwards method and params to the execution host; includes conversation, worktree, schedules and browser operations.
- [Memex src/bin/memex-control.rs:5–26](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/bin/memex-control.rs#L5-L26): Separate opt-in binary invokes control_mcp::run with optional root.
- [Memex src/control_mcp.rs:78–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/src/control_mcp.rs#L78-L96): Advertises tool capability and serves the stdio MCP transport.
- [Memex docs/execution-host.md:92–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/docs/execution-host.md#L92-L96): Documents opt-in same-user authority and scoped browser describe/dispatch bridge.

### T3-W12 — Unproven universal capabilities

**unknown; none; source-only.** Install arbitrary plugins, launch managed cloud compute, or control arbitrary OS apps.

**T3:** Inspected server has built-in app MCP toolkits and device tools; these do not prove a universal plugin marketplace, managed compute product or general desktop CUA.

**Memex:** Inspected native app has runtime clients, execution hosts and scoped browser control; opt-in memex-control exposes a generic host control MCP tool. This is not evidence of a third-party plugin marketplace or arbitrary OS application control.

Do not present a universal absence or parity claim from provider-specific or adjacent-product code.

**Recommendation:** Classify managed cloud, third-party plugin lifecycle and arbitrary OS app control as unresolved product-scope questions; composer skill discovery is owned by the other worker.

**Limits:** Source inspection only; no runtime or test execution.

- [T3 apps/server/src/mcp/McpHttpServer.ts:754–766](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/McpHttpServer.ts#L754-L766): Bounded built-in app toolkit inventory.
- [T3 apps/server/src/mcp/toolkits/device/tools.ts:28–81](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/mcp/toolkits/device/tools.ts#L28-L81): Device list/open/screenshot/close is a separate surface.
- [Memex apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift:104–140](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/MemexExecutionHostCore/ExecutionHost.swift#L104-L140): Bounded host capabilities.
- [Memex apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift:85–95](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/WorkspaceBrowserAutomation.swift#L85-L95): Only registered app-owned HTTP(S) tabs.
- [Memex docs/execution-host.md:92–96](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/docs/execution-host.md#L92-L96): Documents opt-in same-user authority and scoped browser describe/dispatch bridge.
