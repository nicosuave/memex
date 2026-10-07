# Transcript, agent work, organization and navigation

Pinned source review; no tests, builds or apps executed. Current Memex closes the earlier plan/outline/organization/notification/fidelity gaps.

## T3-TR-01: Structured plans and implementation — present

T3: Typed todo_list and proposed-plan cards; direct plan follow-up/new-thread dispatch.

Memex: Latest recognized native plan/update_plan/TodoWrite steps; Refine/Implement attach context to unsent draft; new-conversation preparation is wired.

The old no-plan-workflow finding is obsolete. The products choose different dispatch semantics.

Source inspection only; tests mentioned were read, not executed.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:2688](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2688-L2705); [T3 apps/web/src/components/ChatView.tsx:10091](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ChatView.tsx#L10091-L10140); [Memex apps/macos/Sources/Memex/ConversationWork.swift:44](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L44-L66); [Memex apps/macos/Sources/Memex/ConversationWork.swift:114](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L114-L132); [Memex apps/macos/Sources/Memex/Reader.swift:40](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L40-L46); [Memex apps/macos/Tests/MemexTests/ConversationHistoryTests.swift:98](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationHistoryTests.swift#L98-L110)

## T3-TR-02: Live subagent visibility and navigation — partial

T3: Live projected agent status, grouped timing and child-thread navigation.

Memex: Structured child rows and known spawn/wait schemas provide roster/status; Open resolves only an already indexed same-provider/same-machine session. Runtime child read API is not wired by this panel.

Memex has live roster presentation but provider child capabilities exceed the reachable native control surface.

Source inspection only; tests mentioned were read, not executed.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:3037](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L3037-L3110); [T3 apps/web/src/components/chat/MessagesTimeline.tsx:5303](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L5303-L5326); [Memex apps/macos/Sources/Memex/ConversationWork.swift:30](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L30-L93); [Memex apps/macos/Sources/Memex/ConversationWork.swift:133](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationWork.swift#L133-L152); [Memex runtime packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1284](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/CodexAppServerTransport.swift:1284); [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:105](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L105-L114); [Memex runtime packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:1030](/Users/nico/Code/memex-chat-runtime/packages/sq-acp/Sources/SQACPHost/AgentConversationService.swift:1030)

## T3-TR-03: Forks, lineage and context return — present

T3: Response fork and relationship panel with merge-back.

Memex: Native history fork/rewind for capable connected idle provider, context branch across providers, parent/child navigation, captured context to parent draft, durable pending-operation inspection.

Old no-fork/lineage finding is obsolete. Context return is not a Git merge in either product.

Provider capability and idle-state gates apply; Memex context capture is capped at 4 MB and errors rather than silently truncating.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:2575](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2575-L2586); [T3 apps/web/src/components/chat/ThreadRelationshipsControl.tsx:282](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/ThreadRelationshipsControl.tsx#L282-L300); [Memex apps/macos/Sources/Memex/ConversationHistoryActions.swift:19](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationHistoryActions.swift#L19-L44); [Memex apps/macos/Sources/Memex/StoreConversationHistory.swift:42](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreConversationHistory.swift#L42-L81); [Memex apps/macos/Sources/Memex/StoreConversationHistory.swift:110](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/StoreConversationHistory.swift#L110-L126); [Memex apps/macos/Tests/MemexTests/ConversationHistoryTests.swift:50](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationHistoryTests.swift#L50-L66)

## T3-TR-04: Typed transcript fidelity — present

T3: Typed work, reasoning, lifecycle/error/compaction/todo rows and rich attachments.

Memex: Exact evidence metadata now restores phase/turn/lifecycle; opaque image tool content and full-output artifacts retained; unknown evidence stays raw.

The earlier completed-work and nested-Claude-image defects are addressed at the inspected baseline.

Not equal rendering of every provider field or media type; artifact references do not prove the underlying file still exists.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:2730](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L2730-L2802); [Memex apps/macos/Sources/Memex/ConversationProjection.swift:104](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L104-L125); [Memex apps/macos/Sources/Memex/ConversationProjection.swift:177](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L177-L201); [Memex apps/macos/Sources/Memex/ConversationProjection.swift:286](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationProjection.swift#L286-L310); [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:6](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L6-L57)

## T3-TR-05: Historical search scope — present

T3: Literal SQL LIKE over finished user/assistant messages in active V2 threads/projects, one best result per thread; archived/tool/system/unimported V1 content excluded.

Memex: Native UI uses indexed lexical search with provider/project/time/origin/machine filters and record anchors.

Memex remains a history-retrieval product; T3 search is active-thread discovery. Archiving a T3 thread removes its message content from this search.

Native macOS search is lexical, not the MCP hybrid/semantic surface. The T3 search limitation is an implemented boundary, not an alleged data-loss defect.

Evidence: [T3 apps/server/src/orchestration-v2/ThreadSearch.ts:64](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L64-L68); [T3 apps/server/src/orchestration-v2/ThreadSearch.ts:88](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L88-L143); [Memex apps/macos/Sources/Memex/Client.swift:126](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Client.swift#L126-L143); [Memex apps/macos/Sources/Memex/Client.swift:235](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Client.swift#L235-L249)

## T3-TR-06: Exact in-conversation Find and raw evidence — present

T3: Reviewed MessagesTimeline/ThreadDetailsPanel and shared state expose rich normalized items; no all-history exact-occurrence Find or raw-provider-transcript toggle located.

Memex: Paged literal case-insensitive Find with previous/next and exact raw fallback; live mode searches projected records.

Memex retains an evidence-navigation advantage; browser Find cannot search rows absent from the virtual DOM.

T3 absence claim is bounded to reviewed web transcript/shared runtime, not every external browser extension; live Memex raw evidence is the projected record, not a full provider dump.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1342](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1385); [T3 apps/server/src/orchestration-v2/ThreadSearch.ts:88](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/server/src/orchestration-v2/ThreadSearch.ts#L88-L143); [Memex apps/macos/Sources/Memex/ConversationFind.swift:12](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationFind.swift#L12-L25); [Memex apps/macos/Sources/Memex/ConversationFind.swift:83](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationFind.swift#L83-L136); [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:92](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L92-L102)

## T3-TR-07: Conversation outline and reading position — present

T3: Timeline minimap, citation targeting, remembered position and anchor preservation.

Memex: Wired prompt-outline popover, previous/next selection and exact record reveal, reading/disclosure restoration.

Earlier dormant-outline finding is obsolete.

Source inspection only; tests mentioned were read, not executed.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1342](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1385); [Memex apps/macos/Sources/Memex/Reader.swift:67](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L67-L73); [Memex apps/macos/Sources/Memex/Reader.swift:197](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L197-L223); [Memex apps/macos/Sources/Memex/Reader.swift:268](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/Reader.swift#L268-L269); [Memex apps/macos/Tests/MemexTests/ConversationFidelityTests.swift:80](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationFidelityTests.swift#L80-L89)

## T3-TR-08: Rename, pin, archive, removal and bulk ordering — present

T3: Thread action menu supports rename/pin/archive/delete, with archive distinct from deletion.

Memex: Reachable rename/pin/archive/restore/remove and multi-selection; locked atomic local metadata persists and preserves provider history. Removal is reversible local hiding, not destructive deletion.

Old basic-lifecycle absence is obsolete. Memex deliberately protects ingested provider records.

Source inspection only; tests mentioned were read, not executed.

Evidence: [T3 apps/web/src/components/threadActionMenu.logic.ts:121](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L121-L125); [T3 apps/web/src/components/threadActionMenu.logic.ts:219](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L219-L235); [Memex apps/macos/Sources/Memex/ConversationLibrary.swift:83](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationLibrary.swift#L83-L108); [Memex apps/macos/Sources/Memex/SidebarConversations.swift:211](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/SidebarConversations.swift#L211-L232); [Memex apps/macos/Tests/MemexTests/ConversationLibraryTests.swift:11](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationLibraryTests.swift#L11-L39); [Memex apps/macos/Tests/MemexTests/ConversationLibraryTests.swift:64](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationLibraryTests.swift#L64-L106)

## T3-TR-09: Completion and input notifications — present

T3: Opt-in per-client sound/desktop/in-app notification path with browser permission gate.

Memex: Native opt-in completion/approval/question notifications, sound-only mode, per-conversation visibility suppression, click opens conversation; packaged-app permission gate.

Notification delivery is now implemented in Memex, not merely sidebar indicators.

OS/browser permissions, running client and user opt-in constrain delivery. No actual notification was emitted during research.

Evidence: [T3 apps/web/src/components/ThreadNotificationCoordinator.tsx:124](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ThreadNotificationCoordinator.tsx#L124-L179); [T3 apps/web/src/components/ThreadNotificationCoordinator.tsx:209](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/ThreadNotificationCoordinator.tsx#L209-L231); [Memex apps/macos/Sources/Memex/ConversationNotifications.swift:87](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L87-L103); [Memex apps/macos/Sources/Memex/ConversationNotifications.swift:111](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/ConversationNotifications.swift#L111-L145); [Memex apps/macos/Sources/Memex/MemexApp.swift:82](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L82-L90); [Memex apps/macos/Tests/MemexTests/ConversationNotificationTests.swift:14](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Tests/MemexTests/ConversationNotificationTests.swift#L14-L71)

## T3-TR-11: Sidebar snooze — intentionally-excluded

T3: T3 supports snooze in thread lifecycle actions.

Memex: Not considered for implementation by explicit research contract.

This is an explicit exclusion, not a backlog recommendation.

Source inspection only; tests mentioned were read, not executed.

Evidence: [T3 apps/web/src/components/threadActionMenu.logic.ts:128](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/threadActionMenu.logic.ts#L128-L140)

## T3-TR-10: Customizable keyboard bindings — partial

T3: Editable keybindings with recording/conflict handling; browser warns when native browser shortcuts intercept.

Memex: Built-in native commands and composer shortcuts; no equivalent native customization surface located in command/settings inventory.

Keyboard customization is a real feature difference, independent of rendering performance.

Source inspection only; tests mentioned were read, not executed.

Evidence: [T3 apps/web/src/components/settings/KeybindingsSettings.tsx:751](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/KeybindingsSettings.tsx#L751-L778); [T3 apps/web/src/components/settings/KeybindingsSettings.tsx:1314](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/settings/KeybindingsSettings.tsx#L1314-L1322); [Memex apps/macos/Sources/Memex/MemexApp.swift:12](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/MemexApp.swift#L12-L33)

## T3-TR-12: Long-history publish architecture — unknown

T3: LegendList virtualization and cursor-based progressive history loads.

Memex: NSTableView reuse and bounded visible window; native publish reads/reprojects complete runtime conversation before slicing.

Architecture differs, but source cannot determine latency, CPU or memory cost at actual transcript sizes.

Validation need, not established performance defect; no benchmark or runtime measurement was run.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1342](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1342-L1372); [T3 packages/client-runtime/src/state/threads.ts:636](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/packages/client-runtime/src/state/threads.ts#L636-L691); [Memex apps/macos/Sources/Memex/InAppAgentRuntime.swift:191](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/InAppAgentRuntime.swift#L191-L218); [Memex apps/macos/Sources/Memex/LiveConversation.swift:238](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/LiveConversation.swift#L238-L245); [Memex apps/macos/Sources/Memex/TranscriptController.swift:633](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptController.swift#L633-L640)

## T3-TR-13: Accessibility hooks — present

T3: ARIA labels/expanded state and reduced-motion-aware following.

Memex: Native copy/disclosure accessibility labels and expanded state.

Both implement observable accessibility hooks; source does not establish comprehensive accessibility.

No screen-reader, keyboard-only usability or compliance audit performed.

Evidence: [T3 apps/web/src/components/chat/MessagesTimeline.tsx:1356](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L1356-L1372); [T3 apps/web/src/components/chat/MessagesTimeline.tsx:3070](https://github.com/pingdotgg/t3code/blob/ecfdda5fa804582066c1aa62afea419677c24883/apps/web/src/components/chat/MessagesTimeline.tsx#L3070-L3075); [Memex apps/macos/Sources/Memex/TranscriptController.swift:874](https://github.com/nicosuave/memex/blob/17e958778a7398db8238cc9c8415ca12649067e5/apps/macos/Sources/Memex/TranscriptController.swift#L874-L921)
