import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct ActivityReaderTests {
    @Test func summaryCountsOperationsRatherThanCallAndResultRecords() {
        let records = [entry("a", "Read", input: #"{"path":"a.swift"}"#),
                       result("a-result", "a", #"{"text":"contents"}"#),
                       entry("b", "Read", input: #"{"path":"b.swift"}"#),
                       entry("c", "Grep", input: #"{"pattern":"Reader"}"#)]
        let controller = reader(records)
        #expect(controller.rows.count == 1)
        #expect(controller.measurement(at: 0).title == "Read 2 files and searched once")
        controller.toggle(controller.rows[0].id)
        #expect(controller.rows.count == 4)
        #expect(controller.measurement(at: 1).title == "Read a.swift")
        #expect(controller.measurement(at: 1).symbolName == "doc.text")
        #expect(controller.rows[1].records.map(\.id) == ["a", "a-result"])
    }

    @Test func explicitFailureStaysVisibleAndFindStillRevealsItsOutput() throws {
        let records = [entry("a", "Read", input: #"{"path":"a.swift"}"#),
                       entry("b", "Bash", input: #"{"command":"swift test"}"#),
                       result("b-result", "b", #"{"exit_code":1,"output":"specific failure"}"#)]
        let controller = reader(records)
        #expect(controller.rows.count == 2)
        let failed = controller.measurement(at: 1)
        #expect(failed.title == "Failed · Run swift test")
        #expect(failed.hasFailure)
        #expect(failed.body.isEmpty)
        let hit = try #require(ConversationMatcher.matches(records, query: "specific failure").first)
        controller.update(sessionID: "activity", records: records, provider: "claude",
                          findQuery: "specific failure", findHit: hit, findGeneration: 1)
        #expect(controller.measurement(at: 1).body.contains("specific failure"))
        #expect(controller.selectedFindRange != nil)
    }

    @Test func aPagedFailurePromotesItsOperationWithoutLosingExpandedContents() {
        let calls = [entry("a", "Read", input: #"{"path":"a.swift"}"#),
                     entry("b", "Bash", input: #"{"command":"swift test"}"#)]
        let controller = reader(calls)
        controller.toggle(controller.rows[0].id)
        controller.toggle("activity:b")
        controller.update(sessionID: "activity", records: calls + [result("r", "b", #"{"is_error":true}"#)], provider: "claude")
        #expect(controller.rows.count == 2)
        #expect(controller.measurement(at: 0).contentX == 30)
        #expect(controller.measurement(at: 1).isExpanded)
        #expect(controller.measurement(at: 1).hasFailure)
        #expect(controller.rows[1].records.map(\.id) == ["b", "r"])
    }

    @Test func failureLeavesSurroundingRoutineSpansCompactAndPreservesPairing() throws {
        let calls = (0..<5).map { entry("call-\($0)", "Bash", input: "{\"command\":\"step \($0)\"}") }
        let records = calls + [result("failed-output", "call-2", #"{"exit_code":2,"output":"failed middle"}"#)]
        let controller = reader(records)
        #expect(controller.rows.count == 3)
        #expect(controller.measurement(at: 0).title == "Ran 2 commands")
        #expect(controller.measurement(at: 1).hasFailure)
        #expect(controller.measurement(at: 2).title == "Ran 2 commands")
        #expect(controller.rows[1].records.map(\.id) == ["call-2", "failed-output"])
        #expect(Set(controller.rows.flatMap(\.records).map(\.id)) == Set(records.map(\.id)))
        let hit = try #require(ConversationMatcher.matches(records, query: "step 4").first)
        controller.update(sessionID: "activity", records: records, provider: "claude",
                          findQuery: "step 4", findHit: hit, findGeneration: 1)
        let row = try #require(controller.rows.firstIndex { $0.id == "activity:call-4" })
        #expect(controller.measurement(at: row).isExpanded)
        #expect(controller.selectedFindRange != nil)
    }

    @Test func assistantMessagesAndUnknownStatesRemainVisible() {
        let records = [entry("a", "custom_operation", input: "unstructured"),
                       TranscriptRecord(recordID: "commentary", record: Message(role: "assistant", text: "Checking the result",
                            toolName: nil, toolInput: nil, toolOutput: nil)),
                       entry("b", "Bash", input: #"{"command":"swift test"}"#)]
        let controller = reader(records)
        #expect(controller.rows.count == 3)
        #expect(controller.measurement(at: 0).title == "Custom Operation")
        #expect(!controller.measurement(at: 0).hasFailure)
        #expect(controller.measurement(at: 1).body == "Checking the result")
        #expect(controller.measurement(at: 2).title == "Run swift test")
        #expect(!controller.measurement(at: 2).isExpanded)
    }

    @Test func groupSummaryDescribesOperationsWithoutCountingReasoning() {
        let thought = TranscriptRecord(recordID: "thought", record: Message(role: "reasoning",
            text: "**Inspecting the reader**\n\nLooking at rows.", toolName: nil, toolInput: nil, toolOutput: nil))
        let records = [thought, entry("a", "Bash", input: #"{"command":"swift build"}"#),
                       entry("b", "Bash", input: #"{"command":"swift test"}"#),
                       entry("c", "Edit", input: #"{"file_path":"Models.swift"}"#),
                       entry("d", "lookup_issue", input: "{}")]
        let controller = reader(records)
        #expect(controller.measurement(at: 0).title == "Ran 2 commands, made an edit and called Lookup Issue")
        controller.toggle(controller.rows[0].id)
        #expect(controller.measurement(at: 1).title == "Thought · Inspecting the reader")
        let onlyThoughts = reader([thought, TranscriptRecord(recordID: "more", record: Message(role: "reasoning",
            text: "Plain thinking", toolName: nil, toolInput: nil, toolOutput: nil))])
        #expect(onlyThoughts.measurement(at: 0).title == "Thought")
    }

    @Test func completedWorkSummarizesItsOperations() {
        func record(_ id: String, _ role: String, tool: String? = nil, input: String? = nil, phase: String? = nil,
                    lifecycle: String? = nil) -> TranscriptRecord {
            TranscriptRecord(recordID: id, record: Message(role: role, text: role == "assistant" ? "Text \(id)" : "",
                toolName: tool, toolInput: input, toolOutput: nil, sourceTurnID: "t", assistantPhase: phase,
                lifecycleEvent: lifecycle))
        }
        let records = [record("c", "assistant", phase: "commentary"),
                       record("r", "tool_use", tool: "Read", input: #"{"path":"a.swift"}"#),
                       record("f", "assistant", phase: "final_answer"),
                       record("done", "lifecycle", lifecycle: "task_complete")]
        let controller = reader(records)
        #expect(controller.measurement(at: 0).title == "Worked · read a file")
        #expect(controller.measurement(at: 1).body == "Text f")
    }

    @Test func statusLinesAndEmptyCallsHaveNoDisclosureChevron() throws {
        let interrupted = TranscriptRecord(recordID: "abort", record: Message(role: "lifecycle", text: "", toolName: nil,
            toolInput: nil, toolOutput: nil, lifecycleEvent: "turn_aborted"))
        let failed = TranscriptRecord(recordID: "failed", record: Message(role: "lifecycle", text: "Rate limited", toolName: nil,
            toolInput: nil, toolOutput: nil, lifecycleEvent: "turn_failed"))
        let bare = TranscriptRecord(recordID: "bare", record: Message(role: "tool_use", text: "", toolName: "Plan",
            toolInput: nil, toolOutput: nil))
        let controller = reader([interrupted, TranscriptRecord(recordID: "u", record: Message(role: "user", text: "Next",
            toolName: nil, toolInput: nil, toolOutput: nil)), failed, bare])
        let status = controller.measurement(at: 0)
        #expect(status.title == "Interrupted" && status.isDisclosure && !status.canExpand)
        #expect(controller.measurement(at: 2).canExpand)
        #expect(!controller.measurement(at: 3).canExpand)
        controller.view.layoutSubtreeIfNeeded()
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        let header = try #require(cell.subviews.compactMap { $0 as? NSButton }.first { !$0.isHidden && $0.title == "Interrupted" })
        #expect(header.image == nil)
        header.performClick(nil)
        #expect(!controller.measurement(at: 0).isExpanded)
    }

    @Test func emptyResultSaysNoOutputInsteadOfABareHeading() {
        let records = [entry("a", "Bash", input: #"{"command":"true"}"#), result("a-result", "a", "")]
        let controller = reader(records)
        controller.toggle(controller.rows[0].id)
        let body = controller.measurement(at: 0).attributedBody.string
        #expect(body.hasSuffix("Output\nNo output"))
        #expect(ConversationMatcher.body(records[1]).isEmpty)
    }

    private func reader(_ records: [TranscriptRecord]) -> TranscriptController {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        controller.update(sessionID: "activity", records: records, provider: "claude")
        return controller
    }

    private func entry(_ id: String, _ tool: String, input: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: "tool_use", text: "", toolName: tool,
            toolInput: input, toolOutput: nil, eventID: id))
    }

    private func result(_ id: String, _ call: String, _ output: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: "tool_result", text: "", toolName: nil,
            toolInput: nil, toolOutput: output, parentToolUseID: call))
    }
}
