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
        #expect(controller.measurement(at: 0).title == "3 activities · 2 reads, 1 search")
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
        #expect(controller.measurement(at: 0).title == "2 activities · 2 commands")
        #expect(controller.measurement(at: 1).hasFailure)
        #expect(controller.measurement(at: 2).title == "2 activities · 2 commands")
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
