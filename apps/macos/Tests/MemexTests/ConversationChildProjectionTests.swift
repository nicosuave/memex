import Foundation
import Testing
@testable import Memex
#if canImport(SQACPHost)
import SQACPHost

@Test func childProjectionPreservesTimelineMediaAndToolIdentityWithoutDuplicates() throws {
    let input = AgentMessageProjection(id: .init(rawValue: "input"), role: .user, content: "Inspect this",
        attachments: [.init(id: "image", kind: .image, name: "Picture", previewData: "aW1hZ2U=", sourceURI: "/original/private.png")],
        providerMessageID: "provider-uuid")
    let answer = AgentMessageProjection(id: .init(rawValue: "answer"), role: .assistant, content: "Result", assistantPhase: "final_answer")
    let tool = AgentActivityProjection(id: .init(rawValue: "read"), kind: "command", title: "Read", status: "completed",
        rawInputJSON: #"{"path":"file.txt"}"#, rawOutputJSON: "file body", details: ["file body"])
    let snapshot = AgentThreadSnapshot(id: .init(rawValue: "child-thread"),
        session: .init(providerID: "codex", sessionID: "child", modelID: "child-model", reasoningID: "high"),
        timelineItems: [.message(input.id), .activity(tool.id), .message(answer.id), .activity(tool.id)],
        messageOrder: [input.id, answer.id], messagesByID: [input.id: input, answer.id: answer],
        activityOrder: [tool.id], activitiesByID: [tool.id: tool], isLoadingHistory: true)
    // Public Codable is the canonical external construction API for this projection.
    struct Fixture: Encodable { let session: AgentChildSession; let snapshot: AgentThreadSnapshot; let error: String? }
    let fixture = Fixture(session: .init(id: "child", rootSessionID: "root", parentSessionID: "root", title: "Task", status: "running"),
                          snapshot: snapshot, error: "Read warning")
    let child = try JSONDecoder().decode(AgentChildConversation.self, from: JSONEncoder().encode(fixture))
    let history = ConversationChildHistory(child)
    #expect(history.records.map(\.record.role) == ["user", "tool_use", "tool_result", "assistant"])
    #expect(history.records.map(\.id).count == Set(history.records.map(\.id)).count)
    #expect(history.records[0].sourceID == "provider-uuid")
    #expect(history.records[0].record.eventID == "input")
    let raw = try JSONDecoder().decode(AgentMessageProjection.self, from: Data(history.records[0].rawTranscriptBody.utf8))
    #expect(raw.attachments?.first?.sourceURI == "/original/private.png")
    let rendered = try JSONDecoder().decode([[String: String]].self,
        from: Data(try #require(history.records[0].record.sourceContent).utf8))
    #expect(rendered.allSatisfy { !$0.values.contains("/original/private.png") })
    #expect(SourceContent.blocks(history.records[0].record).count == 1)
    #expect(history.records[1].record.toolInput == #"{"path":"file.txt"}"#)
    #expect(history.records[2].record.toolOutput == "file body")
    #expect(history.records[2].record.parentToolUseID == "read")
    #expect(history.records[3].record.text == "Result")
    #expect(history.records[3].record.assistantPhase == "final_answer")
    #expect(history.loading)
    #expect(history.status == "running")
    #expect(history.error == "Read warning")
    #expect(history.model == "child-model")
    #expect(history.reasoningEffort == "high")
}
#endif
