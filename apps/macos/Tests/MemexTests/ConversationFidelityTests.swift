import AppKit
import Foundation
import Testing
@testable import Memex

@Test func persistedMetadataRestoresExactPhaseTurnAndCompletedWork() throws {
    let source = #"""
    {"presentation":[
      {"entity_id":"entry1","body":{"kind":"entry","data":{"source_order":1,"payload":{"type":"entity","data":{"entity_id":"work"}}}}},
      {"entity_id":"work","evidence":[{"record":"work"}],"body":{"kind":"message","data":{"role":"assistant","parts":[{"type":"text","data":"Checking"}]}}},
      {"entity_id":"meta1","evidence":[{"record":"work"}],"body":{"kind":"entry","data":{"source_order":1,"display":false,"payload":{"type":"metadata","data":{"namespace":"codex","value":{"associated_turn_id":"turn","payload":{"phase":"commentary"}}}}}}},
      {"entity_id":"entry2","body":{"kind":"entry","data":{"source_order":2,"payload":{"type":"entity","data":{"entity_id":"answer"}}}}},
      {"entity_id":"answer","evidence":[{"record":"answer"}],"body":{"kind":"message","data":{"role":"assistant","parts":[{"type":"text","data":"Done"}]}}},
      {"entity_id":"meta2","evidence":[{"record":"answer"}],"body":{"kind":"entry","data":{"source_order":2,"payload":{"type":"metadata","data":{"namespace":"codex","value":{"associated_turn_id":"turn","payload":{"phase":"final_answer"}}}}}}},
      {"entity_id":"complete","body":{"kind":"entry","data":{"source_order":3,"payload":{"type":"metadata","data":{"namespace":"codex/event_msg/task_complete","value":{"type":"event_msg","payload":{"type":"task_complete","turn_id":"turn"}}}}}}}
    ]}
    """#
    let records = try fidelitySnapshot(source).records
    #expect(records.first { $0.record.text == "Checking" }?.record.assistantPhase == "commentary")
    #expect(records.first { $0.record.text == "Done" }?.record.sourceTurnID == "turn")
    #expect(records.filter(\.isRawOnly).count == 2)
    let groups = TranscriptPresentation.group(records)
    #expect(groups.contains { $0.isCompletedWork && $0.records.contains { $0.record.text == "Checking" } })
    #expect(groups.contains { !$0.isCompletedWork && $0.records.contains { $0.record.text == "Done" } })
}

@Test func livePhaseUsesNativeItemIdentityAndKeepsAbortedWorkVisible() throws {
    let records = try fidelitySnapshot(#"""
    {"ephemeral":[
      {"item_id":"a","native_turn_id":"t","source_order":1,"body":{"kind":"message","data":{"role":"assistant","parts":[{"type":"text","data":"Same"}]}}},
      {"item_id":"b","native_turn_id":"t","source_order":2,"body":{"kind":"message","data":{"role":"assistant","parts":[{"type":"text","data":"Same"}]}}},
      {"item_id":"phase","source_order":3,"body":{"kind":"entry","data":{"payload":{"type":"metadata","data":{"namespace":"memex/live","value":{"item_id":"b","phase":"final_answer"}}}}}},
      {"item_id":"end","source_order":4,"body":{"kind":"entry","data":{"payload":{"type":"metadata","data":{"namespace":"memex/live","value":{"type":"event_msg","payload":{"type":"turn_aborted","turn_id":"t"}}}}}}}
    ]}
    """#).records
    #expect(records.first { $0.id == "runtime:a:part-0" }?.record.assistantPhase == nil)
    #expect(records.first { $0.id == "runtime:b:part-0" }?.record.assistantPhase == "final_answer")
    #expect(records.contains { $0.record.lifecycleEvent == "turn_aborted" })
    #expect(!TranscriptPresentation.group(records).contains { $0.isCompletedWork })
}

@Test @MainActor func claudeNestedToolImageAndFullOutputReferenceRemainRenderable() throws {
    let records = try fidelitySnapshot(#"""
    {"presentation":[
      {"entity_id":"entry","body":{"kind":"entry","data":{"source_order":1,"payload":{"type":"entity","data":{"entity_id":"result"}}}}},
      {"entity_id":"result","body":{"kind":"tool_result","data":{"native_correlation_key":"call","outcome":"success","completeness":"partial","full_artifact":{"entity_id":"full"},"parts":[{"type":"opaque","data":{"namespace":"claude","native_type":"image","value":{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aW1hZ2U="}}}}]}}},
      {"entity_id":"full","body":{"kind":"artifact","data":{"native_locations":["/tmp/provider-output.txt"],"content":{"type":"unavailable"}}}}
    ]}
    """#).records
    let result = try #require(records.first { $0.record.role == "tool_result" })
    #expect(result.record.outputCompleteness == "partial")
    #expect(result.record.toolOutput == "")
    let blocks = ToolContentRenderer.richBlocks([result])
    #expect(blocks.contains { if case .embeddedImage(_, let bytes, _) = $0 { return bytes == Data("image".utf8) }; return false })
    #expect(blocks.contains { if case .attachment(let label, let path, false) = $0 { return label == "Full output" && path == "/tmp/provider-output.txt" }; return false })
    #expect(result.rawTranscriptBody.contains("native_correlation_key"))
}

@Test func structuredPlansUseSchemasAndLatestRevision() throws {
    let plain = TranscriptRecord(recordID: "plain", record: Message(role: "assistant", text: "My plan: do work", toolName: nil, toolInput: nil, toolOutput: nil))
    let first = TranscriptRecord(recordID: "first", record: Message(role: "tool_use", text: "", toolName: "functions.update_plan", toolInput: #"{"plan":[{"step":"Inspect","status":"pending"}]}"#, toolOutput: nil))
    var update = first.record
    update.structuredActivity = #"{"plan":{"_0":[{"text":"Inspect","status":"completed"},{"text":"Ship","status":"pending"}]}}"#
    let last = TranscriptRecord(recordID: "last", record: update)
    #expect(ConversationWork.project([plain]).plan == nil)
    let state = ConversationWork.project([first, last])
    #expect(state.plan?.recordID == "last")
    #expect(state.plan?.steps.map(\.status) == ["completed", "pending"])
}

@Test func childStatusesKeepExactNativeIDsAndUnknownState() {
    let spawn = TranscriptRecord(recordID: "call", record: Message(role: "tool_use", text: "", toolName: "spawn_agent", toolInput: #"{"message":"Inspect parser"}"#, toolOutput: nil, eventID: "spawn"))
    let child = TranscriptRecord(recordID: "child", record: Message(role: "tool_result", text: "", toolName: nil, toolInput: nil, toolOutput: #"{"agent_id":"native-child"}"#, parentToolUseID: "spawn"))
    #expect(ConversationWork.project([spawn, child]).agents == [.init(id: "native-child", prompt: "Inspect parser", status: "unknown")])
    let wait = TranscriptRecord(recordID: "wait", record: Message(role: "tool_use", text: "", toolName: "wait", toolInput: "{}", toolOutput: nil, eventID: "wait-call"))
    let done = TranscriptRecord(recordID: "done", record: Message(role: "tool_result", text: "", toolName: nil, toolInput: nil, toolOutput: #"{"status":{"native-child":{"completed":"Parser checked"}}}"#, parentToolUseID: "wait-call"))
    #expect(ConversationWork.project([spawn, child, wait, done]).agents.first?.status == "completed")
}

@Test @MainActor func rawOnlyEvidenceCanBeFoundAndRevealedExactly() throws {
    let record = TranscriptRecord(recordID: "metadata", record: Message(role: "system", text: "", toolName: nil, toolInput: nil, toolOutput: nil),
                                  rawJSON: #"{"provider_detail":"needle"}"#, isRawOnly: true)
    let hit = try #require(ConversationMatcher.matches([record], query: "needle").first)
    let controller = TranscriptController()
    controller.view.frame = CGRect(x: 0, y: 0, width: 700, height: 400)
    controller.update(sessionID: "raw-evidence", records: [record], provider: "codex",
                      findQuery: "needle", findHit: hit, findGeneration: 1)
    #expect(controller.rows.count == 1)
    #expect(controller.measurement(at: 0).attributedBody.string == record.rawTranscriptBody)
    #expect(controller.selectedFindRange == hit.range)
}

@Test func completedSpawnOperationDoesNotClaimChildCompleted() {
    var message = Message(role: "tool_use", text: "", toolName: "spawn_agent", toolInput: nil, toolOutput: nil)
    message.structuredActivity = #"{"subagent":{"_0":{"operation":"spawn","agentIDs":["child"],"prompt":"Work"}}}"#
    message.activityStatus = "completed"
    #expect(ConversationWork.project([TranscriptRecord(recordID: "spawn", record: message)]).agents.first?.status == "unknown")
    var child = Message(role: "subagent", text: "Work", toolName: nil, toolInput: nil, toolOutput: nil)
    child.structuredActivity = #"{"child":{"id":"child","parentSessionID":"parent","title":"Work","status":"running"}}"#
    let state = ConversationWork.project([TranscriptRecord(recordID: "child", record: child), TranscriptRecord(recordID: "spawn", record: message)])
    #expect(state.agents.first?.status == "running")
    #expect(state.agents.first?.parentID == "parent")
}

private func fidelitySnapshot(_ json: String) throws -> ConversationSnapshot {
    ConversationProjection.snapshot(try JSONDecoder().decode(RawTranscriptJSON.self, from: Data(json.utf8)), ready: true, canCancel: true)
}
