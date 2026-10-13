import Foundation
import Testing
@testable import Memex

@Test func independentToolMetadataPreservesPlanAppAndNativeEvidence() throws {
    let records = try metadataSnapshot([
        "presentation": [
            ["entity_id": "entry", "body": ["kind": "entry", "data": ["source_order": 1,
                "payload": ["type": "entity", "data": ["entity_id": "persisted-tool"]]]]],
            ["entity_id": "persisted-tool", "body": ["kind": "tool_invocation", "data": [
                "native_call_id": "native-call", "name": "Plan", "input": ["text": "original input"]]]],
        ],
        "ephemeral": [
            metadataEntry("activity", order: 1, target: "native-call", fields: [
                "activity": ["plan": ["_0": [["text": "Implement", "status": "pending"]]]], "status": "completed"]),
            metadataEntry("document", order: 2, target: "native-call", fields: [
                "planDocument": ["id": "native-plan", "text": "# Full plan\n\nPreserve rationale."]]),
            metadataEntry("app", order: 3, target: "native-call", fields: ["mcpApp": ["id": "app-one"]]),
        ],
    ]).records
    let tool = try #require(records.first { $0.record.role == "tool_use" })
    #expect(tool.record.eventID == "native-call")
    #expect(tool.record.activityStatus == "completed")
    #expect(tool.record.structuredActivity?.contains("Preserve rationale.") == true)
    #expect(tool.record.structuredActivity?.contains("Implement") == true)
    #expect(tool.record.mcpAppJSON?.contains("app-one") == true)
    #expect(tool.rawTranscriptBody.contains("original input"))
    #expect(records.filter { $0.record.role == "tool_use" }.count == 1)
    #expect(records.filter(\.isRawOnly).count == 3)
}

@Test func metadataNullRevokesAppWithoutErasingIndependentActivity() throws {
    let records = try metadataSnapshot(["ephemeral": [
        ["item_id": "call", "source_order": 1, "body": ["kind": "tool_invocation", "data": [
            "native_call_id": "call", "name": "Tool"]]],
        metadataEntry("activity", order: 2, target: "call", fields: ["activity": ["label": "Retained"]]),
        metadataEntry("old-app", order: 3, target: "call", fields: ["mcpApp": ["id": "old-app"]]),
        metadataEntry("revoke", order: 4, target: "call", fields: ["mcpApp": NSNull()]),
    ]]).records
    let tool = try #require(records.first { $0.record.role == "tool_use" })
    #expect(tool.record.mcpAppJSON == nil)
    #expect(tool.record.structuredActivity?.contains("Retained") == true)
}

@Test func restoredPlanWithoutToolRetainsFullDocumentOnce() throws {
    let records = try metadataSnapshot(["ephemeral": [
        metadataEntry("document", order: 1, target: "native-plan", fields: [
            "planDocument": ["id": "native-plan", "text": "# Recovered plan\n\nDetailed design."]]),
    ]]).records
    let visible = records.filter { !$0.isRawOnly }
    #expect(visible.count == 1)
    #expect(visible.first?.record.eventID == "native-plan")
    #expect(visible.first?.record.structuredActivity?.contains("Detailed design.") == true)
    #expect(records.filter(\.isRawOnly).first?.rawTranscriptBody.contains("Detailed design.") == true)
}

private func metadataEntry(_ id: String, order: Int, target: String, fields: [String: Any]) -> [String: Any] {
    let value = fields.merging(["item_id": target]) { _, new in new }
    return ["item_id": "presentation/" + id, "source_order": order,
            "body": ["kind": "entry", "data": ["payload": ["type": "metadata", "data": [
                "namespace": "memex/live", "value": value]]]]]
}

private func metadataSnapshot(_ fields: [String: Any]) throws -> ConversationSnapshot {
    let bytes = try JSONSerialization.data(withJSONObject: fields)
    return ConversationProjection.snapshot(try JSONDecoder().decode(RawTranscriptJSON.self, from: bytes),
        ready: true, canCancel: true)
}
