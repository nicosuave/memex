import AppKit
import Testing
@testable import Memex

@MainActor @Suite struct NativeMcpAppTests {
    private let appJSON = #"{"providerID":"codex","threadID":"native","toolCallID":"call","server":"server","tool":"chart","resourceURI":"ui://chart","argumentsJSON":"{}","interactionMode":"fallbackOnly"}"#

    @Test func requiresExactNativeToolCallIdentity() {
        #expect(NativeMcpAppDescriptor.decode(appJSON, expectedCallID: "different") == nil)
        #expect(NativeMcpAppDescriptor.decode(appJSON, expectedCallID: nil) == nil)
        #expect(NativeMcpAppDescriptor.decode(appJSON, expectedCallID: "call")?.toolCallID == "call")
    }

    @Test func appMetadataDoesNotReplaceOriginalOutputOrRawEvidence() {
        var message = Message(role: "tool_result", text: "", toolName: "chart", toolInput: nil, toolOutput: "Original output", parentToolUseID: "call")
        message.mcpAppJSON = appJSON
        let record = TranscriptRecord(recordID: "result", record: message)
        let blocks = ToolContentRenderer.richBlocks([record])
        #expect(blocks.contains { if case .mcpApp = $0 { true } else { false } })
        #expect(ToolContentRenderer.render([record]).string.contains("Original output"))
        #expect(ToolContentRenderer.render([record], raw: true).string == "Original output")
        let layout = RichContentLayout(blocks: blocks, font: .systemFont(ofSize: 14), context: RichContentContext())
        #expect(layout.materializedView == nil)
        #expect(layout.height(for: 500) > 0)
        #expect(layout.height(for: 500) < NativeMcpAppHost.height)
    }

    @Test func untypedToolOutputCannotCreateAnApp() {
        let message = Message(role: "tool_result", text: "", toolName: "chart", toolInput: nil, toolOutput: appJSON, parentToolUseID: "call")
        #expect(ToolContentRenderer.mcpApps([TranscriptRecord(recordID: "result", record: message)]).isEmpty)
    }

    @Test func duplicatePairedMetadataProducesOneHost() {
        var call = Message(role: "tool_use", text: "", toolName: "chart", toolInput: "{}", toolOutput: nil, eventID: "call")
        call.mcpAppJSON = appJSON
        var result = Message(role: "tool_result", text: "", toolName: "chart", toolInput: nil, toolOutput: "output", parentToolUseID: "call")
        result.mcpAppJSON = appJSON
        #expect(ToolContentRenderer.mcpApps([TranscriptRecord(recordID: "call", record: call), TranscriptRecord(recordID: "result", record: result)]).count == 1)
    }
}
