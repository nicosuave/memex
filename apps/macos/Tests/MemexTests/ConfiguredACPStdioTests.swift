import Foundation
import Testing
@testable import Memex

#if canImport(SQACPHost)
import SQACP
import SQACPHost

/// A real subprocess and the real Rust ACP FFI, without an installed agent or
/// external credentials. SQACPHost unit tests separately use the FFI test stub.
struct ConfiguredACPStdioTests {
    @Test func configuredACPStdioCreatesResumesAndArchivesOriginalIdentity() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-acp-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let script = root.appendingPathComponent("fixture.sh")
        try Self.server.write(to: script, atomically: true, encoding: .utf8)
        let profile = ConfiguredConversationProvider(name: "ACP fixture", executablePath: "/bin/sh",
            arguments: [script.path], homePath: root.path, homeEnvironmentKey: "MEMEX_ACP_FIXTURE_HOME")
        let creation = try await Task.detached {
            try AgentConversationCreation.acp(provider: profile.id, configuration: profile.acpConfiguration, cwd: root.path)
        }.value
        #expect(creation.nativeSessionID == "fixture-session")
        #expect(creation.protocolName == "acp")
        #expect(creation.supportsResume)
        let runtime = try AgentRuntimeClient(databaseURL: root.appendingPathComponent("runtime.sqlite"))
        let archiveURL = root.appendingPathComponent("history")
        let service = try AgentConversationService(runtime: runtime, archiveURL: archiveURL, executionHostID: "local")
        let binding = AgentConversationBinding(sessionID: "fixture-catalog-session", sourceID: nil,
            nativeSessionID: creation.nativeSessionID, providerInstanceID: "fixture-original-home",
            executionHostID: "local", workspaceID: "fixture-workspace", cwd: root.path)
        try service.connectCreated(binding, creation: creation)
        defer { try? service.disconnect(sessionID: binding.sessionID) }
        try await Self.waitForReady(service, binding)
        _ = try service.perform(.prompt, sessionID: binding.sessionID, commandID: "fixture-command",
            issuedAt: "2026-10-05T00:00:00Z", text: "Original question")
        try await Self.waitForMessage("Fixture answer", service: service, binding: binding)
        try service.disconnect(sessionID: binding.sessionID)

        let reopenedRuntime = try AgentRuntimeClient(databaseURL: root.appendingPathComponent("runtime.sqlite"))
        let reopened = try AgentConversationService(runtime: reopenedRuntime, archiveURL: archiveURL, executionHostID: "local")
        try reopened.registerACP(binding, provider: profile.id)
        try reopened.connectACP(binding, configuration: profile.acpConfiguration)
        defer { try? reopened.disconnect(sessionID: binding.sessionID) }
        try await Self.waitForReady(reopened, binding)
        let json = try reopened.read(sessionID: binding.sessionID)
        let view = try #require(try ConversationProjection.conversation(in: json, sessionID: binding.sessionID))
        let records = ConversationProjection.snapshot(view, ready: true, canCancel: true).records
        #expect(records.filter { $0.record.role == "user" }.map(\.record.text) == ["Original question"])
        #expect(records.filter { $0.record.role == "assistant" }.map(\.record.text) == ["Fixture answer"])
        let methods = try String(contentsOf: root.appendingPathComponent("methods.log"), encoding: .utf8)
        #expect(methods.components(separatedBy: "session/new").count - 1 == 1)
        #expect(methods.components(separatedBy: "session/load").count - 1 == 1)
        let files = try FileManager.default.contentsOfDirectory(at: archiveURL.appendingPathComponent("acp-observations"),
            includingPropertiesForKeys: nil)
        #expect(files.count == 2)
        let receipts = try files.flatMap { file in
            try String(contentsOf: file, encoding: .utf8).split(separator: "\n").map { line in
                try JSONDecoder().decode(RawTranscriptJSON.self, from: Data(line.utf8))
            }
        }
        #expect(receipts.allSatisfy { $0["providerInstanceID"].string == "fixture-original-home" })
        #expect(receipts.contains { $0["direction"].string == "dispatch_intent" })
        #expect(receipts.contains { $0["chunk"].string?.contains("session_loaded") == true })
    }

    private static func waitForReady(_ service: AgentConversationService, _ binding: AgentConversationBinding) async throws {
        for _ in 0..<200 {
            if service.supportedActions(sessionID: binding.sessionID).contains(.prompt) { return }
            try await Task.sleep(for: .milliseconds(25))
        }
        throw ConversationRuntimeError(message: "ACP fixture did not become ready")
    }

    private static func waitForMessage(_ text: String, service: AgentConversationService,
                                       binding: AgentConversationBinding) async throws {
        for _ in 0..<200 {
            let json = try service.read(sessionID: binding.sessionID)
            if let view = try ConversationProjection.conversation(in: json, sessionID: binding.sessionID),
               ConversationProjection.snapshot(view, ready: true, canCancel: true).records.contains(where: { $0.record.text == text }),
               view["state"]["running"].bool != true { return }
            try await Task.sleep(for: .milliseconds(25))
        }
        throw ConversationRuntimeError(message: "ACP fixture prompt did not complete")
    }

    private static let server = #"""
    while IFS= read -r request; do
      id=$(printf '%s\n' "$request" | /usr/bin/sed -E 's/.*"id"[[:space:]]*:[[:space:]]*([^,}]+).*/\1/')
      case "$request" in
        *'"initialize"'*)
          printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"agentCapabilities":{"loadSession":true},"authMethods":[]}}\n' "$id"
          ;;
        *'"session/new"'*)
          printf 'session/new\n' >> "$MEMEX_ACP_FIXTURE_HOME/methods.log"
          printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"fixture-session"}}\n' "$id"
          ;;
        *'"session/load"'*)
          printf 'session/load\n' >> "$MEMEX_ACP_FIXTURE_HOME/methods.log"
          printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture-session","update":{"sessionUpdate":"user_message_chunk","content":{"type":"text","text":"Original question"}}}}'
          printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Fixture answer"}}}}'
          printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
          ;;
        *'"session/prompt"'*)
          printf '%s\n' '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"fixture-session","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"Fixture answer"}}}}'
          printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id"
          ;;
      esac
    done
    """#
}
#endif
