import AppKit
import SwiftUI
import Testing
@testable import Memex

#if canImport(SQACPHost)
import SQACPHost

@Suite(.serialized) @MainActor struct NewSessionPresentationTests {
    @Test func newNativeSessionConfirmsAcknowledgedDeliveryWithoutTranscriptEcho() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-new-session-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        let helper = root.appendingPathComponent("helper.sh")
        try #"""
        #!/bin/sh
        while IFS= read -r request; do
          request=$(printf '%s\n' "$request" | /usr/bin/sed 's@\\/@/@g')
          id=$(printf '%s\n' "$request" | /usr/bin/sed -E 's/.*"id"[[:space:]]*:[[:space:]]*([^,}]+).*/\1/')
          case "$request" in
            *'"initialize"'*) printf '{"id":%s,"result":{"protocolVersion":1}}\n' "$id" ;;
            *'"session/new"'*)
              generation=$(printf '%s\n' "$request" | /usr/bin/sed -E 's/.*"loadGeneration"[[:space:]]*:[[:space:]]*([0-9]+).*/\1/')
              printf '{"id":%s,"result":{"sessionId":"fixture-session"}}\n' "$id"
              printf '{"event":{"type":"sessionOpened","loadGeneration":%s,"session":{"id":"fixture-session","status":"idle","isArchived":false},"modelId":"fixture"}}\n' "$generation"
              ;;
            *'"turn/send"'*)
              printf '{"event":{"type":"turnStarted","threadId":"fixture-session","turnId":"fixture-turn"}}\n'
              while [ ! -f allow-ack ]; do /bin/sleep 0.01; done
              printf '{"id":%s,"result":{"turnId":"fixture-turn","userMessageUUID":"fixture-user"}}\n' "$id"
              ;;
            *) printf '{"id":%s,"result":{}}\n' "$id" ;;
          esac
        done
        """#.write(to: helper, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: helper.path)
        let creation = try await Task.detached {
            try AgentConversationCreation.claude(hostExecutablePath: helper.path,
                claudeExecutablePath: "/bin/echo", cwd: root.path)
        }.value
        let source = root.appendingPathComponent("projects/fixture/fixture-session.jsonl")
        let session = Session(source: "claude", sessionID: creation.nativeSessionID, sourcePath: source.path,
            project: "fixture", cwd: root.path, machine: "local")
        let target = InAppResumeTarget(session: session, sourceURL: source, workingDirectory: root,
            providerHome: root, executableURL: URL(fileURLWithPath: "/bin/echo"), helperURL: helper,
            storageURL: root.appendingPathComponent("runtime"))
        let live = LiveConversations()
        let conversation = await live.adopt(CreatedConversation(session: session,
            runtime: NativeConversationRuntime(creation: creation), target: target))
        #expect(conversation.snapshot.ready)
        #expect(conversation.error == nil)
        #expect(conversation.snapshot.warning == nil)
        #expect(conversation.snapshot.records.isEmpty)
        // The normal missing-file poll must not turn an empty new chat into a warning.
        try await Task.sleep(for: .milliseconds(1200))
        #expect(conversation.snapshot.warning == nil)
        #expect(!FileManager.default.fileExists(atPath: source.path))
        conversation.draft = "Fixture prompt"
        await conversation.send()
        let commandID = try #require(conversation.pendingPrompt?.commandID)
        let runtimeSessionID = "memex-" + InAppResumeTarget.digest(session.id)
        let database = target.storageURL.appendingPathComponent("runtime.sqlite")
        #expect(try !InAppAgentRuntime.confirmedDelivery(databaseURL: database,
            sessionID: runtimeSessionID, commandID: commandID))
        try Data().write(to: root.appendingPathComponent("allow-ack"))
        for _ in 0..<200 where conversation.pendingPrompt != nil {
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(conversation.pendingPrompt == nil)
        #expect(!conversation.snapshot.records.contains { $0.record.role == "user" })
        #expect(conversation.canChangeSettings)
        await live.disconnectAll()
        #expect(try InAppAgentRuntime.confirmedDelivery(databaseURL: database,
            sessionID: runtimeSessionID, commandID: commandID))
        #expect(try !InAppAgentRuntime.confirmedDelivery(databaseURL: database,
            sessionID: runtimeSessionID, commandID: "different-command"))
    }

    @Test func historyStatusDoesNotReserveARowWhenIdle() {
        let store = Store()
        let session = Session(source: "claude", sessionID: "header", sourcePath: "/fixture",
            project: "fixture", label: String(repeating: "Long conversation title ", count: 20))
        for width: CGFloat in [320, 780] {
            let view = NSHostingView(rootView: ConversationHistoryActions(store: store, session: session).frame(width: width))
            view.layoutSubtreeIfNeeded()
            #expect(view.fittingSize.height == 0)
        }
    }
}
#endif
