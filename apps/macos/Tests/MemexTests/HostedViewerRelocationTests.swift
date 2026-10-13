import Foundation
import Testing
@testable import Memex

@MainActor struct HostedViewerRelocationTests {
    @Test func explicitMovedConversationRefreshesWorkspaceAndPreservesDurableIntent() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let drafts = ConversationDraftStore(directory: directory)
        let session = Session(source: "codex", sessionID: "native", sourcePath: "/host/.codex/sessions/native.jsonl",
                              project: "project", cwd: "/host/old", machine: "host")
        let attachment = ConversationAttachment(id: "captured", title: "Evidence", path: "/former/path", content: Data("captured bytes".utf8))
        let queued = ConversationQueuedPrompt(.init(.prompt, text: "Later", attachments: [attachment], id: "stable-command"))
        drafts.set(.init(text: "Unsent draft", attachments: [attachment], queue: [queued], queueHeld: true), for: session.id)
        await drafts.flush()
        let host = ExecutionHostConnection(id: "verified-host", name: "Host", machineID: "host", endpoint: URL(string: "https://example.invalid")!)
        let live = LiveConversations(drafts: drafts)
        let store = Store(liveConversations: live)
        try await store.adoptHostedConversation(session, connection: host, conversationID: "host-thread")
        let original = try #require(live.sessions[session.id])
        try await store.adoptHostedConversation(session, connection: host, conversationID: "host-thread")
        #expect(live.sessions[session.id] === original)

        var moved = session
        moved.cwd = "/host/new"
        try await store.adoptHostedConversation(moved, connection: host, conversationID: "host-thread")
        let refreshed = try #require(live.sessions[session.id])
        #expect(refreshed !== original)
        #expect(refreshed.isServerOwned && !refreshed.snapshot.connected)
        #expect(refreshed.session.cwd == "/host/new")
        #expect(store.selected?.cwd == "/host/new")
        #expect(store.conversationLibrary.savedSession(id: session.id)?.cwd == "/host/new")
        #expect(refreshed.draft == "Unsent draft")
        #expect(refreshed.attachments == [attachment])
        #expect(refreshed.queue == [queued] && refreshed.queueHeld)
        await drafts.flush()
        #expect(ConversationDraftStore(directory: directory).drafts[session.id]?.queue == [queued])

        var repairedPairing = host
        repairedPairing.endpoint = URL(string: "https://replacement.invalid")!
        try await store.adoptHostedConversation(moved, connection: repairedPairing, conversationID: "host-thread")
        #expect(live.sessions[session.id] !== refreshed)
        #expect(live.sessions[session.id]?.draft == "Unsent draft")
    }
}
