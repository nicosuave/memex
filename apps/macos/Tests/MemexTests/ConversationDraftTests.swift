import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor struct ConversationDraftTests {
    @Test func earlierDraftFormatStillDecodesWithoutAttachmentsOrPendingIntent() throws {
        let data = Data(#"{"text":"  Nice  ","deliveryUncertain":true}"#.utf8)
        let draft = try JSONDecoder().decode(ConversationDraftStore.Draft.self, from: data)
        #expect(draft == .init(text: "  Nice  ", deliveryUncertain: true))
        #expect(draft.attachments.isEmpty)
        #expect(draft.pendingPrompt == nil)
    }

    @Test func relaunchRestoresExactLatestDraftByNativeIdentity() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = ConversationDraftStore(directory: directory)
        #expect(store.error == nil)
        let local = Session(source: "codex", sessionID: "same", sourcePath: "/source", project: "one")
        var remote = local
        remote.machine = "nicbook-atm"
        for index in 0..<100 { store.set(.init(text: "draft \(index)"), for: local.id) }
        store.set(.init(text: "  Exact\ntext 🧪  ", deliveryUncertain: true), for: local.id)
        store.set(.init(text: "Different machine"), for: remote.id)
        await store.flush()
        let restored = ConversationDraftStore(directory: directory)
        #expect(restored.error == nil)
        #expect(restored.drafts[local.id] == .init(text: "  Exact\ntext 🧪  ", deliveryUncertain: true))
        #expect(restored.drafts[remote.id]?.text == "Different machine")
        let registry = LiveConversations(drafts: restored)
        #expect(registry.listState(for: local) == .init(activity: .failed, hasDraft: true))
        #expect(registry.listState(for: remote) == .init(hasDraft: true))
        restored.set(.init(text: ""), for: local.id)
        await restored.flush()
        #expect(ConversationDraftStore(directory: directory).drafts[local.id] == nil)
        let attributes = try FileManager.default.attributesOfItem(atPath: directory.appendingPathComponent("drafts.json").path)
        #expect((attributes[.posixPermissions] as? NSNumber)?.intValue == 0o600)
    }

    @Test func unreadableSavedDraftsAreNeverOverwritten() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let file = directory.appendingPathComponent("drafts.json")
        let original = Data("unreadable saved content".utf8)
        try original.write(to: file)
        let store = ConversationDraftStore(directory: directory)
        #expect(store.error != nil)
        store.set(.init(text: "new draft"), for: "session")
        await store.flush()
        #expect(store.drafts["session"]?.text == "new draft")
        #expect(try Data(contentsOf: file) == original)
    }

    @Test func openingAViewerPersistsCompleteRecoveredIntentWithoutAnotherEdit() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = ConversationDraftStore(directory: directory)
        let session = Session(source: "codex", sessionID: "saved-intent", sourcePath: "/saved/native.jsonl", project: "saved")
        let attachment = ConversationAttachment(id: "captured", title: "Evidence", path: "/gone/file", content: Data([0, 1, 255]))
        var pending = ConversationPendingPrompt(.init(.prompt, text: "Unconfirmed", attachments: [attachment], id: "pending"))
        pending.phase = .awaitingConfirmation
        let queued = ConversationQueuedPrompt(.init(.prompt, text: "Later", attachments: [attachment], id: "queued"))
        store.set(.init(text: "Unsent", deliveryUncertain: true, attachments: [attachment],
                        pendingPrompt: pending, queue: [queued]), for: session.id)
        await store.flush()

        let live = LiveConversation(session: session, checkOwnership: { _ in false }, drafts: store)
        #expect(!live.snapshot.connected)
        await store.flush()
        pending.phase = .uncertain
        let recovered = try #require(ConversationDraftStore(directory: directory).drafts[session.id])
        #expect(recovered == .init(text: "Unsent", deliveryUncertain: true, attachments: [attachment],
                                  pendingPrompt: pending, queue: [queued], queueHeld: true))
    }
}
