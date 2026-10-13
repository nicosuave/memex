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

    @Test func typingIsDelayedButFlushIsDurableAndHoldAloneIsNotSaved() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = ConversationDraftStore(directory: directory, textDelay: .seconds(60))
        for index in 0..<50 { store.set(.init(text: "typing \(index)"), for: "typed") }
        try await Task.sleep(for: .milliseconds(50))
        #expect(!FileManager.default.fileExists(atPath: directory.appendingPathComponent("drafts.json").path))
        await store.flush()
        #expect(ConversationDraftStore(directory: directory).drafts["typed"]?.text == "typing 49")
        // Disconnecting a browsed conversation records an empty, held queue.
        store.set(.init(text: "", queueHeld: true), for: "browsed")
        #expect(store.drafts["browsed"] == nil)
        await store.flush()
        #expect(ConversationDraftStore(directory: directory).drafts["browsed"] == nil)
    }

    @Test func attachmentBytesAreStoredOnceByDigestAndLegacyFilesMigrate() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let bytes = Data((0..<4096).map { UInt8($0 % 251) })
        let attachment = ConversationAttachment(id: "captured", title: "Large", path: "/gone", content: bytes)
        let small = ConversationAttachment(id: "small", title: "Small", path: "/gone/small", content: Data([0, 255]))
        let queued = ConversationQueuedPrompt(.init(.prompt, text: "Later", attachments: [attachment], id: "queued"))
        let draft = ConversationDraftStore.Draft(text: "Unsent", attachments: [attachment, small], queue: [queued], queueHeld: true)
        let legacy = try JSONSerialization.jsonObject(with: JSONEncoder().encode(["session": draft]))
        let file = directory.appendingPathComponent("drafts.json")
        try JSONSerialization.data(withJSONObject: ["version": 1, "drafts": legacy]).write(to: file)

        let store = ConversationDraftStore(directory: directory)
        #expect(store.error == nil)
        #expect(store.drafts["session"] == draft)
        await store.flush()
        let saved = try String(contentsOf: file, encoding: .utf8)
        #expect(!saved.contains(bytes.base64EncodedString()))
        let blobs = directory.appendingPathComponent("Attachments")
        #expect(try FileManager.default.contentsOfDirectory(atPath: blobs.path).count == 1)
        #expect(ConversationDraftStore(directory: directory).drafts["session"] == draft)

        store.set(.init(text: ""), for: "session")
        await store.flush()
        #expect(try FileManager.default.contentsOfDirectory(atPath: blobs.path).isEmpty)
    }

    @Test func twoProcessesMergeTheirOwnDraftsUnderTheFileLock() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let first = ConversationDraftStore(directory: directory), second = ConversationDraftStore(directory: directory)
        first.set(.init(text: "first", deliveryUncertain: true), for: "one")
        second.set(.init(text: "second", deliveryUncertain: true), for: "two")
        await first.flush()
        await second.flush()
        first.set(.init(text: "first edited", deliveryUncertain: true), for: "one")
        await first.flush()
        let reopened = ConversationDraftStore(directory: directory)
        #expect(reopened.drafts["one"]?.text == "first edited")
        #expect(reopened.drafts["two"]?.text == "second")
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
