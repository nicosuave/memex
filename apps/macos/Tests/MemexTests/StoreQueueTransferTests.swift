import Foundation
import Testing
@testable import Memex

private actor TransferRuntime: ConversationRuntime {
    private(set) var commands: [ConversationCommand] = []
    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) {
        receive(.success(.init(connected: true, ready: true)))
    }
    func perform(_ command: ConversationCommand) { commands.append(command) }
    func disconnect() {}
    nonisolated func created(_ session: Session, directory: URL) -> CreatedConversation {
        .init(session: session, runtime: self, target: .init(session: session,
            sourceURL: URL(fileURLWithPath: session.sourcePath), workingDirectory: directory,
            providerHome: directory.appendingPathComponent("provider"), executableURL: URL(fileURLWithPath: "/bin/echo"),
            helperURL: nil, storageURL: directory.appendingPathComponent("runtime")))
    }
}

private actor TransferFactory {
    private(set) var requests: [NewConversationRequest] = []
    let runtime = TransferRuntime()
    let session: Session
    let directory: URL
    var failCreation: Bool
    let blockDraftDirectory: URL?
    init(session: Session, directory: URL, failCreation: Bool = false, blockDraftDirectory: URL? = nil) {
        self.session = session
        self.directory = directory
        self.failCreation = failCreation
        self.blockDraftDirectory = blockDraftDirectory
    }
    func create(_ request: NewConversationRequest) throws -> CreatedConversation {
        requests.append(request)
        if failCreation { throw ConversationRuntimeError(message: "Provider response lost after creation") }
        if let blockDraftDirectory {
            try FileManager.default.removeItem(at: blockDraftDirectory)
            try FileManager.default.createDirectory(at: blockDraftDirectory, withIntermediateDirectories: true)
        }
        return runtime.created(session, directory: directory)
    }
}

@Suite(.serialized) @MainActor struct StoreQueueTransferTests {
    private func root() throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.resolvingSymlinksInPath()
    }
    private func session(_ id: String, root: URL, machine: String = "local") -> Session {
        .init(source: "codex", sessionID: id,
            sourcePath: root.appendingPathComponent("provider/sessions/\(id).jsonl").path,
            project: "Fixture", cwd: root.path, machine: machine)
    }
    private func entry() -> ConversationQueuedPrompt {
        .init(.init(.prompt, text: "Exact reviewed prompt", attachments: [
            .init(id: "captured-id", title: "Original attachment", path: "/source/changed-later", content: Data([0, 255, 12]))
        ], id: "queued-command-id"))
    }
    private func store(root: URL, factory: TransferFactory) -> Store {
        Store(draftStore: ConversationDraftStore(directory: root.appendingPathComponent("drafts")),
            createdConversations: CreatedConversationCatalog(directory: root.appendingPathComponent("catalog")),
            conversationRelationships: ConversationRelationships(directory: root.appendingPathComponent("relationships")),
            newConversationDraft: NewConversationDraft(directory: root.appendingPathComponent("home")),
            makeConversation: { try await factory.create($0) })
    }

    @Test func durableTransferLeavesHomeUntouchedAndRetryPreservesSideChatEdits() async throws {
        let root = try root()
        defer { try? FileManager.default.removeItem(at: root) }
        let source = session("source", root: root)
        let child = session("child", root: root)
        let factory = TransferFactory(session: child, directory: root)
        let store = store(root: root, factory: factory)
        store.newConversationDraft.value.text = "Unrelated Home work"
        store.newConversationDraft.value.provider = "claude"
        await store.newConversationDraft.flush()
        let home = store.newConversationDraft.value
        let entry = entry()
        try await store.transferQueuedPrompt(source: source, entry: entry)
        let saved = try #require(ConversationDraftStore(directory: root.appendingPathComponent("drafts")).drafts[child.id])
        #expect(saved.text == entry.text)
        #expect(saved.attachments == entry.attachments)
        #expect(store.newConversationDraft.value == home)
        #expect(NewConversationDraft(directory: root.appendingPathComponent("home")).value == home)
        #expect(store.conversationRelationships.parent(of: child.id)?.parent.id == source.id)
        let request = try #require(await factory.requests.first)
        #expect(request.provider == source.source)
        let requestedHome = try #require(request.providerHome)
        #expect(requestedHome.resolvingSymlinksInPath().path == root.appendingPathComponent("provider").resolvingSymlinksInPath().path)
        #expect(request.workingDirectory == root)
        #expect(await factory.runtime.commands.isEmpty)

        let edited = ConversationDraftStore.Draft(text: "Edited after transfer", attachments: entry.attachments)
        store.liveConversations.drafts.set(edited, for: child.id)
        await store.liveConversations.drafts.flush()
        let reopened = self.store(root: root, factory: factory)
        try await reopened.transferQueuedPrompt(source: source, entry: entry)
        #expect(await factory.requests.count == 1)
        #expect(reopened.liveConversations.drafts.drafts[child.id] == edited)
        #expect(await factory.runtime.commands.isEmpty)
        await store.liveConversations.disconnectAll()
    }

    @Test func unknownCreationOutcomeCannotCreateAgainAfterRelaunch() async throws {
        let root = try root()
        defer { try? FileManager.default.removeItem(at: root) }
        let source = session("source", root: root)
        let factory = TransferFactory(session: session("child", root: root), directory: root, failCreation: true)
        let entry = entry()
        do { try await store(root: root, factory: factory).transferQueuedPrompt(source: source, entry: entry); Issue.record("Expected creation failure") }
        catch {}
        let reopened = store(root: root, factory: factory)
        do { try await reopened.transferQueuedPrompt(source: source, entry: entry); Issue.record("Uncertain creation was replayed") }
        catch {}
        #expect(await factory.requests.count == 1)
        #expect(reopened.conversationRelationships.queueTransfers.first?.entry == entry)
        #expect(reopened.conversationRelationships.queueTransfers.first?.result == nil)
        #expect(await factory.runtime.commands.isEmpty)
    }

    @Test func failedDraftSaveRecoversNativeIdentityWithoutAnotherCreation() async throws {
        let root = try root()
        defer { try? FileManager.default.removeItem(at: root) }
        let source = session("source", root: root)
        let child = session("child", root: root)
        let blocker = root.appendingPathComponent("drafts/drafts.json")
        let factory = TransferFactory(session: child, directory: root, blockDraftDirectory: blocker)
        let first = store(root: root, factory: factory)
        let entry = entry()
        do { try await first.transferQueuedPrompt(source: source, entry: entry); Issue.record("Expected save failure") }
        catch {}
        #expect(first.conversationRelationships.queueTransfers.first?.result?.id == child.id)
        #expect(first.conversationRelationships.queueTransfers.first?.completed == false)
        try FileManager.default.removeItem(at: blocker)
        let reopened = store(root: root, factory: factory)
        try await reopened.transferQueuedPrompt(source: source, entry: entry)
        #expect(await factory.requests.count == 1)
        #expect(reopened.liveConversations.drafts.drafts[child.id]?.attachments == entry.attachments)
        #expect(reopened.conversationRelationships.queueTransfers.first?.completed == true)
        #expect(await factory.runtime.commands.isEmpty)
    }

    @Test func remoteSourceNeverCreatesAgainstItsMatchingLocalPath() async throws {
        let root = try root()
        defer { try? FileManager.default.removeItem(at: root) }
        let source = session("remote", root: root, machine: "nicbook-atm")
        let factory = TransferFactory(session: session("child", root: root), directory: root)
        let store = store(root: root, factory: factory)
        do { try await store.transferQueuedPrompt(source: source, entry: entry()); Issue.record("Remote path used locally") }
        catch {}
        #expect(await factory.requests.isEmpty)
        #expect(store.conversationRelationships.queueTransfers.isEmpty)
    }
    @Test func queueRemovalFollowsDurableStoreTransferWithoutDispatch() async throws {
        let root = try root()
        defer { try? FileManager.default.removeItem(at: root) }
        let source = session("source", root: root)
        let child = session("child", root: root)
        let factory = TransferFactory(session: child, directory: root)
        let store = store(root: root, factory: factory)
        let parentRuntime = TransferRuntime()
        let parent = await store.liveConversations.adopt(parentRuntime.created(source, directory: root))
        parent.onTransferQueuedPrompt = { source, entry in try await store.transferQueuedPrompt(source: source, entry: entry) }
        parent.holdQueue()
        let original = entry()
        #expect(parent.replaceDraft(text: original.text, attachments: original.attachments))
        await parent.enqueueDraft()
        let queued = try #require(parent.queue.first)
        await parent.transferQueuedPrompt(id: queued.id)
        #expect(parent.queue.isEmpty)
        let persisted = ConversationDraftStore(directory: root.appendingPathComponent("drafts"))
        #expect(persisted.drafts[source.id]?.queue.isEmpty == true)
        #expect(persisted.drafts[child.id]?.attachments == queued.attachments)
        #expect(persisted.drafts[child.id]?.text == queued.text)
        #expect(await parentRuntime.commands.isEmpty)
        #expect(await factory.runtime.commands.isEmpty)
        await store.liveConversations.disconnectAll()
    }

}
