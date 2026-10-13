import Foundation
import Testing
@testable import Memex

private actor HistoryFixtureRuntime: ConversationRuntime {
    let records: [TranscriptRecord]
    var commands: [ConversationCommand] = []

    init(records: [TranscriptRecord] = []) { self.records = records }
    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) {
        receive(.success(ConversationSnapshot(records: records, connected: true, ready: true,
            controls: ConversationControls(supportsFileContents: true))))
    }
    func perform(_ command: ConversationCommand) { commands.append(command) }
    func disconnect() {}

    nonisolated func created(_ session: Session, directory: URL) -> CreatedConversation {
        CreatedConversation(session: session, runtime: self,
            target: InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
                workingDirectory: directory, providerHome: directory, executableURL: URL(fileURLWithPath: "/bin/echo"),
                helperURL: nil, storageURL: directory.appendingPathComponent("runtime")))
    }
}

@Suite @MainActor struct ConversationHistoryTests {
    private func session(_ id: String, source: String = "codex", directory: URL) -> Session {
        Session(source: source, sessionID: id, sourcePath: directory.appendingPathComponent(id + ".jsonl").path,
                project: "Fixture", cwd: directory.path)
    }
    private func record(_ id: String, turn: String?, text: String) -> TranscriptRecord {
        var message = Message(role: "user", text: text, toolName: nil, toolInput: nil, toolOutput: nil, eventID: id)
        message.sourceTurnID = turn
        return TranscriptRecord(recordID: id, record: message)
    }

    @Test func boundariesPreserveNativeIdentityAndCollapseSameTurnChunks() {
        let records = [record("first", turn: "one", text: "Question"),
                       record("first-part", turn: "one", text: "More context"),
                       record("second", turn: "two", text: "Follow up")]
        let choices = ConversationHistoryBoundary.choices(in: records)
        #expect(choices.map(\.userMessageID) == ["first", "second"])
        #expect(choices.first?.nextUserMessageID == "second")
        #expect(choices.last?.throughEnd == true)
        let imported = TranscriptRecord(recordID: "history", record: Message(role: "user", text: "Unknown boundary",
            toolName: nil, toolInput: nil, toolOutput: nil))
        #expect(ConversationHistoryBoundary.choices(in: [imported]).isEmpty)
    }

    @Test func pendingNativeMutationSurvivesReopenAndCannotBeRepeated() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let source = session("source", directory: directory)
        let child = session("child", directory: directory)
        let first = ConversationRelationships(directory: directory)
        let request = try first.begin(source: source, operation: "fork")
        try first.recordResult(child, for: request)
        let reopened = ConversationRelationships(directory: directory)
        #expect(reopened.pending.first?.result == child)
        #expect(throws: ConversationRuntimeError.self) { try reopened.begin(source: source, operation: "fork") }
        try reopened.finish(request, link: .init(parent: source, child: child, kind: .nativeFork,
                                                 sourceRecordID: "record", createdAt: Date()))
        let finished = ConversationRelationships(directory: directory)
        #expect(finished.pending.isEmpty)
        #expect(finished.parent(of: child.id)?.parent == source)
        #expect(finished.children(of: source.id).map(\.child) == [child])
    }

    @Test func corruptRelationshipJournalIsNeverReplaced() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let file = directory.appendingPathComponent("relationships.json")
        let bytes = Data("unreadable original".utf8)
        try bytes.write(to: file)
        let relationships = ConversationRelationships(directory: directory)
        #expect(relationships.error != nil)
        #expect(throws: (any Error).self) { try relationships.begin(source: session("source", directory: directory), operation: "fork") }
        #expect(try Data(contentsOf: file) == bytes)
    }

    @Test func providerTransitionCapturesContextIntoUnsentDraftAndMergePreservesParentDraft() async throws {
        guard InAppAgentRuntime.isAvailable else { return }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let source = session("source", directory: directory)
        let child = session("child", source: "claude", directory: directory)
        let parentRuntime = HistoryFixtureRuntime(records: [record("native-record", turn: "turn", text: "Original question")])
        let childRuntime = HistoryFixtureRuntime(records: [record("child-record", turn: "child-turn", text: "Branch findings")])
        let store = Store(draftStore: ConversationDraftStore(directory: directory.appendingPathComponent("drafts")),
            makeConversation: { request in
                #expect(request.provider == "claude")
                return childRuntime.created(child, directory: request.workingDirectory)
            })
        let parent = await store.liveConversations.adopt(parentRuntime.created(source, directory: directory))
        parent.draft = "Keep my unfinished prompt"
        let plan = ConversationWork.Plan(recordID: "plan-record", steps: [.init(id: 0, text: "Implement the requirement", status: "pending")])
        let result = try await store.branchWithContext(source: source, provider: "claude", isolatedWorkspace: false, plan: plan)
        #expect(result.id == child.id)
        #expect(store.conversationRelationships.parent(of: child.id)?.kind == .providerTransition)
        let branch = try #require(store.liveConversations.sessions[child.id])
        #expect(branch.attachments.count == 2)
        #expect(String(decoding: branch.attachments[0].content, as: UTF8.self).contains("Original question"))
        let capturedPlan = try #require(JSONSerialization.jsonObject(with: branch.attachments[1].content) as? [String: String])
        #expect(capturedPlan["type"] == "text")
        #expect(capturedPlan["text"]?.hasSuffix(plan.text) == true)
        #expect(capturedPlan["text"]?.contains("#plan-record") == true)
        #expect(branch.draft == "Implement the attached plan.")
        #expect(await childRuntime.commands.isEmpty)
        try await store.mergeContextToParent(from: child)
        #expect(parent.draft == "Keep my unfinished prompt")
        #expect(parent.attachments.count == 1)
        #expect(String(decoding: parent.attachments[0].content, as: UTF8.self).contains("Branch findings"))
        #expect(await parentRuntime.commands.isEmpty)
        await store.liveConversations.disconnectAll()
    }

    @Test func hostedSessionDoesNotExposeLocalWorkspaceOrEndOnQuit() async throws {
        guard InAppAgentRuntime.isAvailable else { return }
        let directory = FileManager.default.temporaryDirectory
        let row = session("hosted", directory: directory)
        let runtime = HistoryFixtureRuntime()
        let hosted = LiveConversation(session: row, makeRuntime: { runtime },
            resolveTarget: { _ in runtime.created(row, directory: directory).target },
            checkOwnership: { _ in false }, isServerOwned: true)
        let collection = LiveConversations(makeConversation: { _, _ in hosted })
        collection.prepare(row)
        #expect(collection.sessions[row.id]?.isServerOwned == true)
        let store = Store(liveConversations: collection)
        store.sessions = [row]; store.scope = .all; store.selectedID = row.id
        #expect(!store.canAccessLocalFiles(for: row))
        #expect(store.selectedWorkspace == nil)
        #expect(collection.workEndingOnQuit.isEmpty)
    }
}
