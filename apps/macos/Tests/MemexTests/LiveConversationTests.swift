import AppKit
import Foundation
import Testing
@testable import Memex

#if canImport(SQACPHost)
import SQACP
import SQACPHost
#endif

private func liveSession(source: String = "codex", root: URL = URL(fileURLWithPath: "/tmp/memex-resume-tests")) -> Session {
    let path = source == "codex" ? "sessions/2026/10/04/session.jsonl" : "projects/workspace/session.jsonl"
    return Session(source: source, sessionID: "native-session", sourcePath: root.appendingPathComponent(path).path,
                   project: "workspace", cwd: root.path, machine: "local")
}

private func fakeTarget(_ session: Session) -> InAppResumeTarget {
    InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
        workingDirectory: URL(fileURLWithPath: session.cwd!), providerHome: URL(fileURLWithPath: "/tmp/provider"),
        executableURL: URL(fileURLWithPath: "/bin/echo"), helperURL: nil, storageURL: URL(fileURLWithPath: "/tmp/runtime"))
}

private func liveMessage(_ text: String) -> Message {
    Message(role: "assistant", text: text, toolName: nil, toolInput: nil, toolOutput: nil)
}

private actor RecordingConversationRuntime: ConversationRuntime {
    var commands: [ConversationCommand] = []
    var failSend = false
    var stopped = false
    var connections = 0
    var readyOnConnect = true
    var holdPrompt = false
    private var promptWaiter: CheckedContinuation<Void, Never>?
    private var receive: (@Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void)?

    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) {
        self.receive = receive
        connections += 1
        receive(.success(ConversationSnapshot(connected: true, ready: readyOnConnect, canCancel: true)))
    }
    func perform(_ command: ConversationCommand) async throws {
        commands.append(command)
        if holdPrompt, command.action == .prompt { await withCheckedContinuation { promptWaiter = $0 } }
        if failSend { throw ConversationRuntimeError(message: "Acknowledgement lost") }
    }
    func disconnect() { stopped = true }
    func failNextSend() { failSend = true }
    func delayReadiness() { readyOnConnect = false }
    func holdNextPrompt() { holdPrompt = true }
    func acknowledgePrompt() { promptWaiter?.resume(); promptWaiter = nil }
    func emit(_ snapshot: ConversationSnapshot) { receive?(.success(snapshot)) }
    func confirm(_ command: ConversationCommand, messageID: String = "native-user", turnID: String = "native-turn") {
        let user = TranscriptRecord(recordID: "runtime:\(messageID)", record: Message(
            role: "user", text: command.text, toolName: nil, toolInput: nil, toolOutput: nil,
            eventID: messageID, sourceTurnID: turnID))
        receive?(.success(ConversationSnapshot(records: [user], connected: true, ready: true,
            deliveries: [.init(commandID: command.id, status: "completed", error: nil,
                               nativeTurnID: turnID, nativeMessageID: messageID)])))
    }
    func rejectOwnership() {
        receive?(.failure(ConversationRuntimeError(message: "Open elsewhere", kind: .openElsewhere)))
    }
    func rejectSettings() {
        receive?(.failure(ConversationRuntimeError(message: "Setting unavailable during this turn", kind: .settingsRejected)))
    }
}

@MainActor private func waitFor(_ predicate: () -> Bool) async throws {
    for _ in 0..<200 {
        if predicate() { return }
        try await Task.sleep(for: .milliseconds(5))
    }
    #expect(predicate(), "Timed out waiting for the conversation callback")
}

@Suite(.serialized) @MainActor struct LiveConversationTests {
    @Test func adoptsCreatedConversationWithoutSendingAnInitialPrompt() async throws {
        let runtime = RecordingConversationRuntime()
        let session = liveSession()
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let conversations = LiveConversations(drafts: ConversationDraftStore(directory: root))
        let conversation = await conversations.adopt(CreatedConversation(session: session, runtime: runtime, target: fakeTarget(session)))
        #expect(conversation.snapshot.ready)
        #expect(await runtime.connections == 1)
        #expect(await runtime.commands.isEmpty)
        conversation.draft = "First request"
        await conversation.send()
        #expect(await runtime.commands.count == 1)
        #expect(await runtime.commands.first?.text == "First request")
        await conversations.disconnectAll()
    }

    @Test func claudeProjectDirectoryMatchesNativeEncoding() {
        #expect(NewConversationRuntime.claudeProjectDirectory("/tmp/new_work.x") == "-tmp-new-work-x")
        #expect(NewConversationRuntime.claudeProjectDirectory("/tmp/😀") == "-tmp---")
        let longPath = "/" + String(repeating: "a", count: 201)
        #expect(NewConversationRuntime.claudeProjectDirectory(longPath) == "-" + String(repeating: "a", count: 199) + "-85qkr6")
    }

    @Test func draftPersistsBeforeDeliveryAndKeepsEditsMadeDuringSend() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let drafts = ConversationDraftStore(directory: directory)
        let driver = RecordingConversationRuntime()
        await driver.holdNextPrompt()
        let session = liveSession()
        let conversation = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: drafts)
        conversation.draft = "Original prompt"
        let send = Task { await conversation.send() }
        try await waitFor { conversation.submitting }
        await drafts.flush()
        let restoredDrafts = ConversationDraftStore(directory: directory)
        #expect(restoredDrafts.drafts[session.id]?.text == "")
        #expect(restoredDrafts.drafts[session.id]?.pendingPrompt?.text == "Original prompt")
        #expect(restoredDrafts.drafts[session.id]?.deliveryUncertain == true)
        let restored = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: restoredDrafts)
        #expect(!restored.canSubmit)
        #expect(restored.listState == .init(activity: .failed, hasDraft: true))
        conversation.draft = "New unsent edit"
        // Wait for the test transport to own the acknowledgement continuation.
        while await driver.commands.isEmpty { await Task.yield() }
        await driver.acknowledgePrompt()
        await send.value
        await drafts.flush()
        #expect(conversation.draft == "New unsent edit")
        #expect(conversation.pendingPrompt?.text == "Original prompt")
        #expect(!conversation.canSubmit)
        // A local queue receipt is insufficient. Only the native echo removes the intent.
        await driver.confirm(try #require(await driver.commands.first))
        try await waitFor { conversation.pendingPrompt == nil }
        await drafts.flush()
        #expect(ConversationDraftStore(directory: directory).drafts[session.id] == .init(text: "New unsent edit"))
        #expect(await driver.commands.map(\.text) == ["Original prompt"])
        await conversation.disconnect()
    }

    @Test func unreadableDraftStorePreventsProviderDeliveryAndPreservesPrompt() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let file = directory.appendingPathComponent("drafts.json")
        let original = Data("unreadable saved draft".utf8)
        try original.write(to: file)
        let driver = RecordingConversationRuntime()
        let drafts = ConversationDraftStore(directory: directory)
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver },
            resolveTarget: fakeTarget, drafts: drafts)
        conversation.draft = "Must remain unsent"
        await conversation.send()
        #expect(await driver.commands.isEmpty)
        #expect(conversation.draft == "Must remain unsent")
        #expect(conversation.pendingPrompt == nil)
        #expect(!conversation.isWorking)
        #expect(conversation.error?.contains("could not be saved") == true)
        #expect(try Data(contentsOf: file) == original)
        await conversation.disconnect()
    }

    @Test func uncertainDraftRequiresExplicitReloadAndNeverReplays() async throws {
        let drafts = ConversationDraftStore()
        let driver = RecordingConversationRuntime()
        let session = liveSession()
        let conversation = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: drafts)
        await driver.failNextSend()
        conversation.draft = "Unconfirmed prompt"
        await conversation.send()
        #expect(drafts.drafts[session.id]?.deliveryUncertain == true)
        await driver.emit(ConversationSnapshot(connected: true, ready: true))
        try await waitFor { conversation.snapshot.ready }
        #expect(drafts.drafts[session.id]?.deliveryUncertain == true)
        let restored = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: drafts)
        await restored.send()
        #expect(await driver.commands.count == 1)
        await restored.connect()
        #expect(!restored.canSubmit)
        #expect(restored.pendingPrompt?.text == "Unconfirmed prompt")
        restored.restorePendingDraft()
        #expect(restored.canSubmit)
        #expect(restored.draft == "Unconfirmed prompt")
        #expect(drafts.drafts[session.id]?.deliveryUncertain == false)
        #expect(await driver.commands.count == 1)
        await restored.disconnect()
        await conversation.disconnect()
    }

    @Test func pendingPromptRequiresItsOwnNativeAcknowledgementEvenForRepeatedText() async throws {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        conversation.draft = "Same message"
        await conversation.send()
        let command = try #require(await driver.commands.first)
        conversation.draft = "Next draft"
        let unrelated = TranscriptRecord(recordID: "older-message", record: Message(
            role: "user", text: "Same message", toolName: nil, toolInput: nil, toolOutput: nil,
            eventID: "other-user", sourceTurnID: "other-turn"))
        await driver.emit(ConversationSnapshot(records: [unrelated], connected: true, ready: true,
            deliveries: [.init(commandID: "older-command", status: "completed", error: nil,
                               nativeTurnID: "current-turn", nativeMessageID: "current-user")]))
        try await waitFor { conversation.snapshot.records.count == 1 }
        #expect(conversation.pendingPrompt?.commandID == command.id)
        #expect(!conversation.canSubmit)
        await driver.confirm(command, messageID: "current-user", turnID: "current-turn")
        try await waitFor { conversation.pendingPrompt == nil }
        #expect(conversation.draft == "Next draft")
        #expect(conversation.canSubmit)
        #expect(await driver.commands.count == 1)
        await conversation.disconnect()
    }

    @Test func nativeAcknowledgementClearsPendingWithoutTranscriptEchoAndKeepsSettingsAvailable() async throws {
        let drafts = ConversationDraftStore()
        let driver = RecordingConversationRuntime()
        let session = liveSession(source: "claude")
        let conversation = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: drafts)
        conversation.draft = "First prompt"
        await conversation.send()
        let command = try #require(await driver.commands.first)
        conversation.draft = "Next draft"
        for status in ["queued", "dispatching"] {
            await driver.emit(ConversationSnapshot(connected: true, ready: true, running: true,
                deliveries: [.init(commandID: command.id, status: status, error: nil,
                    nativeTurnID: "turn", nativeMessageID: "user")]))
            try await waitFor { conversation.snapshot.deliveries.first?.status == status }
            #expect(conversation.pendingPrompt != nil)
        }
        await driver.emit(ConversationSnapshot(connected: true, ready: true, running: true,
            deliveries: [.init(commandID: command.id, status: "completed", error: nil,
                nativeTurnID: "turn", nativeMessageID: "user")]))
        try await waitFor { conversation.pendingPrompt == nil }
        #expect(conversation.snapshot.records.isEmpty)
        #expect(conversation.canChangeSettings)
        #expect(conversation.draft == "Next draft")
        #expect(drafts.drafts[session.id]?.deliveryUncertain == false)
        #expect(await driver.commands.count == 1)
        await conversation.disconnect()
    }

    @Test func savedNativeAcknowledgementRecoversWithoutConnectingOrSendingHeldWork() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let drafts = ConversationDraftStore(directory: directory)
        let session = liveSession(source: "claude")
        let command = ConversationCommand(.prompt, text: "Already sent")
        var pending = ConversationPendingPrompt(command)
        pending.phase = .uncertain
        let queued = ConversationQueuedPrompt(ConversationCommand(.prompt, text: "Held queue"))
        drafts.set(.init(text: "Next draft", deliveryUncertain: true, pendingPrompt: pending,
            queue: [queued], queueHeld: true), for: session.id)
        await drafts.flush()
        let restored = ConversationDraftStore(directory: directory)
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: restored)
        await conversation.reconcileSavedDelivery { _, _ in false }
        #expect(conversation.pendingPrompt != nil)
        await conversation.reconcileSavedDelivery { suppliedSession, id in
            suppliedSession.id == session.id && id == command.id
        }
        #expect(conversation.pendingPrompt == nil)
        #expect(conversation.error == nil)
        #expect(conversation.canSubmit)
        #expect(conversation.draft == "Next draft")
        #expect(conversation.queue == [queued] && conversation.queueHeld)
        #expect(await driver.connections == 0)
        #expect(await driver.commands.isEmpty)
        await restored.flush()
        let final = ConversationDraftStore(directory: directory).drafts[session.id]
        #expect(final?.deliveryUncertain == false && final?.pendingPrompt == nil)
        #expect(final?.text == "Next draft" && final?.queue == [queued])
    }

    @Test func pendingConfigurationBlocksSendWithoutChangingTheDraft() async throws {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        await conversation.connect()
        var controls = ConversationControls(models: [.init(id: "provider-model", title: "Provider model")])
        await driver.emit(ConversationSnapshot(connected: true, ready: true, controls: controls))
        try await waitFor { conversation.snapshot.controls != nil }
        await conversation.setModel("invented-model")
        #expect(await driver.commands.isEmpty)
        let selecting = Task { await conversation.setModel("provider-model") }
        while await driver.commands.isEmpty { await Task.yield() }
        #expect(await driver.commands.map(\.action) == [.model])
        controls.pendingChanges = true
        await driver.emit(ConversationSnapshot(connected: true, ready: true, controls: controls))
        try await waitFor { !conversation.canSubmit }
        conversation.draft = "Wait for settings"
        await conversation.send()
        #expect(conversation.draft == "Wait for settings")
        #expect(await driver.commands.count == 1)
        controls.pendingChanges = false
        controls.selectedModelID = "provider-model"
        await driver.emit(ConversationSnapshot(connected: true, ready: true, controls: controls))
        await selecting.value
        try await waitFor { conversation.canSubmit }
        await conversation.send()
        #expect(await driver.commands.map(\.action) == [.model, .prompt])
        await conversation.disconnect()
    }

    @Test func settingsRemainAvailableDuringWorkAndApprovalWithoutSendingOrStopping() async throws {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(source: "claude"), makeRuntime: { driver }, resolveTarget: fakeTarget)
        await conversation.connect()
        let permission = ConversationControls.Configuration(id: "permission_mode", title: "Permissions", category: "mode",
            selectedID: "default", choices: [.init(id: "default", title: "Default"), .init(id: "auto", title: "Auto")])
        var controls = ConversationControls(models: [.init(id: "provider-model", title: "Provider model")], configurations: [permission])
        let approval = ConversationApproval(id: "tool", title: "Bash", detail: nil, options: [])
        var snapshot = ConversationSnapshot(connected: true, ready: true, running: true, approvals: [approval], controls: controls)
        await driver.emit(snapshot)
        try await waitFor { conversation.snapshot.running }
        conversation.draft = "Keep this draft"
        #expect(!conversation.canSend)
        #expect(conversation.canChangeSettings)
        let selectingModel = Task { await conversation.setModel("provider-model") }
        while await driver.commands.isEmpty { await Task.yield() }
        controls.selectedModelID = "provider-model"
        snapshot.controls = controls
        await driver.emit(snapshot)
        await selectingModel.value
        let selectingPermission = Task { await conversation.configure(permission, value: "auto") }
        while await driver.commands.count < 2 { await Task.yield() }
        controls.configurations[0] = .init(id: permission.id, title: permission.title,
            category: permission.category, selectedID: "auto", choices: permission.choices)
        snapshot.controls = controls
        await driver.emit(snapshot)
        await selectingPermission.value
        #expect(await driver.commands.map(\.action) == [.model, .configuration])
        #expect(conversation.snapshot.approvals == [approval])
        #expect(conversation.snapshot.running)
        #expect(conversation.draft == "Keep this draft")
        #expect(conversation.canChangeSettings)
        await conversation.disconnect()
    }

    @Test func rejectedSettingsKeepTheRunningConversationConnected() async throws {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(source: "claude"), makeRuntime: { driver }, resolveTarget: fakeTarget)
        await conversation.connect()
        await driver.emit(ConversationSnapshot(connected: true, ready: true, running: true,
            controls: ConversationControls(models: [.init(id: "provider-model", title: "Provider model")])))
        try await waitFor { conversation.snapshot.running }
        let selecting = Task { await conversation.setModel("provider-model") }
        while await driver.commands.isEmpty { await Task.yield() }
        await driver.rejectSettings()
        await selecting.value
        #expect(conversation.settingsError == "Setting unavailable during this turn")
        #expect(conversation.error == nil)
        #expect(conversation.snapshot.connected && conversation.snapshot.ready && conversation.snapshot.running)
        #expect(conversation.canChangeSettings)
        #expect(await driver.stopped == false)
        await conversation.disconnect()
    }

    #if canImport(SQACPHost)
    @Test func attachmentDraftSurvivesRelaunchAndSendsCapturedBytes() async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let file = root.appendingPathComponent("context.txt")
        try Data("original captured contents".utf8).write(to: file)
        let driver = RecordingConversationRuntime()
        let drafts = ConversationDraftStore(directory: root.appendingPathComponent("drafts"))
        let session = liveSession()
        let conversation = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: drafts)
        await conversation.connect()
        let controls = ConversationControls(supportsFileContents: true)
        await driver.emit(ConversationSnapshot(connected: true, ready: true, controls: controls))
        try await waitFor { conversation.snapshot.controls != nil }
        await conversation.attachFiles([file])
        #expect(conversation.attachmentError == nil)
        let captured = try #require(conversation.attachments.first)
        #expect(conversation.hasPrompt)
        await drafts.flush()
        await conversation.disconnect()
        try Data("new contents must not replace attachment".utf8).write(to: file)
        let restored = LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget,
            drafts: ConversationDraftStore(directory: root.appendingPathComponent("drafts")))
        #expect(restored.attachments == [captured])
        #expect(restored.listState.hasDraft)
        restored.draft = "Use the attached context"
        await restored.send()
        let command = try #require(await driver.commands.last)
        #expect(command.attachments == [captured])
        let encoded = try JSONDecoder().decode(AcpPromptContentBlock.self, from: captured.content)
        #expect(encoded == .resource(.init(resource: .blob(.init(uri: file.absoluteString,
            blob: Data("original captured contents".utf8).base64EncodedString(), mimeType: "text/plain")))))
        await driver.confirm(command)
        try await waitFor { restored.pendingPrompt == nil }
        #expect(restored.attachments.isEmpty)
        await restored.disconnect()
    }
    #endif

    @Test func retainedConversationStatusTracksWorkAttentionCompletionAndDraft() async throws {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        #expect(conversation.listState.label == nil)
        conversation.draft = "Prompt"
        #expect(conversation.listState == .init(hasDraft: true))
        await conversation.send()
        #expect(conversation.listState == .init(activity: .working))
        let approval = ConversationApproval(id: "approval", title: "Run", detail: "Full request", options: [])
        await driver.emit(ConversationSnapshot(connected: true, ready: true, running: true, approvals: [approval]))
        try await waitFor { conversation.listState.activity == .approval }
        let question = ConversationQuestion(id: "question", title: nil, prompt: "Which?", placeholder: nil, choices: [])
        await driver.emit(ConversationSnapshot(connected: true, ready: true, running: true, questions: [question]))
        try await waitFor { conversation.listState.activity == .question }
        await driver.emit(ConversationSnapshot(connected: true, ready: true))
        try await waitFor { conversation.listState.activity == .completed }
        conversation.draft = "Next prompt"
        #expect(conversation.listState == .init(activity: .completed, hasDraft: true))
        await conversation.disconnect()
    }

    #if canImport(SQACPHost)
    @Test func indexedAnchorWinsOverUnrelatedLiveIDsUntilSearchIsCleared() async throws {
        let driver = RecordingConversationRuntime()
        let registry = LiveConversations { session, drafts in
            LiveConversation(session: session, makeRuntime: { driver }, resolveTarget: fakeTarget, drafts: drafts)
        }
        var session = liveSession()
        session.searchRecordID = "rid1:exact-source-hit"
        registry.prepare(session)
        let store = Store(liveConversations: registry)
        store.sessions = [session]
        store.selectedID = session.id
        let conversation = try #require(store.selectedLiveConversation)
        conversation.draft = "Retained draft"
        await conversation.connect()
        #expect(store.readerUsesLiveSnapshot)
        store.query = "repeated text"
        let key = store.readerTranscriptKey
        #expect(!store.readerUsesLiveSnapshot)
        #expect(store.readerAnchorID == "rid1:exact-source-hit")
        await driver.emit(ConversationSnapshot(records: [.init(recordID: "runtime-unrelated", record: liveMessage("repeated text"))],
                                              connected: true, ready: true, running: true))
        try await waitFor { conversation.snapshot.running }
        #expect(store.readerTranscriptKey == key)
        #expect(!store.readerUsesLiveSnapshot)
        #expect(conversation.draft == "Retained draft")
        store.query = ""
        #expect(store.readerUsesLiveSnapshot)
        #expect(store.readerTranscriptKey != key)
        #expect(await driver.commands.isEmpty)
        await conversation.disconnect()
    }
    #endif

    @Test func externalOwnerBlocksLaunchThenUnlocksWithoutSendingTheDraft() async {
        let driver = RecordingConversationRuntime()
        var locked = true
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver },
            resolveTarget: fakeTarget, checkOwnership: { _ in locked })
        conversation.draft = "Keep this draft"
        #expect(conversation.isOpenElsewhere)
        #expect(!conversation.canSubmit)
        await conversation.send()
        #expect(await driver.connections == 0)
        locked = false
        conversation.refreshOwnership()
        #expect(conversation.canSubmit)
        #expect(conversation.draft == "Keep this draft")
        #expect(await driver.commands.isEmpty)
        await conversation.send()
        #expect(await driver.connections == 1)
        #expect(await driver.commands.map(\.text) == ["Keep this draft"])
        await conversation.disconnect()
    }

    @Test func sendRechecksOwnershipAfterSelection() async {
        let driver = RecordingConversationRuntime()
        var locked = false
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver },
            resolveTarget: fakeTarget, checkOwnership: { _ in locked })
        conversation.draft = "Keep this draft"
        #expect(conversation.canSubmit)
        locked = true
        await conversation.send()
        #expect(conversation.isOpenElsewhere)
        #expect(await driver.connections == 0)
        #expect(await driver.commands.isEmpty)
        #expect(conversation.draft == "Keep this draft")
    }

    @Test func writerConflictDuringLoadBecomesLockedAndFinishesThePendingSend() async throws {
        let driver = RecordingConversationRuntime()
        await driver.delayReadiness()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        conversation.draft = "Keep this draft"
        let sending = Task { await conversation.send() }
        try await waitFor { conversation.snapshot.connected }
        await driver.rejectOwnership()
        try await waitFor { conversation.isOpenElsewhere }
        await sending.value
        #expect(!conversation.isWorking)
        #expect(conversation.error == nil)
        #expect(conversation.draft == "Keep this draft")
        #expect(await driver.stopped)
        #expect(await driver.commands.isEmpty)
        await driver.emit(ConversationSnapshot(connected: true, ready: true))
        try await Task.sleep(for: .milliseconds(20))
        #expect(!conversation.snapshot.connected)
        conversation.refreshOwnership()
        #expect(conversation.canSubmit)
        #expect(await driver.commands.isEmpty)
    }

    @Test func ourConnectedProviderIsNotTreatedAsAnExternalOwner() async {
        let driver = RecordingConversationRuntime()
        var locked = false
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver },
            resolveTarget: fakeTarget, checkOwnership: { _ in locked })
        await conversation.connect()
        locked = true
        conversation.refreshOwnership()
        #expect(conversation.canSend)
        #expect(!conversation.isOpenElsewhere)
        await conversation.disconnect()
    }

    @Test func ownershipProbeFailureBlocksSendUntilItCanBeChecked() async {
        let driver = RecordingConversationRuntime()
        var fail = true
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver },
            resolveTarget: fakeTarget, checkOwnership: { _ in
                if fail { throw POSIXError(.EACCES) }
                return false
            })
        conversation.draft = "Keep this draft"
        await conversation.send()
        #expect(conversation.ownershipError != nil)
        #expect(!conversation.canSubmit)
        #expect(await driver.connections == 0)
        fail = false
        conversation.refreshOwnership()
        #expect(conversation.canSubmit)
        #expect(conversation.ownershipError == nil)
        #expect(conversation.draft == "Keep this draft")
        #expect(await driver.commands.isEmpty)
    }

    @Test func firstSendLoadsSessionAndSendsOnceWithoutASeparateResumeAction() async throws {
        let driver = RecordingConversationRuntime()
        await driver.delayReadiness()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        #expect(conversation.canSubmit)
        #expect(await driver.connections == 0)
        conversation.draft = "Continue here"
        let sending = Task { await conversation.send() }
        try await waitFor { conversation.snapshot.connected }
        #expect(!conversation.canSubmit)
        await conversation.send()
        #expect(await driver.commands.isEmpty)
        await driver.emit(ConversationSnapshot(connected: true, ready: true, canCancel: true))
        await sending.value
        #expect(await driver.connections == 1)
        #expect(await driver.commands.map(\.text) == ["Continue here"])
        #expect(conversation.draft.isEmpty)
        await conversation.disconnect()
    }

    @Test func stopWhileLoadingRetainsDraftAndAllowsAnExplicitRetry() async throws {
        let driver = RecordingConversationRuntime()
        await driver.delayReadiness()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        conversation.draft = "Keep this draft"
        let sending = Task { await conversation.send() }
        try await waitFor { conversation.snapshot.connected }
        #expect(conversation.isWorking)
        await conversation.stop()
        await sending.value
        await driver.emit(ConversationSnapshot(connected: true, ready: true, canCancel: true))
        #expect(await driver.commands.isEmpty)
        #expect(conversation.draft == "Keep this draft")
        #expect(conversation.canSubmit)
        #expect(!conversation.isWorking)
    }

    @Test func failedInitialConnectionRetainsDraftWithoutAutomaticRetry() async {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: { _ in
            throw ConversationRuntimeError(message: "Original workspace unavailable")
        })
        conversation.draft = "Keep this draft"
        await conversation.send()
        await conversation.send()
        #expect(conversation.draft == "Keep this draft")
        #expect(conversation.error == "Original workspace unavailable")
        #expect(!conversation.canSubmit)
        #expect(await driver.connections == 0)
        #expect(await driver.commands.isEmpty)
        conversation.refreshOwnership()
        #expect(!conversation.canSubmit)
        #expect(conversation.error == "Original workspace unavailable")
    }

    @Test func uncertainSendRetainsDraftAndDoesNotReplayOrAcceptStaleCallbacks() async throws {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        await conversation.connect()
        try await waitFor { conversation.canSend }
        await driver.failNextSend()
        conversation.draft = "Keep this prompt"
        await conversation.send()
        #expect(conversation.draft.isEmpty)
        #expect(conversation.pendingPrompt?.text == "Keep this prompt")
        #expect(conversation.pendingPrompt?.phase == .uncertain)
        #expect(!conversation.canSend)
        #expect(conversation.error == "Acknowledgement lost")
        conversation.refreshOwnership()
        #expect(!conversation.canSubmit)
        #expect(conversation.error == "Acknowledgement lost")
        await conversation.send()
        #expect(await driver.commands.count == 1)
        await conversation.disconnect()
        await driver.emit(ConversationSnapshot(connected: true, ready: true))
        try await Task.sleep(for: .milliseconds(20))
        #expect(!conversation.snapshot.connected)
        #expect(await driver.stopped)
        #expect(await driver.commands.count == 1)
    }

    @Test func sendIsSingleFlightAndStopAndInteractionRepliesUseOriginalIDs() async throws {
        let driver = RecordingConversationRuntime()
        let conversation = LiveConversation(session: liveSession(), makeRuntime: { driver }, resolveTarget: fakeTarget)
        await conversation.connect()
        try await waitFor { conversation.canSend }
        conversation.draft = "One prompt"
        await conversation.send()
        #expect(conversation.draft.isEmpty)
        #expect(!conversation.canSend)
        #expect(conversation.canStop)
        conversation.draft = "Second prompt"
        await conversation.send()
        #expect(await driver.commands.count == 1)
        await conversation.stop()
        let approval = ConversationApproval(id: "approval:original", title: "Run command", detail: nil,
            options: [.init(id: "deny", title: "Deny", kind: "reject_once")])
        let question = ConversationQuestion(id: "request::question", title: nil, prompt: "Which?", placeholder: nil, choices: [])
        await driver.emit(ConversationSnapshot(connected: true, ready: true, running: true, canCancel: true,
                                              approvals: [approval], questions: [question]))
        try await waitFor { conversation.snapshot.questions.count == 1 }
        #expect(!conversation.canSend)
        await conversation.approve(approval, option: approval.options[0])
        await conversation.answer(question, text: "Choice")
        let commands = await driver.commands
        #expect(commands.map(\.action) == [.prompt, .cancel, .approval, .userInput])
        #expect(commands[2].requestID == "approval:original")
        #expect(commands[2].text == "deny")
        #expect(commands[3].requestID == "request::question")
        #expect(Set(commands.map(\.id)).count == 4)
        await conversation.disconnect()
    }

    @Test func liveFindIncludesEarlierPagesAndPreservesSelectedOccurrence() {
        let client = MemexClient()
        let find = ConversationFindState(client: client)
        find.isOpen = true
        find.query = "needle"
        var records = (0..<250).map { index in
            TranscriptRecord(recordID: "\(index)", record: liveMessage(index == 3 ? "needle needle" : "message"))
        }
        find.search(records: records)
        find.move(1)
        records.append(TranscriptRecord(recordID: "250", record: liveMessage("needle")))
        find.search(records: records)
        #expect(find.hits.count == 3)
        #expect(find.selectedHit?.recordID == "3")
        #expect(find.selectedHit?.occurrence == 1)
    }

    @Test func streamingFollowsBottomButPreservesAnEarlierReadingPosition() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 300)
        controller.view.layoutSubtreeIfNeeded()
        var records = (0..<20).map { index in
            TranscriptRecord(recordID: "\(index)", record: liveMessage("Message \(index)"))
        }
        controller.update(sessionID: "live", records: records, provider: "codex", startsAtEnd: true, followLatest: true)
        let bottom = controller.scrollView.contentView.bounds.origin.y
        records.append(TranscriptRecord(recordID: "20", record: liveMessage("New message")))
        controller.update(sessionID: "live", records: records, provider: "codex", startsAtEnd: true, followLatest: true)
        #expect(controller.scrollView.contentView.bounds.origin.y > bottom)
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: 70))
        let earlier = controller.scrollView.contentView.bounds.origin.y
        records[20] = TranscriptRecord(recordID: "20", record: liveMessage("New message growing\n\nMore content"))
        controller.update(sessionID: "live", records: records, provider: "codex", startsAtEnd: true, followLatest: true)
        #expect(abs(controller.scrollView.contentView.bounds.origin.y - earlier) < 1)
    }
}

@Test func inAppResumeUsesSourceInstallationAndRejectsUnavailableWorkspaces() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    for provider in ["codex", "claude"] {
        let session = liveSession(source: provider, root: root.appendingPathComponent(provider))
        let source = URL(fileURLWithPath: session.sourcePath)
        try FileManager.default.createDirectory(at: source.deletingLastPathComponent(), withIntermediateDirectories: true)
        try Data("{}\n".utf8).write(to: source)
        let target = try InAppResumeTarget.resolve(session,
            environment: ["MEMEX_CODEX_EXECUTABLE": "/bin/echo", "MEMEX_CLAUDE_EXECUTABLE": "/bin/echo"],
            applicationSupport: root.appendingPathComponent("support"), helperURL: URL(fileURLWithPath: "/bin/echo"))
        #expect(target.providerHome.path == URL(fileURLWithPath: session.cwd!).resolvingSymlinksInPath().path)
        #expect(target.environment[provider == "codex" ? "CODEX_HOME" : "CLAUDE_CONFIG_DIR"] == target.providerHome.path)
        var missing = session
        missing.cwd = root.appendingPathComponent("missing").path
        #expect(throws: ConversationRuntimeError.self) { try InAppResumeTarget.resolve(missing) }
        var remote = session
        remote.machine = "nicbook-atm"
        #expect(InAppResumeTarget.unavailableReason(for: remote)?.contains("nicbook-atm") == true)
    }
}

@Test func conversationProjectionUsesPresentationAndOverlaysToolsByNativeIdentity() throws {
    let json = #"""
    {"presentation":[
      {"entity_id":"entry","body":{"kind":"entry","data":{"source_order":1,"payload":{"type":"entity","data":{"entity_id":"message"}}}}},
      {"entity_id":"message","body":{"kind":"message","data":{"role":"assistant","parts":[{"type":"text","data":"Hello"},{"type":"tool_call","data":{"invocation_id":"disk-call"}}]}}},
      {"entity_id":"disk-call","body":{"kind":"tool_invocation","data":{"native_call_id":"call-1","name":"exec_command","raw_arguments":"{}"}}}
    ],"persisted":[{"entity_id":"hidden","body":{"kind":"message","data":{"role":"assistant","parts":[{"type":"text","data":"MUST NOT DUPLICATE"}]}}}],
    "ephemeral":[{"item_id":"stream-call","source_order":2,"body":{"kind":"tool_invocation","data":{"native_call_id":"call-1","name":"exec_command","raw_arguments":"{\"cmd\":\"pwd\"}"}}}],
    "connected":true,"state":{"running":true,"pending_interactions":["approval-1","input-1"]}}
    """#
    let thread = #"""
    {"pendingRequests":[
      {"requestId":"approval-1","kind":"approval","payload":{"title":"Run command","options":[{"id":"deny","name":"Deny","kind":"reject_once"}]}},
      {"requestId":"input-1","kind":"user_input","payload":{"prompt":"Which?","choices":[{"id":"a","title":"A","value":"a"}]}},
      {"requestId":"stale","kind":"approval","payload":{"title":"Stale"}}
    ]}
    """#
    let snapshot = ConversationProjection.snapshot(try decodeJSON(json), thread: try decodeJSON(thread),
        operations: try decodeJSON(#"[{"command":{"type":"thread.turn.start"},"status":"dispatching"}]"#), ready: true, canCancel: true)
    #expect(snapshot.records.count == 2)
    #expect(snapshot.records[0].record.text == "Hello")
    #expect(snapshot.records[1].record.toolInput == #"{"cmd":"pwd"}"#)
    #expect(snapshot.approvals.map(\.id) == ["approval-1"])
    #expect(snapshot.questions.map(\.id) == ["input-1"])
    #expect(snapshot.pendingPrompt)
}

@Test func promptRecoveryWarningDistinguishesCurrentDeliveryFromAnInterruptedConnection() throws {
    let idle = try decodeJSON(#"{"connected":true,"state":{"running":false}}"#)
    for status in ["pending", "leased", "dispatching"] {
        let operations = try decodeJSON("""
        [{"command":{"commandId":"current-send","type":"thread.turn.start"},"status":"\(status)"}]
        """)
        let sending = ConversationProjection.snapshot(idle, operations: operations,
            ready: true, canCancel: true, sentPromptIDs: ["current-send"])
        #expect(sending.pendingPrompt)
        #expect(sending.warning == nil)
        // Reopening loses the connection's ownership of this still-pending send.
        let recovered = ConversationProjection.snapshot(idle, operations: operations, ready: true, canCancel: true)
        #expect(recovered.pendingPrompt)
        #expect(recovered.warning?.contains("previous prompt") == true)
        let active = ConversationProjection.snapshot(try decodeJSON(#"{"state":{"running":true}}"#),
            operations: operations, ready: true, canCancel: true)
        #expect(active.warning == nil)
    }
    let completed = ConversationProjection.snapshot(idle,
        operations: try decodeJSON(#"[{"command":{"type":"thread.turn.start"},"status":"completed"}]"#),
        ready: true, canCancel: true)
    #expect(!completed.pendingPrompt)
    #expect(completed.warning == nil)
}

@Test func inAppResumeKeepsOwningHomeWhenSessionStorageIsSymlinked() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    let home = root.appendingPathComponent("provider")
    let storage = root.appendingPathComponent("external-volume")
    try FileManager.default.createDirectory(at: home, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(at: storage, withIntermediateDirectories: true)
    try FileManager.default.createSymbolicLink(at: home.appendingPathComponent("sessions"), withDestinationURL: storage)
    try Data("{}\n".utf8).write(to: storage.appendingPathComponent("session.jsonl"))
    let session = Session(source: "codex", sessionID: "native", sourcePath: home.appendingPathComponent("sessions/session.jsonl").path,
                          project: "test", cwd: root.path, machine: "local")
    let target = try InAppResumeTarget.resolve(session, environment: ["MEMEX_CODEX_EXECUTABLE": "/bin/echo"], applicationSupport: root)
    #expect(target.providerHome == home.resolvingSymlinksInPath())
    #expect(target.sourceURL == storage.appendingPathComponent("session.jsonl").resolvingSymlinksInPath())
}

private func decodeJSON(_ json: String) throws -> RawTranscriptJSON {
    try JSONDecoder().decode(RawTranscriptJSON.self, from: Data(json.utf8))
}

#if canImport(SQACPHost)
@Test func codexUserMessageDoesNotDuplicateWhenItsSourceArrivesDuringTheTurn() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
    let source = root.appendingPathComponent("session.jsonl")
    // Codex's model-input message ID differs from its UI item ID. The native
    // completion event follows the recorded input before the turn completes.
    let json = #"""
    {"type":"session_meta","payload":{"id":"native-session","cwd":"/tmp"}}
    {"type":"event_msg","payload":{"type":"task_started","turn_id":"native-turn"}}
    {"type":"turn_context","payload":{"turn_id":"native-turn"}}
    {"type":"response_item","payload":{"id":"model-input-id","type":"message","role":"user","content":[{"type":"input_text","text":"A single sent message"}]},"metadata":{"user_input_order":0}}
    {"type":"event_msg","payload":{"type":"item_completed","thread_id":"native-session","turn_id":"native-turn","item":{"type":"UserMessage","id":"native-ui-id","client_id":"client-message-id","content":[{"type":"text","text":"A single sent message","text_elements":[]}]}}}
    """#
    try Data((json + "\n").utf8).write(to: source)
    let runtime = try AgentRuntimeClient(databaseURL: root.appendingPathComponent("runtime.sqlite"))
    let service = try AgentConversationService(runtime: runtime, archiveURL: root.appendingPathComponent("history"), executionHostID: "local")
    _ = try service.addSource(agent: "codex", format: "jsonl", url: source,
        nativeNamespace: "test-installation", nativeSessionID: "native-session", sessionID: "canonical-session")
    func request(_ method: String, _ params: [String: Any]) throws -> RawTranscriptJSON {
        let data = try JSONSerialization.data(withJSONObject: ["id": UUID().uuidString, "method": method, "params": params])
        let result = try decodeJSON(runtime.requestJSON(String(decoding: data, as: UTF8.self)))
        #expect(result["error"] == .null)
        return result["result"]
    }
    let connected = try request("conversation.connect", ["sessionId": "canonical-session", "protocol": "codex_app_server"])
    let token = try #require(connected["token"].string)
    let live = try request("conversation.apply", ["sessionId": "canonical-session", "token": token,
        "update": ["type": "snapshot", "through_sequence": 1, "final_item": true,
            "entity": ["item_id": "native-ui-id", "native_turn_id": "native-turn", "source_order": 1,
                "body": ["kind": "message", "data": ["role": "user", "native_message_id": "native-ui-id",
                    "parts": [["type": "text", "data": "A single sent message"]]]]]]])
    let snapshot = ConversationProjection.snapshot(live["conversation"], ready: true, canCancel: true,
        ownedLiveUserTurns: ["native-turn"])
    let prompts = snapshot.records.filter { $0.record.role == "user" }
    #expect(prompts.count == 1, "A persisted prompt and its live native item must occupy one row: \(prompts.map(\.id))")
    #expect(prompts.first?.record.eventID == "native-ui-id")
    // Unowned/resumed turns do not claim complete native user-message coverage.
    let unowned = ConversationProjection.snapshot(live["conversation"], ready: true, canCancel: true)
    #expect(unowned.records.filter { $0.record.role == "user" }.count == 2)
    // Presentation selection does not discard canonical evidence.
    #expect(live["conversation"]["persisted"].array.contains {
        $0["body"]["data"]["native_message_id"].string == "model-input-id"
    })

    // A second native user item can have identical text. Keep both occurrences
    // when steering adds another input to the same turn.
    let repeated = try request("conversation.apply", ["sessionId": "canonical-session", "token": token,
        "update": ["type": "snapshot", "through_sequence": 2, "final_item": true,
            "entity": ["item_id": "native-ui-repeat", "native_turn_id": "native-turn", "source_order": 2,
                "body": ["kind": "message", "data": ["role": "user", "native_message_id": "native-ui-repeat",
                    "parts": [["type": "text", "data": "A single sent message"]]]]]]])
    let repeatedSnapshot = ConversationProjection.snapshot(repeated["conversation"], ready: true, canCancel: true,
        ownedLiveUserTurns: ["native-turn"])
    let repeatedPrompts = repeatedSnapshot.records.filter { $0.record.role == "user" }
    #expect(repeatedPrompts.map(\.record.eventID) == ["native-ui-id", "native-ui-repeat"])

    let handle = try FileHandle(forWritingTo: source)
    try handle.seekToEnd()
    try handle.write(contentsOf: Data((#"{"type":"response_item","payload":{"id":"model-input-repeat","type":"message","role":"user","content":[{"type":"input_text","text":"A single sent message"}]}}"# + "\n").utf8))
    let refreshed = try service.refresh(sessionID: "canonical-session")
    let whileRunning = try #require(try ConversationProjection.conversation(in: refreshed, sessionID: "canonical-session"))
    let refreshedSnapshot = ConversationProjection.snapshot(whileRunning, ready: true, canCancel: true,
        ownedLiveUserTurns: ["native-turn"])
    #expect(refreshedSnapshot.records.filter { $0.record.role == "user" }.map(\.record.eventID)
        == ["native-ui-id", "native-ui-repeat"])
    try handle.write(contentsOf: Data((#"{"type":"event_msg","payload":{"type":"task_complete","turn_id":"native-turn"}}"# + "\n").utf8))
    try handle.close()
    let completedSource = try service.refresh(sessionID: "canonical-session")
    let complete = try #require(try ConversationProjection.conversation(in: completedSource, sessionID: "canonical-session"))
    let completedSnapshot = ConversationProjection.snapshot(complete, ready: true, canCancel: true,
        ownedLiveUserTurns: ["native-turn"])
    let savedPrompts = completedSnapshot.records.filter { $0.record.role == "user" }
    #expect(savedPrompts.map(\.record.eventID) == ["model-input-id", "model-input-repeat"])
    #expect(complete["ephemeral"].array.isEmpty)
}

@Test func attachmentProjectionUsesRetainedMetadataWithoutShowingTransportText() throws {
    let uri = "file:///private/attachment-store/captured.txt"
    let reference = "Read attached file context.txt: \(uri)"
    let attachment = AgentAttachmentProjection(id: "captured-content", kind: .file, name: "context.txt",
        mimeType: "text/plain", uri: uri, byteCount: 8, sourceURI: "file:///original/context.txt")
    let manifest = "Sidequery attachment metadata (v1):\n"
        + String(decoding: try JSONEncoder().encode([attachment]), as: UTF8.self)
    let content: [String: Any] = ["ephemeral": [["item_id": "native-message", "source_order": 1,
        "body": ["kind": "message", "data": ["role": "user", "native_message_id": "native-user",
            "parts": ["User caption", reference, reference, manifest].map { ["type": "text", "data": $0] }]]]]]
    let json = try JSONDecoder().decode(RawTranscriptJSON.self, from: JSONSerialization.data(withJSONObject: content))
    let snapshot = ConversationProjection.snapshot(json, ready: true, canCancel: true)
    let record = try #require(snapshot.records.first)
    #expect(snapshot.records.count == 1)
    // Keep an identical reference deliberately included in the user's prose.
    #expect(record.record.text == "User caption\n" + reference)
    #expect(record.record.eventID == "native-user")
    #expect(SourceContent.blocks(record.record) == [.attachment(label: "context.txt", source: uri, image: false)])
    #expect(record.rawTranscriptBody.contains("Sidequery attachment metadata"))
}

/// Opt-in native regression: creates a disposable conversation and samples the
/// handoff from local send intent through live echo and persisted transcript.
@Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_SEND_TRANSITION_CHECK"] != nil))
@MainActor func nativeSendShowsEachPromptOnceThroughoutDelivery() async throws {
    let directory = try #require(ProcessInfo.processInfo.environment["MEMEX_SEND_TRANSITION_CHECK"])
    let storage = FileManager.default.temporaryDirectory.appendingPathComponent("memex-send-transition-" + UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: storage) }
    let created = try await NewConversationRuntime.create(
        .init(provider: "codex", workingDirectory: URL(fileURLWithPath: directory)), applicationSupport: storage)
    let conversations = LiveConversations(drafts: ConversationDraftStore(directory: storage.appendingPathComponent("drafts")))
    let conversation = await conversations.adopt(created)
    let prompt = "Reply with exactly MEMEX_SEND_TRANSITION_OK. Do not use tools or change files."
    for sendNumber in 1...2 {
        conversation.draft = prompt
        let send = Task { await conversation.send() }
        var duplicateStates: Set<String> = []
        var completedAt: ContinuousClock.Instant?
        let deadline = ContinuousClock.now.advanced(by: .seconds(90))
        while ContinuousClock.now < deadline {
            if let error = conversation.error {
                await conversations.disconnectAll()
                throw ConversationRuntimeError(message: error)
            }
            let users = conversation.snapshot.records.filter {
                $0.record.role == "user" && $0.record.text.contains("MEMEX_SEND_TRANSITION_OK")
            }
            let pendingCount = conversation.pendingPrompt?.text == prompt ? 1 : 0
            if users.count + pendingCount > sendNumber {
                let identities = users.map {
                    "\($0.id) event=\($0.record.eventID ?? "nil") turn=\($0.record.sourceTurnID ?? "nil")"
                }.joined(separator: "; ")
                duplicateStates.insert("send=\(sendNumber) pending=\(pendingCount) users=[\(identities)] deliveries=\(conversation.snapshot.deliveries)")
            }
            if conversation.canSubmit && conversation.snapshot.records.filter({
                $0.record.role == "assistant" && $0.record.text == "MEMEX_SEND_TRANSITION_OK"
            }).count == sendNumber {
                completedAt = completedAt ?? .now
                if let completedAt, completedAt.duration(to: .now) > .seconds(2) { break }
            }
            try await Task.sleep(for: .milliseconds(10))
        }
        await send.value
        #expect(completedAt != nil, "Native send did not finish")
        #expect(duplicateStates.isEmpty, "Duplicate visible sends: \(duplicateStates.sorted())")
        #expect(conversation.snapshot.records.filter {
            $0.record.role == "user" && $0.record.text.contains("MEMEX_SEND_TRANSITION_OK")
        }.count == sendNumber)
    }
    await conversations.disconnectAll()
}

/// Resume-only diagnostic for a specified local session. Sends no prompt.
@Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_LOAD_TEST_SESSION"] != nil))
@MainActor func nativeRuntimeLoadsSpecifiedSession() async throws {
    let path = try #require(ProcessInfo.processInfo.environment["MEMEX_LOAD_TEST_SESSION"])
    let session = try JSONDecoder().decode(Session.self, from: Data(contentsOf: URL(fileURLWithPath: path)))
    let storage = FileManager.default.temporaryDirectory.appendingPathComponent("memex-load-" + UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: storage) }
    let conversation = LiveConversation(session: session, resolveTarget: {
        try InAppResumeTarget.resolve($0, applicationSupport: storage)
    })
    await conversation.connect()
    if let error = conversation.error {
        await conversation.disconnect()
        throw ConversationRuntimeError(message: error)
    }
    #expect(conversation.snapshot.ready)
    await conversation.disconnect()
}

/// Opt-in only: point at explicitly created disposable native sessions, never a
/// user's working conversation. This exercises the same actor as the app.
@Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_LIVE_TEST_SESSIONS"] != nil))
@MainActor func nativeRuntimeResumesDisposableProviderSessions() async throws {
    let environment = ProcessInfo.processInfo.environment
    let path = try #require(environment["MEMEX_LIVE_TEST_SESSIONS"])
    let sessions = try JSONDecoder().decode([Session].self, from: Data(contentsOf: URL(fileURLWithPath: path)))
    let storage = FileManager.default.temporaryDirectory.appendingPathComponent("memex-live-" + UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: storage) }
    for session in sessions {
        let conversation = LiveConversation(session: session, resolveTarget: {
            try InAppResumeTarget.resolve($0, applicationSupport: storage)
        })
        await conversation.connect()
        try await awaitLiveState(conversation) { $0.canSend }
        #expect(conversation.snapshot.records.contains { $0.record.role == "assistant" && $0.record.text.contains("MEMEX_SEED_OK") })
        conversation.draft = "This is a disposable Memex resume test. Reply exactly MEMEX_RESUMED_OK. Do not use tools."
        await conversation.send()
        try await awaitLiveState(conversation) {
            $0.canSend && $0.snapshot.records.contains { $0.record.role == "assistant" && $0.record.text.contains("MEMEX_RESUMED_OK") }
        }
        let source = try String(contentsOfFile: session.sourcePath, encoding: .utf8)
        #expect(source.contains("MEMEX_RESUMED_OK"))
        #expect(conversation.snapshot.records.filter { $0.record.role == "assistant" && $0.record.text.contains("MEMEX_RESUMED_OK") }.count == 1)
        await conversation.disconnect()
        await conversation.connect()
        try await awaitLiveState(conversation) { $0.canSend }
        #expect(conversation.snapshot.records.filter { $0.record.role == "assistant" && $0.record.text.contains("MEMEX_RESUMED_OK") }.count == 1)
        await conversation.disconnect()
    }
}

@MainActor private func awaitLiveState(_ conversation: LiveConversation, predicate: (LiveConversation) -> Bool) async throws {
    let deadline = ContinuousClock.now.advanced(by: .seconds(100))
    while ContinuousClock.now < deadline {
        if let error = conversation.error {
            await conversation.disconnect()
            throw ConversationRuntimeError(message: "\(conversation.session.source): \(error)")
        }
        if predicate(conversation) { return }
        try await Task.sleep(for: .milliseconds(100))
    }
    let status = conversation.status
    await conversation.disconnect()
    throw ConversationRuntimeError(message: "\(conversation.session.source): timed out in \(status)")
}

@Test func nativeRuntimeImportsAndProjectsRealProviderRecords() throws {
    for provider in ["codex", "claude"] {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let source = root.appendingPathComponent("session.jsonl")
        let json = provider == "codex" ? #"""
        {"type":"session_meta","payload":{"id":"native-session","cwd":"/tmp","originator":"codex_cli_rs","timestamp":"2026-10-04T10:00:00Z"}}
        {"type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"Hello native runtime"}]}}
        {"type":"response_item","payload":{"id":"assistant-1","type":"message","role":"assistant","content":[{"type":"output_text","text":"A persisted answer"}]}}
        {"type":"response_item","payload":{"id":"reasoning-1","type":"reasoning","summary":[],"encrypted_content":"ENCRYPTED_PAYLOAD"}}
        {"type":"response_item","payload":{"id":"image-1","type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a0FYAAAAASUVORK5CYII="}]}}
        """# : #"""
        {"type":"user","sessionId":"native-session","uuid":"user-1","message":{"role":"user","content":"Hello native runtime"}}
        {"type":"assistant","sessionId":"native-session","uuid":"assistant-1","parentUuid":"user-1","message":{"id":"api-1","role":"assistant","content":[{"type":"text","text":"A persisted answer"}]}}
        {"type":"assistant","sessionId":"native-session","uuid":"reasoning-1","message":{"id":"api-2","role":"assistant","content":[{"type":"redacted_thinking","data":"ENCRYPTED_PAYLOAD"}]}}
        {"type":"user","sessionId":"native-session","uuid":"image-1","message":{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+a0FYAAAAASUVORK5CYII="}}]}}
        """#
        try Data((json + "\n").utf8).write(to: source)
        let runtime = try AgentRuntimeClient(databaseURL: root.appendingPathComponent("runtime.sqlite"))
        let service = try AgentConversationService(runtime: runtime, archiveURL: root.appendingPathComponent("history"), executionHostID: "local")
        let imported = try service.addSource(agent: provider, format: "jsonl", url: source,
            nativeNamespace: "test-installation", nativeSessionID: "native-session", sessionID: "canonical-session")
        let conversation = try #require(try ConversationProjection.conversation(in: imported, sessionID: "canonical-session"))
        let snapshot = ConversationProjection.snapshot(conversation, ready: false, canCancel: false)
        let visible = TranscriptPresentation.project(snapshot.records)
        #expect(visible.map(\.record.text) == ["Hello native runtime", "A persisted answer", ""])
        #expect(snapshot.records.contains { $0.isRawOnly && $0.rawTranscriptBody.contains("ENCRYPTED_PAYLOAD") })
        let image = try #require(visible.last)
        #expect(SourceContent.blocks(image.record).count == 1)
        #expect(image.rawTranscriptBody.contains("iVBOR"))
        #expect(!visible.contains { $0.record.text.contains("ENCRYPTED_PAYLOAD") || $0.record.text.contains("iVBOR") })
        #expect(conversation["persisted"].array.contains { $0["body"]["kind"].string == "session" && !$0["body"]["data"]["source_ids"].array.isEmpty })
    }
}

@Test func createdConversationHistoryReadsNativeFilesBeforeIndexing() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    for provider in ["codex", "claude"] {
        let session = liveSession(source: provider, root: root.appendingPathComponent(provider))
        #expect(try await NewConversationRuntime.records(for: session).isEmpty)
        let source = URL(fileURLWithPath: session.sourcePath)
        try FileManager.default.createDirectory(at: source.deletingLastPathComponent(), withIntermediateDirectories: true)
        let json = provider == "codex" ? #"""
        {"type":"session_meta","payload":{"id":"native-session","cwd":"/tmp","originator":"codex_cli_rs","timestamp":"2026-10-04T10:00:00Z"}}
        {"type":"response_item","payload":{"id":"user-1","type":"message","role":"user","content":[{"type":"input_text","text":"Read without launching a provider"}]}}
        """# : #"""
        {"type":"user","sessionId":"native-session","uuid":"user-1","message":{"role":"user","content":"Read without launching a provider"}}
        """#
        try Data((json + "\n").utf8).write(to: source)
        let before = try Data(contentsOf: source)
        let records = try await NewConversationRuntime.records(for: session)
        #expect(records.contains { $0.record.text == "Read without launching a provider" })
        #expect(try Data(contentsOf: source) == before)
    }
}
#endif
