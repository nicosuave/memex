import Foundation
import Testing
@testable import Memex

private actor QueueRuntime: ConversationRuntime {
    private(set) var commands: [ConversationCommand] = []
    private var receive: (@Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void)?
    var rejectStop = false
    private var holdPrompt = false
    private var promptWaiter: CheckedContinuation<Void, Never>?
    private var mutationWaiter: CheckedContinuation<Void, Never>?
    private var session: Session?
    private(set) var mutationCount = 0
    private(set) var disconnectCount = 0

    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) {
        self.receive = receive
        session = target.session
        receive(.success(.init(connected: true, ready: true, canCancel: true, canSteer: true)))
    }
    func perform(_ command: ConversationCommand) async throws {
        commands.append(command)
        if holdPrompt && command.action == .prompt { await withCheckedContinuation { promptWaiter = $0 } }
        if command.action == .cancel && rejectStop { throw ConversationRuntimeError(message: "Interrupt rejected") }
    }
    func mutateHistory(_ request: ConversationHistoryMutation) async throws -> Session {
        mutationCount += 1
        await withCheckedContinuation { mutationWaiter = $0 }
        return try #require(session)
    }
    func disconnect() { disconnectCount += 1 }
    func emit(_ snapshot: ConversationSnapshot) { receive?(.success(snapshot)) }
    func rejectCancellation() { rejectStop = true }
    func suspendPrompt() { holdPrompt = true }
    func releasePrompt() { promptWaiter?.resume(); promptWaiter = nil }
    func releaseMutation() { mutationWaiter?.resume(); mutationWaiter = nil }
}

@MainActor private final class CheckpointGate {
    private(set) var arrivals = 0
    private(set) var finished: Set<Int> = []
    private var waiters: [Int: CheckedContinuation<Void, Never>] = [:]

    func capture() async {
        let index = arrivals
        arrivals += 1
        await withCheckedContinuation { waiters[index] = $0 }
        finished.insert(index)
    }

    func release(_ index: Int) { waiters.removeValue(forKey: index)?.resume() }
}

@Suite(.serialized) @MainActor struct ConversationQueueTests {
    private var session: Session {
        Session(source: "codex", sessionID: "queue-session", sourcePath: "/tmp/queue/sessions/native.jsonl",
                project: "queue", cwd: "/tmp", machine: "local")
    }

    private func conversation(_ runtime: QueueRuntime, drafts: ConversationDraftStore = ConversationDraftStore(),
                              timeout: Duration = .seconds(15)) -> LiveConversation {
        LiveConversation(session: session, makeRuntime: { runtime }, resolveTarget: { session in
            InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
                workingDirectory: URL(fileURLWithPath: "/tmp"), providerHome: URL(fileURLWithPath: "/tmp/queue"),
                executableURL: URL(fileURLWithPath: "/bin/echo"), helperURL: nil,
                storageURL: URL(fileURLWithPath: "/tmp/queue-runtime"))
        }, checkOwnership: { _ in false }, drafts: drafts, stopTimeoutDuration: timeout)
    }

    private func wait(_ predicate: () -> Bool) async throws {
        for _ in 0..<200 {
            if predicate() { return }
            try await Task.sleep(for: .milliseconds(5))
        }
        #expect(predicate())
    }

    @Test func queueEditsAndOrderSurviveRelaunchWithoutDispatch() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = ConversationDraftStore(directory: directory)
        let runtime = QueueRuntime()
        let live = conversation(runtime, drafts: store)
        live.draft = "First"
        await live.enqueueDraft()
        live.draft = "Second"
        await live.enqueueDraft()
        let firstID = try #require(live.queue.first?.id)
        let secondID = try #require(live.queue.last?.id)
        live.editQueued(id: secondID, text: "Edited second")
        live.moveQueued(id: secondID, offset: -1)
        await store.flush()
        let restored = conversation(runtime, drafts: ConversationDraftStore(directory: directory))
        #expect(restored.queue.map(\.id) == [secondID, firstID])
        #expect(restored.queue.map(\.text) == ["Edited second", "First"])
        #expect(restored.queueHeld)
        #expect(await runtime.commands.isEmpty)
        await restored.connect()
        #expect(await runtime.commands.isEmpty)
        restored.cancelQueued(id: firstID)
        await restored.resumeQueue()
        #expect(await runtime.commands.map(\.id) == [secondID])
        #expect(restored.pendingPrompt?.commandID == secondID)
        await restored.disconnect()
    }

    @Test func stopHoldsQueueAndSettlesOnlyAfterTerminalSnapshot() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        await live.connect()
        await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true, canSteer: true))
        try await wait { live.snapshot.running }
        live.draft = "Next turn"
        await live.enqueueDraft()
        await live.stop()
        #expect(live.status == "Stopping…")
        #expect(live.queueHeld)
        await runtime.emit(.init(connected: true, ready: true, canCancel: true, canSteer: true))
        try await wait { live.status == "Ready" }
        #expect(live.completion == .stopped)
        #expect(live.queue.count == 1)
        #expect(await runtime.commands.map(\.action) == [.cancel])
        await live.resumeQueue()
        #expect(await runtime.commands.map(\.action) == [.cancel, .prompt])
        await live.disconnect()
    }

    @Test func rejectedStopAndMissingTerminalStateReportFailureWithoutDraining() async throws {
        for reject in [false, true] {
            let runtime = QueueRuntime()
            if reject { await runtime.rejectCancellation() }
            let live = conversation(runtime, timeout: .milliseconds(20))
            await live.connect()
            await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true))
            try await wait { live.snapshot.running }
            live.draft = "Held"
            await live.enqueueDraft()
            await live.stop()
            try await wait { live.error != nil }
            #expect(live.status != "Stopping…")
            #expect(live.queueHeld)
            #expect(live.queue.count == 1)
            #expect(await runtime.commands.map(\.action) == [.cancel])
            await live.disconnect()
        }
    }

    @Test func steerKeepsCommandIdentityAndDoesNotMistakeEarlierSameTurnEchoForAcceptance() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        await live.connect()
        await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true, canSteer: true))
        try await wait { live.canSteer }
        live.draft = "Steering input"
        await live.steerDraft()
        let command = try #require(await runtime.commands.first)
        #expect(command.action == .steer)
        let earlier = TranscriptRecord(recordID: "earlier", record: Message(role: "user", text: "Earlier input",
            toolName: nil, toolInput: nil, toolOutput: nil, eventID: "earlier", sourceTurnID: "native-turn"))
        await runtime.emit(.init(records: [earlier], connected: true, ready: true, running: true,
            canCancel: true, canSteer: true, deliveries: [.init(commandID: command.id, status: "dispatching",
                error: nil, nativeTurnID: "native-turn", nativeMessageID: nil)]))
        try await wait { live.snapshot.records.count == 1 }
        #expect(live.pendingPrompt?.commandID == command.id)
        await runtime.emit(.init(records: [earlier], connected: true, ready: true, running: true,
            canCancel: true, canSteer: true, deliveries: [.init(commandID: command.id, status: "completed",
                error: nil, nativeTurnID: "native-turn", nativeMessageID: nil)]))
        try await wait { live.pendingPrompt == nil }
        #expect(await runtime.commands.count == 1)
        await live.disconnect()
    }

    @Test func unsupportedSteerLeavesDraftAndQueueUnchanged() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        await live.connect()
        await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true))
        try await wait { live.snapshot.running }
        live.draft = "Keep this draft"
        await live.steerDraft()
        #expect(live.draft == "Keep this draft")
        #expect(await runtime.commands.isEmpty)
        await live.enqueueDraft()
        let id = try #require(live.queue.first?.id)
        await live.promoteQueued(id: id)
        #expect(live.queue.map(\.id) == [id])
        #expect(await runtime.commands.isEmpty)
        await live.disconnect()
    }

    @Test func recoveryDoesNotReplayAnUnconfirmedDequeuedCommand() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let store = ConversationDraftStore(directory: directory)
        let runtime = QueueRuntime()
        let live = conversation(runtime, drafts: store)
        live.draft = "Pending first"
        await live.enqueueDraft()
        live.draft = "Queued second"
        await live.enqueueDraft()
        await live.resumeQueue()
        let command = try #require(await runtime.commands.first)
        await store.flush()
        let restored = conversation(runtime, drafts: ConversationDraftStore(directory: directory))
        #expect(restored.pendingPrompt?.commandID == command.id)
        #expect(restored.pendingPrompt?.phase == .uncertain)
        #expect(restored.queue.map(\.text) == ["Queued second"])
        await restored.connect()
        await restored.resumeQueue()
        #expect(await runtime.commands.count == 1)
        #expect(restored.queueHeld)
        await restored.disconnect()
        await live.disconnect()
    }

    @Test func firstTurnSettingsWaitForObservedSelectionBeforeReturning() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        await live.connect()
        let models: [ConversationControls.Choice] = [.init(id: "old", title: "Old"), .init(id: "new", title: "New")]
        await runtime.emit(.init(connected: true, ready: true,
            controls: .init(models: models, selectedModelID: "old")))
        try await wait { live.snapshot.controls != nil }
        var finished = false
        let applying = Task { let success = await live.applySettings(modelID: "new", configurationValues: [:]); finished = true; return success }
        while await runtime.commands.isEmpty { await Task.yield() }
        #expect(!finished)
        await runtime.emit(.init(connected: true, ready: true,
            controls: .init(models: models, selectedModelID: "new", pendingChanges: true)))
        try await wait { live.snapshot.controls?.selectedModelID == "new" }
        #expect(!finished)
        await runtime.emit(.init(connected: true, ready: true,
            controls: .init(models: models, selectedModelID: "new")))
        #expect(await applying.value)
        #expect(await runtime.commands.map(\.action) == [.model])
        await live.disconnect()
    }

    @Test func stopDuringBeforeCheckpointCancelsOnlyTheUndispatchedPrompt() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        let gate = CheckpointGate()
        live.beforePrompt = { _, _ in await gate.capture() }
        live.draft = "Preparing first"
        await live.enqueueDraft()
        live.draft = "Leave queued"
        await live.enqueueDraft()
        let resuming = Task { await live.resumeQueue() }
        try await wait { gate.arrivals == 1 }
        #expect(live.canStop)
        #expect(await runtime.commands.isEmpty)
        await live.stop()
        #expect(live.queueHeld)
        #expect(live.status == "Stopped")
        gate.release(0)
        await resuming.value
        #expect(await runtime.commands.isEmpty)
        #expect(live.draft == "Preparing first")
        #expect(live.queue.map(\.text) == ["Leave queued"])
        #expect(live.pendingPrompt == nil)
        #expect(live.canSubmit)
        await live.disconnect()
    }

    @Test func idleSnapshotWhileAwaitingSendDoesNotCaptureAnUnstartedTurn() async throws {
        let runtime = QueueRuntime()
        await runtime.suspendPrompt()
        let live = conversation(runtime)
        var captures = 0
        live.afterTurn = { _, _ in captures += 1 }
        await live.connect()
        live.draft = "New prompt"
        let sending = Task { await live.send() }
        while await runtime.commands.isEmpty { await Task.yield() }
        let command = try #require(await runtime.commands.first)
        let revision = live.revision
        await runtime.emit(.init(connected: true, ready: true, canCancel: true))
        try await wait { live.revision > revision }
        #expect(captures == 0)
        #expect(live.completion == nil)
        await runtime.releasePrompt()
        await sending.value
        // Even when running and completed were coalesced, this exact native
        // acceptance and echo still produce one completed-turn checkpoint.
        let user = TranscriptRecord(recordID: "native-user", record: Message(role: "user", text: command.text,
            toolName: nil, toolInput: nil, toolOutput: nil, eventID: "native-user", sourceTurnID: "native-turn"))
        await runtime.emit(.init(records: [user], connected: true, ready: true,
            deliveries: [.init(commandID: command.id, status: "completed", error: nil,
                               nativeTurnID: "native-turn", nativeMessageID: "native-user")]))
        try await wait { captures == 1 }
        #expect(live.completion == .completed)
        #expect(live.pendingPrompt == nil)
        await live.disconnect()
    }

    @Test func overlappingAndObsoleteCheckpointCompletionsCannotReleaseANewerCapture() async throws {
        for reconnect in [false, true] {
            let runtime = QueueRuntime()
            let live = conversation(runtime)
            let gate = CheckpointGate()
            live.afterTurn = { _, _ in await gate.capture() }
            await live.connect()
            await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true))
            try await wait { live.snapshot.running }
            await runtime.emit(.init(connected: true, ready: true, canCancel: true))
            try await wait { gate.arrivals == 1 }
            if reconnect { await live.connect() }
            await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true))
            try await wait { live.snapshot.running }
            await runtime.emit(.init(connected: true, ready: true, canCancel: true))
            try await wait { gate.arrivals == 2 }
            #expect(live.status == "Saving checkpoint…")
            live.draft = "Next queued turn"
            await live.enqueueDraft()
            await live.resumeQueue()
            #expect(await runtime.commands.isEmpty)
            gate.release(0)
            try await wait { gate.finished.contains(0) }
            #expect(live.status == "Saving checkpoint…")
            #expect(!live.canSend)
            #expect(await runtime.commands.isEmpty)
            gate.release(1)
            try await wait { live.pendingPrompt != nil }
            #expect(await runtime.commands.map(\.text) == ["Next queued turn"])
            await live.disconnect()
        }
    }

    @Test func lateNativeEchoDoesNotCaptureTheSameCompletedTurnTwice() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        var captures = 0
        live.afterTurn = { _, _ in captures += 1 }
        await live.connect()
        live.draft = "Delayed native echo"
        await live.send()
        let command = try #require(await runtime.commands.first)
        await runtime.emit(.init(connected: true, ready: true, running: true))
        try await wait { live.snapshot.running }
        await runtime.emit(.init(connected: true, ready: true))
        try await wait { captures == 1 }
        let user = TranscriptRecord(recordID: "late-user", record: Message(role: "user", text: command.text,
            toolName: nil, toolInput: nil, toolOutput: nil, eventID: "late-user", sourceTurnID: "native-turn"))
        await runtime.emit(.init(records: [user], connected: true, ready: true,
            deliveries: [.init(commandID: command.id, status: "completed", error: nil,
                               nativeTurnID: "native-turn", nativeMessageID: "late-user")]))
        try await wait { live.pendingPrompt == nil }
        #expect(captures == 1)
        await live.disconnect()
    }

    @Test func checkpointBlocksHistoryMutationAndStaleMutationCannotDisconnectReopenedSession() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        let gate = CheckpointGate()
        live.afterTurn = { _, _ in await gate.capture() }
        let user = TranscriptRecord(recordID: "native-user", record: Message(role: "user", text: "Existing turn",
            toolName: nil, toolInput: nil, toolOutput: nil, eventID: "native-user", sourceTurnID: "native-turn"))
        let request = ConversationHistoryMutation(operation: .revert,
            boundary: try #require(ConversationHistoryBoundary.choices(in: [user]).first))
        await live.connect()
        await runtime.emit(.init(records: [user], connected: true, ready: true, running: true, canMutateHistory: true))
        try await wait { live.snapshot.running }
        await runtime.emit(.init(records: [user], connected: true, ready: true, canMutateHistory: true))
        try await wait { gate.arrivals == 1 }
        #expect(!live.canMutateHistory)
        do { _ = try await live.mutateHistory(request); Issue.record("Mutation crossed checkpoint gate") }
        catch {}
        #expect(await runtime.mutationCount == 0)
        gate.release(0)
        try await wait { live.canMutateHistory }
        let mutation = Task { try await live.mutateHistory(request) }
        while await runtime.mutationCount == 0 { await Task.yield() }
        await live.disconnect()
        await live.connect()
        let disconnects = await runtime.disconnectCount
        await runtime.releaseMutation()
        do { _ = try await mutation.value; Issue.record("An obsolete mutation reported current success") }
        catch {}
        #expect(live.snapshot.connected)
        #expect(live.canSend)
        #expect(!live.changingHistory)
        #expect(await runtime.disconnectCount == disconnects)
        await live.disconnect()
    }
    @Test func reviewedTransferPreservesBytesAndFailureLeavesOriginalQueued() async throws {
        let runtime = QueueRuntime()
        let store = ConversationDraftStore()
        let live = conversation(runtime, drafts: store)
        let attachment = ConversationAttachment(id: "exact-id", title: "capture", path: "/changed/source", content: Data([0, 1, 255]))
        #expect(live.replaceDraft(text: "Review before sending", attachments: [attachment]))
        await live.enqueueDraft()
        let entry = try #require(live.queue.first)
        live.onTransferQueuedPrompt = { _, captured in
            #expect(captured == entry)
            throw ConversationRuntimeError(message: "Draft storage unavailable")
        }
        await live.transferQueuedPrompt(id: entry.id)
        #expect(live.queue == [entry])
        #expect(live.queueHeld)
        #expect(await runtime.commands.isEmpty)
        var saved: ConversationQueuedPrompt?
        live.onTransferQueuedPrompt = { _, captured in saved = captured }
        await live.transferQueuedPrompt(id: entry.id)
        #expect(saved?.attachments == [attachment])
        #expect(live.queue.isEmpty)
        #expect(store.drafts[session.id]?.queue.isEmpty == true)
        #expect(await runtime.commands.isEmpty)
    }

    @Test func explicitRestartWaitsForConfirmedStopAndSendsOneNewTurn() async throws {
        let runtime = QueueRuntime()
        let live = conversation(runtime)
        await live.connect()
        await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true))
        try await wait { live.canInterruptAndRestart }
        live.draft = "New turn, not steering"
        let restarting = Task { await live.interruptAndRestartDraft() }
        try await wait { live.status == "Stopping…" }
        #expect(await runtime.commands.map(\.action) == [.cancel])
        #expect(live.draft == "New turn, not steering")
        await runtime.emit(.init(connected: true, ready: true, canCancel: true))
        await restarting.value
        #expect(await runtime.commands.map(\.action) == [.cancel, .prompt])
        #expect(live.queueHeld)
        await live.disconnect()
    }

    @Test func restartTimeoutAndEditedDraftNeverSend() async throws {
        for edit in [false, true] {
            let runtime = QueueRuntime()
            let live = conversation(runtime, timeout: .milliseconds(80))
            await live.connect()
            await runtime.emit(.init(connected: true, ready: true, running: true, canCancel: true))
            try await wait { live.canInterruptAndRestart }
            live.draft = "Original"
            let restarting = Task { await live.interruptAndRestartDraft() }
            try await wait { live.status == "Stopping…" }
            if edit {
                live.draft = "Changed during stop"
                await runtime.emit(.init(connected: true, ready: true, canCancel: true))
            }
            await restarting.value
            #expect(await runtime.commands.map(\.action) == [.cancel])
            #expect(live.draft == (edit ? "Changed during stop" : "Original"))
            await live.disconnect()
        }
    }

}
