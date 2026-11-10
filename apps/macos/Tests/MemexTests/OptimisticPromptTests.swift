import Foundation
import Testing
@testable import Memex

private func session() -> Session {
    let root = URL(fileURLWithPath: "/tmp/memex-optimistic-tests")
    return Session(source: "codex", sessionID: "native-session",
                   sourcePath: root.appendingPathComponent("sessions/2026/11/09/session.jsonl").path,
                   project: "workspace", cwd: root.path, machine: "local")
}

private func target(_ session: Session) -> InAppResumeTarget {
    InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
        workingDirectory: URL(fileURLWithPath: session.cwd!), providerHome: URL(fileURLWithPath: "/tmp/provider"),
        executableURL: URL(fileURLWithPath: "/bin/echo"), helperURL: nil, storageURL: URL(fileURLWithPath: "/tmp/runtime"))
}

private func record(_ id: String, role: String, _ text: String, eventID: String? = nil, turnID: String? = nil) -> TranscriptRecord {
    TranscriptRecord(recordID: id, record: Message(role: role, text: text, toolName: nil, toolInput: nil,
                                                    toolOutput: nil, eventID: eventID, sourceTurnID: turnID))
}

private actor FakeRuntime: ConversationRuntime {
    var commands: [ConversationCommand] = []
    var connections = 0
    private var holdPrompts = false
    private var promptWaiter: CheckedContinuation<Void, Never>?
    private var receive: (@Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void)?
    private let initial: [TranscriptRecord]

    init(initial: [TranscriptRecord] = []) { self.initial = initial }

    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) {
        self.receive = receive
        connections += 1
        receive(.success(ConversationSnapshot(records: initial, connected: true, ready: true, canCancel: true)))
    }
    func perform(_ command: ConversationCommand) async throws {
        commands.append(command)
        if holdPrompts, command.action == .prompt { await withCheckedContinuation { promptWaiter = $0 } }
    }
    func mutateHistory(_ request: ConversationHistoryMutation) async throws -> Session {
        throw ConversationRuntimeError(message: "unsupported")
    }
    func disconnect() {}
    func holdPrompt() { holdPrompts = true }
    func releasePrompt() { holdPrompts = false; promptWaiter?.resume(); promptWaiter = nil }
    func emit(_ snapshot: ConversationSnapshot) { receive?(.success(snapshot)) }
}

/// An ownership probe the test resolves by hand, standing in for a slow `lsof`.
@MainActor private final class ProbeGate {
    private(set) var started = false
    private var continuation: CheckedContinuation<Bool, Never>?
    func check() async -> Bool {
        started = true
        return await withCheckedContinuation { continuation = $0 }
    }
    func resolve(openElsewhere: Bool) {
        continuation?.resume(returning: openElsewhere)
        continuation = nil
    }
}

@MainActor private func waitFor(_ predicate: () -> Bool) async throws {
    for _ in 0..<400 {
        if predicate() { return }
        try await Task.sleep(for: .milliseconds(5))
    }
    #expect(predicate(), "Timed out waiting for the conversation")
}

private func accepted(_ command: ConversationCommand) -> ConversationDelivery {
    ConversationDelivery(commandID: command.id, status: "completed", error: nil,
                         nativeTurnID: "native-turn", nativeMessageID: "native-user")
}

@Suite(.serialized) @MainActor struct OptimisticPromptTests {
    @Test func sentPromptIsShownBeforeTheOwnershipProbeFinishes() async throws {
        let runtime = FakeRuntime()
        let gate = ProbeGate()
        let conversation = LiveConversation(session: session(), makeRuntime: { runtime }, resolveTarget: target,
                                            checkOwnership: { _ in await gate.check() })
        conversation.draft = "Hello"
        let sending = Task { await conversation.send() }
        try await waitFor { gate.started }

        // The probe has not answered and nothing has reached a provider.
        #expect(conversation.draft.isEmpty)
        #expect(conversation.isWorking)
        #expect(conversation.status == "Connecting…")
        #expect(conversation.displayRecords.map(\.record.text) == ["Hello"])
        #expect(await runtime.connections == 0)

        gate.resolve(openElsewhere: false)
        await sending.value
        #expect(await runtime.commands.map(\.text) == ["Hello"])
        await conversation.disconnect()
    }

    @Test func promptReturnsToTheDraftWhenTheProbeFindsAnotherWriter() async throws {
        let runtime = FakeRuntime()
        let gate = ProbeGate()
        let conversation = LiveConversation(session: session(), makeRuntime: { runtime }, resolveTarget: target,
                                            checkOwnership: { _ in await gate.check() })
        conversation.draft = "Keep me"
        let sending = Task { await conversation.send() }
        try await waitFor { gate.started }
        gate.resolve(openElsewhere: true)
        await sending.value

        #expect(conversation.isOpenElsewhere)
        #expect(conversation.draft == "Keep me")
        #expect(conversation.pendingPrompt == nil)
        #expect(conversation.optimisticRecord == nil)
        #expect(conversation.displayRecords.isEmpty)
        #expect(await runtime.connections == 0)
        #expect(await runtime.commands.isEmpty)
    }

    @Test func rowSurvivesAcceptanceAndIsReplacedByTheNativeEchoExactlyOnce() async throws {
        let earlier = record("runtime:a1", role: "assistant", "Earlier answer")
        let runtime = FakeRuntime(initial: [earlier])
        let conversation = LiveConversation(session: session(), makeRuntime: { runtime }, resolveTarget: target,
                                            checkOwnership: { _ in false })
        await conversation.connect()
        conversation.draft = "Next question"
        await conversation.send()
        let command = try #require(await runtime.commands.first)
        #expect(conversation.displayRecords.map(\.id) == ["runtime:a1", "optimistic:" + command.id])

        // Accepted, and output is already streaming, but the user record has not arrived.
        let streaming = record("runtime:a2", role: "assistant", "Working on it")
        await runtime.emit(ConversationSnapshot(records: [earlier, streaming], connected: true, ready: true,
                                                running: true, deliveries: [accepted(command)]))
        try await waitFor { conversation.pendingPrompt == nil }
        #expect(conversation.displayRecords.map(\.id) == ["runtime:a1", "optimistic:" + command.id, "runtime:a2"])

        let echo = record("runtime:u1", role: "user", "Next question", eventID: "native-user", turnID: "native-turn")
        await runtime.emit(ConversationSnapshot(records: [earlier, echo, streaming], connected: true, ready: true,
                                                running: true, deliveries: [accepted(command)]))
        try await waitFor { conversation.optimisticPrompt == nil }
        #expect(conversation.displayRecords.map(\.id) == ["runtime:a1", "runtime:u1", "runtime:a2"])
        await conversation.disconnect()
    }

    @Test func echoWithoutNativeIdentityIsMatchedByTextAfterDispatchOnly() async throws {
        // The same words were sent in an earlier turn; that record must not count as this prompt's echo.
        let earlierUser = record("runtime:u0", role: "user", "continue")
        let earlierAnswer = record("runtime:a0", role: "assistant", "Done")
        let runtime = FakeRuntime(initial: [earlierUser, earlierAnswer])
        let conversation = LiveConversation(session: session(), makeRuntime: { runtime }, resolveTarget: target,
                                            checkOwnership: { _ in false })
        await conversation.connect()
        conversation.draft = "continue"
        await conversation.send()
        let command = try #require(await runtime.commands.first)
        #expect(conversation.displayRecords.last?.id == "optimistic:" + command.id)

        let delivery = ConversationDelivery(commandID: command.id, status: "completed", error: nil,
                                            nativeTurnID: "turn-without-user-record", nativeMessageID: nil)
        let echo = record("runtime:u1", role: "user", "continue")
        await runtime.emit(ConversationSnapshot(records: [earlierUser, earlierAnswer, echo], connected: true, ready: true,
                                                running: true, deliveries: [delivery]))
        try await waitFor { conversation.optimisticPrompt == nil }
        #expect(conversation.displayRecords.map(\.id) == ["runtime:u0", "runtime:a0", "runtime:u1"])
        await conversation.disconnect()
    }

    @Test func followUpCanBeQueuedWhileAnEarlierPromptIsStillDispatching() async throws {
        let runtime = FakeRuntime()
        let conversation = LiveConversation(session: session(), makeRuntime: { runtime }, resolveTarget: target,
                                            checkOwnership: { _ in false })
        await conversation.connect()
        await runtime.holdPrompt()
        conversation.draft = "First"
        let sending = Task { await conversation.send() }
        try await waitFor { conversation.pendingPrompt?.phase == .awaitingConfirmation }

        #expect(conversation.canQueue)
        conversation.draft = "Second"
        await conversation.enqueueDraft()
        #expect(conversation.queue.map(\.text) == ["Second"])
        #expect(conversation.draft.isEmpty)
        // Queued work waits for the provider to be idle; it is never sent alongside the first prompt.
        #expect(await runtime.commands.map(\.text) == ["First"])

        await runtime.releasePrompt()
        await sending.value
        let first = try #require(await runtime.commands.first)
        let echo = record("runtime:u1", role: "user", "First", eventID: "native-user", turnID: "native-turn")
        await runtime.emit(ConversationSnapshot(records: [echo], connected: true, ready: true, canCancel: true,
                                                deliveries: [accepted(first)]))
        try await waitFor { conversation.queue.isEmpty }
        try await waitFor { conversation.pendingPrompt?.text == "Second" }
        #expect(await runtime.commands.map(\.text) == ["First", "Second"])
        await conversation.disconnect()
    }

    @Test func promptNeedingReviewLeavesTheTranscriptForTheRecoveryView() async throws {
        let runtime = FakeRuntime()
        let conversation = LiveConversation(session: session(), makeRuntime: { runtime }, resolveTarget: target,
                                            checkOwnership: { _ in false })
        await conversation.connect()
        conversation.draft = "Uncertain"
        await conversation.send()
        let command = try #require(await runtime.commands.first)
        #expect(conversation.optimisticRecord != nil)

        await runtime.emit(ConversationSnapshot(connected: true, ready: true, deliveries: [
            ConversationDelivery(commandID: command.id, status: "failed", error: "Lost", nativeTurnID: nil, nativeMessageID: nil),
        ]))
        try await waitFor { conversation.pendingPrompt?.phase == .uncertain }
        #expect(conversation.optimisticRecord == nil)
        #expect(conversation.displayRecords.isEmpty)
        #expect(!conversation.canQueue)
        await conversation.disconnect()
    }

    @Test func steerStaysOutOfTheTranscriptUntilTheProviderPlacesIt() async throws {
        let answer = record("runtime:a1", role: "assistant", "Working")
        let runtime = FakeRuntime(initial: [answer])
        let conversation = LiveConversation(session: session(), makeRuntime: { runtime }, resolveTarget: target,
                                            checkOwnership: { _ in false })
        await conversation.connect()
        await runtime.emit(ConversationSnapshot(records: [answer], connected: true, ready: true, running: true,
                                                canCancel: true, canSteer: true))
        try await waitFor { conversation.canSteer }
        conversation.draft = "Also check the tests"
        await conversation.steerDraft()

        #expect(conversation.pendingPrompt?.isSteer == true)
        #expect(conversation.optimisticRecord == nil)
        #expect(conversation.displayRecords.map(\.id) == ["runtime:a1"])
        await conversation.disconnect()
    }
}

@Suite(.serialized) @MainActor struct SentRowRevealTests {
    @Test func requestingTheNewestRowBringsItIntoViewAndResumesFollowing() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 300)
        controller.view.layoutSubtreeIfNeeded()
        var records = (0..<20).map { record("r\($0)", role: "assistant", "Message \($0)") }
        controller.update(sessionID: "live", records: records, provider: "codex", startsAtEnd: true, followLatest: true)
        // The reader is back in earlier history when they send.
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: controller.scrollView.contentView.bounds.minY - 120))
        #expect(!controller.isFollowingLatest)

        records.append(record("optimistic:c1", role: "user", "Sent while reading"))
        controller.update(sessionID: "live", records: records, provider: "codex", startsAtEnd: true,
                          requestedRecordID: "optimistic:c1", requestGeneration: 1, followLatest: true)
        #expect(controller.isFollowingLatest)
        let end = controller.table.rect(ofRow: controller.rows.count - 1).maxY
        #expect(abs(end - controller.scrollView.contentView.bounds.maxY) < 1)
    }
}
