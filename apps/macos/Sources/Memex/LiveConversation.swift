import CryptoKit
import Foundation
import Observation

struct ConversationRuntimeError: LocalizedError, Sendable {
    enum Kind: Sendable { case other, openElsewhere, settingsRejected }
    let message: String
    var kind: Kind = .other
    var errorDescription: String? { message }
}

struct ConversationApproval: Identifiable, Equatable, Sendable {
    struct Option: Identifiable, Equatable, Sendable {
        let id: String
        let title: String
        let kind: String
    }
    let id: String
    let title: String
    let detail: String?
    let options: [Option]
    var kind: String? = nil
}

struct ConversationQuestion: Identifiable, Equatable, Sendable {
    struct Choice: Identifiable, Equatable, Sendable {
        let id: String
        let title: String
        let value: String
        var description: String? = nil
    }
    let id: String
    let title: String?
    let prompt: String
    let placeholder: String?
    let choices: [Choice]
    var defaultValue: String? = nil
    var multiSelect = false
    var isSecret = false
}

struct ConversationSnapshot: Equatable, Sendable {
    var hostConversationID: String? = nil
    var records: [TranscriptRecord] = []
    var connected = false
    var ready = false
    var running = false
    var pendingPrompt = false
    var canCancel = false
    var canSteer = false
    var canMutateHistory = false
    var approvals: [ConversationApproval] = []
    var questions: [ConversationQuestion] = []
    var warning: String?
    var controls: ConversationControls?
    var deliveries: [ConversationDelivery] = []
    var childHistories: [ConversationChildHistory] = []
    var mcpAppConnection: NativeMcpAppConnection? = nil
}

struct ConversationCommand: Sendable, Equatable {
    enum Action: Sendable, Equatable { case prompt, steer, cancel, approval, userInput, model, configuration }
    let id: String
    let issuedAt: String
    let action: Action
    let text: String
    let requestID: String?
    let attachments: [ConversationAttachment]

    init(_ action: Action, text: String = "", requestID: String? = nil, attachments: [ConversationAttachment] = [],
         id: String = UUID().uuidString, issuedAt: String = Date().ISO8601Format()) {
        self.id = id
        self.issuedAt = issuedAt
        self.action = action
        self.text = text
        self.requestID = requestID
        self.attachments = attachments
    }
}

protocol ConversationRuntime: Sendable {
    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) async throws
    func perform(_ command: ConversationCommand) async throws
    func mutateHistory(_ request: ConversationHistoryMutation) async throws -> Session
    func readChild(_ id: String) async throws -> ConversationChildHistory
    func disconnect() async
}

extension ConversationRuntime {
    func readChild(_ id: String) async throws -> ConversationChildHistory {
        throw ConversationRuntimeError(message: "This provider does not expose child conversation history.")
    }
}

@MainActor @Observable
final class LiveConversation {
    enum Ownership: Equatable {
        case available, openElsewhere, unavailable(String)
    }

    let session: Session
    let isServerOwned: Bool
    var draft = "" { didSet { persistDraft() } }
    private(set) var attachments: [ConversationAttachment] = []
    private(set) var attachmentError: String?
    private(set) var loadingAttachments = false
    private(set) var pendingPrompt: ConversationPendingPrompt?
    private(set) var queue: [ConversationQueuedPrompt] = []
    private(set) var queueHeld = false
    private(set) var snapshot = ConversationSnapshot()
    private(set) var hasSnapshot = false
    private(set) var connecting = false
    private(set) var submitting = false
    private(set) var changingHistory = false
    private var checkpointCaptures: Set<UUID> = []
    private var capturingCheckpoint: Bool { !checkpointCaptures.isEmpty }
    private var observedProviderWork = false
    private var checkpointedPromptIDs: Set<String> = []
    private(set) var connectionAttempted = false
    private var preparingPrompt = false
    private var applyingSettings = false
    private(set) var error: String?
    private(set) var settingsError: String?
    private(set) var ownership: Ownership = .available
    private(set) var revision = 0
    private(set) var focusRequest = 0
    private(set) var visibleLimit = 120
    private(set) var completion: ConversationActivity?
    private var deliveryUncertain = false
    private var stopping = false
    private var restartRequested = false
    private(set) var transferringQueue = false
    private var adoptingCreatedSession: Bool
    @ObservationIgnored private let drafts: ConversationDraftStore?
    @ObservationIgnored private var runtime: (any ConversationRuntime)?
    @ObservationIgnored private var generation = UUID()
    @ObservationIgnored private var connectionWaiter: CheckedContinuation<Bool, Never>?
    @ObservationIgnored private let makeRuntime: @Sendable () -> any ConversationRuntime
    @ObservationIgnored private let resolveTarget: @Sendable (Session) throws -> InAppResumeTarget
    @ObservationIgnored private let checkOwnership: (Session) throws -> Bool
    @ObservationIgnored var onNotificationState: ((Session, ConversationNotificationState) -> Void)?
    @ObservationIgnored var onTransferQueuedPrompt: ((Session, ConversationQueuedPrompt) async throws -> Void)?
    @ObservationIgnored var beforePrompt: ((Session, ConversationCommand) async -> Void)?
    @ObservationIgnored var afterTurn: ((Session, ConversationSnapshot) async -> Void)?
    @ObservationIgnored private var stopTimeout: Task<Void, Never>?
    @ObservationIgnored private let stopTimeoutDuration: Duration
    @ObservationIgnored private var settingsWaiter: (predicate: (ConversationControls) -> Bool, continuation: CheckedContinuation<Bool, Never>)?
    @ObservationIgnored private var settingsTimeout: Task<Void, Never>?

    init(session: Session, makeRuntime: @escaping @Sendable () -> any ConversationRuntime = { InAppAgentRuntime.make() },
         resolveTarget: @escaping @Sendable (Session) throws -> InAppResumeTarget = { try InAppResumeTarget.resolve($0) },
         checkOwnership: @escaping (Session) throws -> Bool = ConversationOwnership.isOpenElsewhere,
         drafts: ConversationDraftStore? = nil, adoptingCreatedSession: Bool = false, isServerOwned: Bool = false,
         stopTimeoutDuration: Duration = .seconds(15)) {
        self.session = session
        self.isServerOwned = isServerOwned
        self.makeRuntime = makeRuntime
        self.resolveTarget = resolveTarget
        self.checkOwnership = checkOwnership
        self.drafts = drafts
        self.adoptingCreatedSession = adoptingCreatedSession
        self.stopTimeoutDuration = stopTimeoutDuration
        if let saved = drafts?.drafts[session.id] {
            attachments = saved.attachments
            pendingPrompt = saved.pendingPrompt
            deliveryUncertain = saved.deliveryUncertain
            queue = saved.queue
            // Reopening a viewer cannot authorize sending previously queued work.
            queueHeld = saved.queueHeld || !saved.queue.isEmpty
            if pendingPrompt?.phase == .preparing { pendingPrompt?.phase = .notSent }
            if deliveryUncertain { pendingPrompt?.phase = .uncertain }
            if saved.deliveryUncertain {
                error = "The previous send could not be confirmed. Reload and review the conversation before sending again."
                connectionAttempted = true
            }
            // The draft observer saves the entire state. Restore it last so a
            // viewer opened without further edits cannot erase saved intent.
            draft = saved.text
        }
        refreshOwnership()
    }

    private func persistDraft() {
        drafts?.set(.init(text: draft, deliveryUncertain: deliveryUncertain, attachments: attachments,
                         pendingPrompt: pendingPrompt, queue: queue, queueHeld: queueHeld), for: session.id)
    }

    var draftSaveError: String? { drafts?.error }

    var listState: ConversationListState {
        let activity: ConversationActivity?
        if error != nil || ownershipError != nil { activity = .failed }
        else if !snapshot.approvals.isEmpty { activity = .approval }
        else if !snapshot.questions.isEmpty { activity = .question }
        else if connecting { activity = .starting }
        else if isWorking { activity = stopping ? .stopping : .working }
        else if isOpenElsewhere { activity = .openElsewhere }
        else { activity = completion }
        return ConversationListState(activity: activity,
                                     hasDraft: hasPrompt || !queue.isEmpty || pendingPrompt?.phase == .notSent || pendingPrompt?.phase == .uncertain)
    }

    var isOpenElsewhere: Bool { ownership == .openElsewhere }
    var ownershipError: String? {
        if case .unavailable(let message) = ownership { return message }
        return nil
    }

    func refreshOwnership() {
        // Once our provider is loading or connected, its own writer lock is
        // expected. Only inspect ownership before opening a native session.
        guard !adoptingCreatedSession, !connecting, !snapshot.connected else { return }
        do {
            ownership = try checkOwnership(session) ? .openElsewhere : .available
        } catch {
            ownership = .unavailable("Could not check whether this conversation is open elsewhere: \(error.localizedDescription)")
        }
    }

    var canSend: Bool {
        ownership == .available && snapshot.connected && snapshot.ready && !snapshot.running && !snapshot.pendingPrompt
            && snapshot.approvals.isEmpty && snapshot.questions.isEmpty && !connecting && !submitting && error == nil
            && snapshot.controls?.pendingChanges != true && !loadingAttachments && !applyingSettings && !changingHistory && !capturingCheckpoint
    }
    var canSubmit: Bool {
        ownership == .available && !preparingPrompt && pendingPrompt == nil
            && (canSend || (!connectionAttempted && !connecting && !submitting && error == nil))
    }
    var hasPrompt: Bool { draft.nilIfBlank != nil || !attachments.isEmpty }
    var canQueue: Bool { ownership == .available && !preparingPrompt && !loadingAttachments && !deliveryUncertain && error == nil && !changingHistory }
    var canSteer: Bool {
        canQueue && snapshot.connected && snapshot.ready && snapshot.running && snapshot.canSteer
            && pendingPrompt == nil && !submitting && !stopping && snapshot.controls?.pendingChanges != true
            && snapshot.approvals.isEmpty && snapshot.questions.isEmpty && !capturingCheckpoint
    }
    var canInterruptAndRestart: Bool {
        canQueue && snapshot.connected && snapshot.ready && snapshot.running && !snapshot.canSteer
            && snapshot.canCancel && !stopping && !submitting && pendingPrompt == nil
            && snapshot.approvals.isEmpty && snapshot.questions.isEmpty
            && snapshot.controls?.pendingChanges != true && !capturingCheckpoint
    }
    var canTransferQueuedPrompt: Bool {
        onTransferQueuedPrompt != nil && !transferringQueue && !isServerOwned && session.machineID == "local"
    }
    var canChangeSettings: Bool {
        ownership == .available && snapshot.connected && snapshot.ready && !connecting && !submitting
            && !preparingPrompt && pendingPrompt == nil && error == nil && !applyingSettings
            && snapshot.controls?.pendingChanges != true && !changingHistory && !capturingCheckpoint
    }
    var isWorking: Bool { connecting || preparingPrompt || submitting || snapshot.running || snapshot.pendingPrompt || changingHistory || capturingCheckpoint }
    var canMutateHistory: Bool { canSend && !preparingPrompt && snapshot.canMutateHistory && pendingPrompt == nil && queue.isEmpty }
    var canStop: Bool {
        !stopping && (connecting || (preparingPrompt && pendingPrompt?.phase == .preparing)
            || (snapshot.connected && snapshot.canCancel && (snapshot.running || snapshot.pendingPrompt) && !submitting))
    }
    private var visibleStart: Int {
        var remaining = visibleLimit
        var start = snapshot.records.endIndex
        while start > 0 && remaining > 0 {
            start -= 1
            if !snapshot.records[start].isRawOnly { remaining -= 1 }
        }
        // Keep the evidence next to the first readable row in the window.
        while start > 0 && snapshot.records[start - 1].isRawOnly { start -= 1 }
        return start
    }
    var visibleRecords: [TranscriptRecord] { Array(snapshot.records[visibleStart...]) }
    var hasEarlierRecords: Bool { visibleStart > 0 }
    func loadEarlierRecords() { visibleLimit += MemexClient.pageSize }
    func revealRecord(_ id: String) {
        if let index = snapshot.records.firstIndex(where: { $0.id == id }) {
            let readable = snapshot.records[index...].filter { !$0.isRawOnly }.count
            visibleLimit = max(visibleLimit, readable + MemexClient.pageSize / 2)
        }
    }
    var status: String {
        if changingHistory { return "Changing conversation history…" }
        if isOpenElsewhere { return "Open elsewhere" }
        if connecting { return "Connecting…" }
        if error != nil { return "Connection needs attention" }
        if !snapshot.connected { return "Disconnected" }
        if !snapshot.approvals.isEmpty { return "Waiting for approval" }
        if !snapshot.questions.isEmpty { return "Waiting for your answer" }
        if stopping { return "Stopping…" }
        if preparingPrompt && pendingPrompt?.phase == .notSent && completion == .stopped { return "Stopped" }
        if capturingCheckpoint { return "Saving checkpoint…" }
        if snapshot.running { return "Working…" }
        if submitting || snapshot.pendingPrompt { return "Sending…" }
        if !snapshot.ready { return "Loading session…" }
        return "Ready"
    }

    func focus() { focusRequest += 1 }

    func reportAttachmentError(_ message: String?) { attachmentError = message }

    func readChild(_ id: String) async {
        guard snapshot.connected, let runtime, !id.isEmpty else { return }
        let token = generation
        snapshot.childHistories.removeAll { $0.id == id }
        snapshot.childHistories.append(.init(id: id, loading: true))
        do {
            let history = try await runtime.readChild(id)
            guard generation == token, history.id == id else { return }
            snapshot.childHistories.removeAll { $0.id == id }
            snapshot.childHistories.append(history)
        } catch {
            guard generation == token else { return }
            snapshot.childHistories.removeAll { $0.id == id }
            snapshot.childHistories.append(.init(id: id, error: error.localizedDescription))
        }
    }

    func mutateHistory(_ request: ConversationHistoryMutation) async throws -> Session {
        guard canMutateHistory, let runtime,
              ConversationHistoryBoundary.choices(in: snapshot.records).contains(request.boundary) else {
            throw ConversationRuntimeError(message: "Connect an idle conversation and select a verified native turn before changing history. Clear or finish queued work first.")
        }
        changingHistory = true
        let token = generation
        defer { if generation == token { changingHistory = false } }
        queueHeld = true
        persistDraft()
        await drafts?.flush()
        guard generation == token else { throw ConversationRuntimeError(message: "The connection changed before history could be updated.") }
        if let draftSaveError { throw ConversationRuntimeError(message: draftSaveError) }
        do {
            let session = try await runtime.mutateHistory(request)
            guard generation == token else {
                throw ConversationRuntimeError(message: "The connection changed while updating history. Inspect the native session before retrying.")
            }
            if request.operation == .revert { await disconnect() }
            return session
        } catch {
            if generation == token { await disconnect() }
            throw error
        }
    }

    func showHistory(_ records: [TranscriptRecord]) {
        guard !connecting, !snapshot.connected else { return }
        snapshot.records = records
        hasSnapshot = true
        revision += 1
    }

    @discardableResult
    func connect() async -> Bool {
        refreshOwnership()
        guard ownership == .available, !connecting else { return false }
        adoptingCreatedSession = false
        finishConnecting(false)
        let token = UUID()
        generation = token
        checkpointCaptures.removeAll()
        checkpointedPromptIDs.removeAll()
        observedProviderWork = false
        changingHistory = false
        connectionAttempted = true
        connecting = true
        if !queue.isEmpty { queueHeld = true; persistDraft() }
        settleStop()
        submitting = false
        error = nil
        snapshot.connected = false
        snapshot.ready = false
        snapshot.approvals = []
        snapshot.questions = []
        let previous = runtime
        runtime = nil
        await previous?.disconnect()
        guard generation == token else { return false }
        do {
            let session = session
            let resolver = resolveTarget
            let target = try await Task.detached { try resolver(session) }.value
            guard generation == token, !Task.isCancelled else { return false }
            let driver = makeRuntime()
            runtime = driver
            try await driver.connect(target) { [weak self] result in
                Task { @MainActor [weak self] in
                    guard let self, self.generation == token else { return }
                    switch result {
                    case .success(let snapshot):
                        guard self.snapshot != snapshot || !self.hasSnapshot else { return }
                        let wasConnecting = self.connecting
                        let wasWorking = self.observedProviderWork
                        let providerIsIdle = snapshot.connected && snapshot.ready
                            && !snapshot.running && !snapshot.pendingPrompt
                            && snapshot.approvals.isEmpty && snapshot.questions.isEmpty
                        self.observedProviderWork = !providerIsIdle
                            && (wasWorking || snapshot.running || snapshot.pendingPrompt)
                        // A local pending flag is not native work. A fast turn may
                        // finish between publications. Its exact native command
                        // acknowledgement establishes that it reached the provider.
                        let pending = self.pendingPrompt
                        let acceptedLocalPrompt = pending.map { prompt in
                            prompt.phase == .awaitingConfirmation && prompt.isSteer != true
                                && !self.checkpointedPromptIDs.contains(prompt.commandID)
                                && snapshot.deliveries.contains {
                                    $0.commandID == prompt.commandID && $0.isAccepted
                                }
                        } ?? false
                        let finishedTurn = (wasWorking || acceptedLocalPrompt) && providerIsIdle
                        if snapshot.running || snapshot.pendingPrompt {
                            if !self.stopping { self.completion = nil }
                        }
                        else if finishedTurn,
                                self.completion == nil { self.completion = self.stopping ? .stopped : .completed }
                        self.snapshot = snapshot
                        if self.stopping && !snapshot.running && !snapshot.pendingPrompt {
                            self.completion = .stopped
                            self.settleStop()
                        }
                        self.reconcilePendingPrompt()
                        self.hasSnapshot = true
                        self.revision += 1
                        if snapshot.ready {
                            self.connecting = false
                            self.finishConnecting(true)
                            if wasConnecting, self.deliveryUncertain, self.pendingPrompt == nil {
                                self.deliveryUncertain = false
                                self.persistDraft()
                            }
                        }
                        self.checkSettingsWaiter()
                        if finishedTurn, let afterTurn = self.afterTurn {
                            if let pending { self.checkpointedPromptIDs.insert(pending.commandID) }
                            let captureID = UUID()
                            self.checkpointCaptures.insert(captureID)
                            self.notifyState()
                            await afterTurn(self.session, snapshot)
                            guard self.generation == token else { return }
                            self.checkpointCaptures.remove(captureID)
                        }
                        self.notifyState()
                        self.scheduleQueueDrain()
                    case .failure(let failure):
                        if failure.kind == .settingsRejected {
                            self.settingsError = failure.message
                            self.finishSettings(false)
                        } else {
                            await self.connectionFailed(failure)
                        }
                    }
                }
            }
            guard generation == token else { await driver.disconnect(); return false }
            let ready: Bool
            if snapshot.ready { ready = true }
            else if error != nil { ready = false }
            else { ready = await withCheckedContinuation { connectionWaiter = $0 } }
            guard ready, generation == token else { return false }
            focus()
            return true
        } catch {
            guard generation == token else { return false }
            await connectionFailed(error as? ConversationRuntimeError
                ?? ConversationRuntimeError(message: error.localizedDescription))
            return false
        }
    }

    private func connectionFailed(_ failure: ConversationRuntimeError) async {
        queueHeld = true
        settleStop()
        finishSettings(false)
        generation = UUID()
        checkpointCaptures.removeAll()
        checkpointedPromptIDs.removeAll()
        observedProviderWork = false
        changingHistory = false
        connecting = false
        submitting = false
        snapshot.connected = false
        snapshot.ready = false
        snapshot.running = false
        snapshot.pendingPrompt = false
        snapshot.controls = nil
        if pendingPrompt?.phase == .awaitingConfirmation {
            pendingPrompt?.phase = .uncertain
            deliveryUncertain = true
            persistDraft()
        }
        if failure.kind == .openElsewhere {
            ownership = .openElsewhere
            // Resume was rejected before a prompt could be delivered. Unlocking
            // permits a new explicit Send, never a replay of the retained draft.
            connectionAttempted = false
            error = nil
            hasSnapshot = false
        } else {
            error = failure.message
        }
        finishConnecting(false)
        persistDraft()
        notifyState()
        let previous = runtime
        runtime = nil
        await previous?.disconnect()
    }

    private func finishConnecting(_ ready: Bool) {
        let waiter = connectionWaiter
        connectionWaiter = nil
        waiter?.resume(returning: ready)
    }

    func send() async {
        refreshOwnership()
        guard canSubmit, hasPrompt else { return }
        let text = draft.trimmingCharacters(in: .whitespacesAndNewlines)
        let attachments = attachments
        let command = ConversationCommand(.prompt, text: text, attachments: attachments)
        pendingPrompt = ConversationPendingPrompt(command)
        self.attachments = []
        draft = ""
        preparingPrompt = true
        defer {
            preparingPrompt = false
            restoreUnsentPrompt()
            notifyState()
            scheduleQueueDrain()
        }
        // Browsing history does not launch a provider. The first explicit send
        // loads its original session, then sends exactly once when it is ready.
        if !connectionAttempted, !(await connect()) { return }
        guard canSend else { return }
        completion = nil
        stopping = false
        _ = await perform(command)
    }

    private func restoreUnsentPrompt() {
        guard let pending = pendingPrompt, pending.phase == .preparing || pending.phase == .notSent else { return }
        pendingPrompt?.phase = .notSent
        if draft.isEmpty && attachments.isEmpty { restorePendingDraft() }
        else { persistDraft() }
    }

    /// Explicit recovery never sends. Preserve any newer draft behind the returned intent.
    func restorePendingDraft() {
        guard let pending = pendingPrompt, pending.phase == .notSent || pending.phase == .uncertain,
              !isWorking else { return }
        pendingPrompt = nil
        deliveryUncertain = false
        attachments = pending.attachments + attachments.filter { item in !pending.attachments.contains { $0.id == item.id } }
        draft = [pending.text, draft].filter { !$0.isEmpty }.joined(separator: "\n\n")
        persistDraft()
        focus()
    }

    private func reconcilePendingPrompt() {
        guard let pending = pendingPrompt,
              let delivery = snapshot.deliveries.first(where: { $0.commandID == pending.commandID }) else { return }
        if delivery.isAccepted || (pending.isSteer == true && delivery.status == "completed")
            || delivery.hasNativeEcho(in: snapshot.records) && pending.isSteer != true {
            pendingPrompt = nil
            deliveryUncertain = false
            if pending.phase == .uncertain { error = nil }
            persistDraft()
        } else if delivery.status == "failed" {
            pendingPrompt?.phase = .uncertain
            deliveryUncertain = true
            error = delivery.error ?? "The send could not be confirmed. Review the native conversation before retrying."
            persistDraft()
        }
    }

    /// Recover only an exact, durable provider acknowledgement. Reading a saved
    /// receipt neither starts a provider nor authorizes replaying held work.
    func reconcileSavedDelivery(
        read: @Sendable (Session, String) async throws -> Bool = InAppAgentRuntime.confirmedDelivery
    ) async {
        guard let pending = pendingPrompt, deliveryUncertain, runtime == nil, !isWorking else { return }
        let token = generation
        guard (try? await read(session, pending.commandID)) == true,
              generation == token, pendingPrompt?.commandID == pending.commandID,
              runtime == nil, !isWorking else { return }
        pendingPrompt = nil
        deliveryUncertain = false
        error = nil
        connectionAttempted = false
        persistDraft()
        notifyState()
    }

    func setModel(_ id: String) async {
        _ = await applySettings(modelID: id, configurationValues: [:])
    }

    func configure(_ option: ConversationControls.Configuration, value: String) async {
        guard canChangeSettings, snapshot.controls?.configurations.contains(option) == true,
              option.choices.contains(where: { $0.id == value }) else { return }
        _ = await applySettings(modelID: nil, configurationValues: [option.id: value])
    }

    @discardableResult
    func replaceDraft(text: String, attachments: [ConversationAttachment]) -> Bool {
        guard ownership == .available, !preparingPrompt, !loadingAttachments else { return false }
        self.attachments = attachments
        draft = text
        persistDraft()
        return true
    }

    /// A remembered choice is applied only if this provider advertises it. Wait
    /// for native acknowledgement before the first prompt can use those choices.
    func applySettings(modelID: String?, configurationValues: [String: String]) async -> Bool {
        guard canChangeSettings else { return false }
        settingsError = nil
        applyingSettings = true
        defer { applyingSettings = false; scheduleQueueDrain() }
        if let modelID, snapshot.controls?.models.contains(where: { $0.id == modelID }) == true,
           snapshot.controls?.selectedModelID != modelID {
            guard await perform(ConversationCommand(.model, text: modelID)),
                  await waitForSettings({ $0.selectedModelID == modelID }) else { return false }
        }
        for (id, value) in configurationValues.sorted(by: { $0.key < $1.key }) {
            guard let option = snapshot.controls?.configurations.first(where: { $0.id == id }),
                  option.choices.contains(where: { $0.id == value }) else { continue }
            if option.selectedID != value {
                guard await perform(ConversationCommand(.configuration, text: value, requestID: id)),
                      await waitForSettings({ $0.configurations.first(where: { $0.id == id })?.selectedID == value }) else { return false }
            }
            if session.source == "claude", id == "permission_mode" {
                ConversationComposerPreferences.saveClaudePermissionMode(value, sessionID: session.id)
            }
        }
        return error == nil && snapshot.ready
    }

    private func waitForSettings(_ predicate: @escaping (ConversationControls) -> Bool) async -> Bool {
        if settingsError != nil { return false }
        if let controls = snapshot.controls, !controls.pendingChanges, predicate(controls) { return true }
        return await withCheckedContinuation { continuation in
            finishSettings(false)
            settingsWaiter = (predicate, continuation)
            settingsTimeout = Task { [weak self] in
                do { try await Task.sleep(for: .seconds(15)) } catch { return }
                guard let self, self.settingsWaiter != nil else { return }
                self.error = "The agent did not confirm the selected settings. Your draft has been retained."
                self.finishSettings(false)
                self.notifyState()
            }
        }
    }

    private func checkSettingsWaiter() {
        guard let waiter = settingsWaiter, let controls = snapshot.controls,
              !controls.pendingChanges, waiter.predicate(controls) else { return }
        finishSettings(true)
    }

    private func finishSettings(_ success: Bool) {
        settingsTimeout?.cancel()
        settingsTimeout = nil
        let waiter = settingsWaiter
        settingsWaiter = nil
        waiter?.continuation.resume(returning: success)
    }

    func clearSettingsError() { settingsError = nil }

    /// Explicit fallback: stop first, then submit this retained draft only after
    /// the provider reports idle. A failure, timeout, reconnect or edit cancels it.
    func interruptAndRestartDraft() async {
        guard canInterruptAndRestart, hasPrompt else { return }
        let text = draft
        let captured = attachments
        let token = generation
        restartRequested = true
        await stop(preservingRestart: true)
        while (stopping || capturingCheckpoint) && generation == token && error == nil {
            do { try await Task.sleep(for: .milliseconds(50)) } catch { restartRequested = false; return }
        }
        guard restartRequested, generation == token, error == nil,
              draft == text, attachments == captured, canSend else { restartRequested = false; return }
        restartRequested = false
        await send()
    }

    func transferQueuedPrompt(id: String) async {
        guard !transferringQueue, let transfer = onTransferQueuedPrompt,
              let entry = queue.first(where: { $0.id == id }) else { return }
        holdQueue()
        transferringQueue = true
        defer { transferringQueue = false }
        await drafts?.flush()
        guard draftSaveError == nil else { return }
        do {
            try await transfer(session, entry)
            guard let index = queue.firstIndex(where: { $0 == entry }) else {
                error = "The side-chat draft was saved, but the original queued message changed. Review both drafts before sending."
                return
            }
            queue.remove(at: index)
            persistDraft()
            await drafts?.flush()
        } catch { self.error = error.localizedDescription }
    }

    func enqueueDraft() async {
        guard canQueue, hasPrompt else { return }
        queue.append(ConversationQueuedPrompt(ConversationCommand(.prompt,
            text: draft.trimmingCharacters(in: .whitespacesAndNewlines), attachments: attachments)))
        attachments = []
        draft = ""
        persistDraft()
        await drafts?.flush()
        guard draftSaveError == nil else { queueHeld = true; persistDraft(); return }
        await drainQueue()
    }

    func holdQueue() {
        queueHeld = true
        persistDraft()
    }

    func editQueued(id: String, text: String, attachments: [ConversationAttachment]? = nil) {
        guard !transferringQueue, let index = queue.firstIndex(where: { $0.id == id }) else { return }
        let updatedAttachments = attachments ?? queue[index].attachments
        guard text.nilIfBlank != nil || !updatedAttachments.isEmpty else { return }
        queue[index].text = text
        queue[index].attachments = updatedAttachments
        persistDraft()
    }

    func moveQueued(id: String, offset: Int) {
        guard let index = queue.firstIndex(where: { $0.id == id }), !queue.isEmpty else { return }
        let destination = max(0, min(queue.count - 1, index + offset))
        guard !transferringQueue, index != destination else { return }
        let item = queue.remove(at: index)
        queue.insert(item, at: destination)
        persistDraft()
    }

    func cancelQueued(id: String) {
        guard !transferringQueue else { return }
        queue.removeAll { $0.id == id }
        persistDraft()
    }

    func resumeQueue() async {
        guard !transferringQueue, !queue.isEmpty, !deliveryUncertain, pendingPrompt == nil else { return }
        if !snapshot.connected || !snapshot.ready {
            guard await connect() else { return }
        }
        guard error == nil else { return }
        queueHeld = false
        persistDraft()
        await drafts?.flush()
        await drainQueue()
    }

    func promoteQueued(id: String) async {
        guard !transferringQueue, let index = queue.firstIndex(where: { $0.id == id }) else { return }
        moveQueued(id: id, offset: -index)
        if canSteer {
            await dispatchQueued(steer: true)
        } else if !snapshot.running {
            await resumeQueue()
        }
    }

    func steerDraft() async {
        guard canSteer, hasPrompt else { return }
        let command = ConversationCommand(.steer, text: draft.trimmingCharacters(in: .whitespacesAndNewlines), attachments: attachments)
        pendingPrompt = ConversationPendingPrompt(command)
        attachments = []
        draft = ""
        preparingPrompt = true
        defer { preparingPrompt = false; restoreUnsentPrompt(); notifyState(); scheduleQueueDrain() }
        _ = await perform(command)
    }

    private func scheduleQueueDrain() {
        guard !queueHeld, !queue.isEmpty, canSend, pendingPrompt == nil, !preparingPrompt else { return }
        Task { [weak self] in await self?.drainQueue() }
    }

    private func drainQueue() async {
        guard !queueHeld, !queue.isEmpty, canSend, pendingPrompt == nil, !preparingPrompt,
              draftSaveError == nil else { return }
        await dispatchQueued(steer: false)
    }

    private func dispatchQueued(steer: Bool) async {
        guard !queue.isEmpty, pendingPrompt == nil, !preparingPrompt,
              steer ? canSteer : canSend else { return }
        let command = queue.removeFirst().command(steer: steer)
        pendingPrompt = ConversationPendingPrompt(command)
        preparingPrompt = true
        completion = nil
        persistDraft()
        defer { preparingPrompt = false; restoreUnsentPrompt(); notifyState(); scheduleQueueDrain() }
        _ = await perform(command)
    }

    private func settleStop() {
        stopping = false
        stopTimeout?.cancel()
        stopTimeout = nil
    }

    private func notifyState() {
        onNotificationState?(session, .init(activity: listState.activity,
            requestIDs: snapshot.approvals.map(\.id) + snapshot.questions.map(\.id)))
    }

    func attachFiles(_ urls: [URL]) async {
        guard !loadingAttachments, ownership == .available else { return }
        attachmentError = nil
        if !connectionAttempted, !(await connect()) { return }
        guard let controls = snapshot.controls, snapshot.ready else { return }
        loadingAttachments = true
        defer { loadingAttachments = false }
        do {
            let existing = attachments
            let captured = try await Task.detached {
                try ConversationAttachment.capture(urls, controls: controls, existing: existing)
            }.value
            attachments.append(contentsOf: captured)
            persistDraft()
        } catch { attachmentError = error.localizedDescription }
    }

    func removeAttachment(_ id: String) {
        attachments.removeAll { $0.id == id }
        attachmentError = nil
        persistDraft()
    }

    func stop(preservingRestart: Bool = false) async {
        if !preservingRestart { restartRequested = false }
        queueHeld = true
        persistDraft()
        if connecting {
            await disconnect()
            connectionAttempted = false
            completion = .stopped
            notifyState()
            // Cancellation before provider dispatch is known not to have sent.
            if pendingPrompt?.phase == .preparing {
                pendingPrompt?.phase = .notSent
                persistDraft()
            }
            return
        }
        if preparingPrompt, pendingPrompt?.phase == .preparing {
            // The checkpoint callback has not crossed the provider boundary.
            // Let it finish its file work, but invalidate this local dispatch.
            pendingPrompt?.phase = .notSent
            completion = .stopped
            persistDraft()
            notifyState()
            return
        }
        guard canStop else { return }
        stopping = true
        notifyState()
        let token = generation
        stopTimeout?.cancel()
        stopTimeout = Task { [weak self, stopTimeoutDuration] in
            do { try await Task.sleep(for: stopTimeoutDuration) } catch { return }
            guard let self, self.generation == token, self.stopping else { return }
            self.settleStop()
            self.error = "The agent has not confirmed that it stopped. Queued work is held; reconnect to inspect its state."
            self.notifyState()
        }
        if !(await perform(ConversationCommand(.cancel))) { settleStop() }
        notifyState()
    }

    func approve(_ approval: ConversationApproval, option: ConversationApproval.Option) async {
        guard snapshot.connected, !submitting, snapshot.approvals.contains(approval), approval.options.contains(option) else { return }
        _ = await perform(ConversationCommand(.approval, text: option.id, requestID: approval.id))
    }

    func respondToElicitation(_ approval: ConversationApproval, response: String) async {
        guard snapshot.connected, !submitting, snapshot.approvals.contains(approval), approval.kind == "mcp_elicitation" else { return }
        _ = await perform(ConversationCommand(.approval, text: response, requestID: approval.id))
    }

    func answer(_ question: ConversationQuestion, text: String) async {
        guard snapshot.connected, !submitting, snapshot.questions.contains(question), let answer = text.nilIfBlank else { return }
        _ = await perform(ConversationCommand(.userInput, text: answer, requestID: question.id))
    }

    private func perform(_ command: ConversationCommand) async -> Bool {
        guard let runtime, !submitting else { return false }
        let token = generation
        submitting = true
        defer {
            if generation == token { submitting = false; notifyState(); scheduleQueueDrain() }
        }
        if command.action == .prompt, let beforePrompt {
            await beforePrompt(session, command)
            guard generation == token, pendingPrompt?.commandID == command.id,
                  pendingPrompt?.phase == .preparing else { return false }
        }
        if command.action == .prompt || command.action == .steer {
            snapshot.pendingPrompt = true
            notifyState()
            // Persist before crossing the provider boundary. A process exit while
            // awaiting acknowledgement must restore a draft that requires review.
            deliveryUncertain = true
            pendingPrompt?.phase = .awaitingConfirmation
            persistDraft()
            await drafts?.flush()
            guard generation == token else { return false }
            guard draftSaveError == nil else {
                deliveryUncertain = false
                pendingPrompt?.phase = .notSent
                snapshot.pendingPrompt = false
                error = "The outgoing message could not be saved. Restore the draft and retry after saving succeeds."
                persistDraft()
                submitting = false
                return false
            }
        }
        do {
            try await runtime.perform(command)
            if command.action == .prompt || command.action == .steer, generation == token {
                // Queuing a command is not confirmation that the provider accepted it.
                reconcilePendingPrompt()
            }
            return generation == token
        } catch {
            guard generation == token else { return false }
            if (error as? ConversationRuntimeError)?.kind == .settingsRejected {
                settingsError = error.localizedDescription
                return false
            }
            self.error = error.localizedDescription
            if command.action == .prompt || command.action == .steer {
                deliveryUncertain = true
                pendingPrompt?.phase = .uncertain
                snapshot.pendingPrompt = false
                persistDraft()
            }
            // A provider can accept a command before its acknowledgement is lost.
            // Reconnect explicitly; never turn an uncertain result into another send.
            snapshot.ready = false
            return false
        }
    }

    func disconnect() async {
        restartRequested = false
        queueHeld = true
        settleStop()
        finishSettings(false)
        generation = UUID()
        checkpointCaptures.removeAll()
        checkpointedPromptIDs.removeAll()
        observedProviderWork = false
        changingHistory = false
        connectionAttempted = true
        finishConnecting(false)
        connecting = false
        submitting = false
        snapshot.connected = false
        snapshot.ready = false
        snapshot.running = false
        snapshot.pendingPrompt = false
        snapshot.controls = nil
        snapshot.approvals = []
        snapshot.questions = []
        let previous = runtime
        runtime = nil
        persistDraft()
        notifyState()
        await previous?.disconnect()
    }
}

@MainActor @Observable
final class LiveConversations {
    private(set) var sessions: [String: LiveConversation] = [:]
    private struct HostedBinding: Equatable {
        let hostID: String
        let endpoint: URL
        let machineID: String
        let conversationID: String?
        init(_ connection: ExecutionHostConnection, conversationID: String?) {
            hostID = connection.id
            endpoint = connection.endpoint
            machineID = connection.machineID
            self.conversationID = conversationID
        }
    }
    @ObservationIgnored private var hostedBindings: [String: HostedBinding] = [:]
    @ObservationIgnored private var hostedRefreshes: [String: UUID] = [:]
    let drafts: ConversationDraftStore
    private let executionHosts: ExecutionHostConnections?
    @ObservationIgnored private let makeConversation: (Session, ConversationDraftStore) -> LiveConversation
    @ObservationIgnored var onNotificationState: ((Session, ConversationNotificationState) -> Void)? {
        didSet { for conversation in sessions.values { conversation.onNotificationState = onNotificationState } }
    }
    @ObservationIgnored var onTransferQueuedPrompt: ((Session, ConversationQueuedPrompt) async throws -> Void)? {
        didSet { for conversation in sessions.values { conversation.onTransferQueuedPrompt = onTransferQueuedPrompt } }
    }
    @ObservationIgnored var beforePrompt: ((Session, ConversationCommand) async -> Void)? {
        didSet { for conversation in sessions.values { conversation.beforePrompt = beforePrompt } }
    }
    @ObservationIgnored var afterTurn: ((Session, ConversationSnapshot) async -> Void)? {
        didSet { for conversation in sessions.values { conversation.afterTurn = afterTurn } }
    }

    var workEndingOnQuit: [LiveConversation] {
        sessions.values.filter { !$0.isServerOwned
            && ($0.isWorking || !$0.snapshot.approvals.isEmpty || !$0.snapshot.questions.isEmpty) }
    }

    init(drafts: ConversationDraftStore = ConversationDraftStore(), executionHosts: ExecutionHostConnections? = nil,
         makeConversation: @escaping (Session, ConversationDraftStore) -> LiveConversation = { LiveConversation(session: $0, drafts: $1) }) {
        self.drafts = drafts
        self.executionHosts = executionHosts
        self.makeConversation = makeConversation
    }

    func prepare(_ session: Session) {
        guard sessions[session.id] == nil else { return }
        if let connection = executionHosts?.connection(for: session) {
            prepareHosted(session, connection: connection)
            return
        }
        guard InAppAgentRuntime.isAvailable, InAppResumeTarget.unavailableReason(for: session) == nil,
              sessions[session.id] == nil else { return }
        let conversation = makeConversation(session, drafts)
        conversation.onNotificationState = onNotificationState
        conversation.onTransferQueuedPrompt = onTransferQueuedPrompt
        conversation.beforePrompt = beforePrompt
        conversation.afterTurn = afterTurn
        sessions[session.id] = conversation
        Task { await conversation.reconcileSavedDelivery() }
    }

    func prepareHosted(_ session: Session, connection: ExecutionHostConnection, conversationID: String? = nil) {
        guard sessions[session.id] == nil else { return }
        let conversation = LiveConversation(session: session, makeRuntime: {
            do {
                let token = try ExecutionHostCredential.read(connection.id)
                return try RemoteConversationRuntime(connection: connection, token: token, conversationID: conversationID)
            } catch { return UnavailableConversationRuntime(message: error.localizedDescription) }
        },
            resolveTarget: { try RemoteConversationRuntime.target(for: $0, connection: connection) },
            checkOwnership: { _ in false }, drafts: drafts, isServerOwned: true)
        conversation.onNotificationState = onNotificationState
        conversation.onTransferQueuedPrompt = onTransferQueuedPrompt
        conversation.beforePrompt = beforePrompt
        conversation.afterTurn = afterTurn
        sessions[session.id] = conversation
        hostedBindings[session.id] = HostedBinding(connection, conversationID: conversationID)
    }

    /// An explicit host read may relocate the same native conversation without
    /// changing its transcript identity. Replace only its stale viewer, retaining
    /// durable draft/queue state and never resuming provider work automatically.
    func refreshHosted(_ session: Session, connection: ExecutionHostConnection, conversationID: String) async throws {
        let requested = HostedBinding(connection, conversationID: conversationID)
        if let current = sessions[session.id] {
            guard current.session.cwd != session.cwd || hostedBindings[session.id] != requested else { return }
            guard current.isServerOwned || !current.isWorking else {
                throw ConversationRuntimeError(message: "Stop this app-owned conversation before opening it on an execution host.")
            }
            let token = UUID()
            hostedRefreshes[session.id] = token
            await current.disconnect()
            await drafts.flush()
            guard hostedRefreshes[session.id] == token, sessions[session.id] === current else { throw CancellationError() }
            hostedRefreshes.removeValue(forKey: session.id)
            if let error = drafts.error { throw ConversationRuntimeError(message: error) }
            sessions.removeValue(forKey: session.id)
        }
        prepareHosted(session, connection: connection, conversationID: conversationID)
    }

    @discardableResult
    func adopt(_ created: CreatedConversation) async -> LiveConversation {
        let conversation = LiveConversation(session: created.session,
            makeRuntime: { created.runtime }, resolveTarget: { _ in created.target },
            drafts: drafts, adoptingCreatedSession: true)
        sessions[created.session.id] = conversation
        conversation.onNotificationState = onNotificationState
        conversation.onTransferQueuedPrompt = onTransferQueuedPrompt
        conversation.beforePrompt = beforePrompt
        conversation.afterTurn = afterTurn
        _ = await conversation.connect()
        return conversation
    }

    func listState(for session: Session) -> ConversationListState {
        if let conversation = sessions[session.id] { return conversation.listState }
        let draft = drafts.drafts[session.id]
        return ConversationListState(activity: draft?.deliveryUncertain == true ? .failed : nil,
                                     hasDraft: draft?.text.nilIfBlank != nil || draft?.attachments.isEmpty == false
                                        || draft?.pendingPrompt != nil || draft?.queue.isEmpty == false)
    }

    func disconnectAll() async {
        for conversation in sessions.values { await conversation.disconnect() }
        await drafts.flush()
    }
}

private actor UnavailableConversationRuntime: ConversationRuntime {
    let message: String
    init(message: String) { self.message = message }
    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) throws {
        throw ConversationRuntimeError(message: message)
    }
    func perform(_ command: ConversationCommand) throws { throw ConversationRuntimeError(message: message) }
    func disconnect() {}
}
