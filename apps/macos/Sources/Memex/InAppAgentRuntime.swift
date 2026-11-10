import Foundation

#if canImport(SQACPHost)
import SQACP
import SQACPHost
#endif

enum InAppAgentRuntime {
    static var isAvailable: Bool {
        #if canImport(SQACPHost)
        AgentRuntimeClient.schemaVersion > 0
        #else
        false
        #endif
    }

    static func make() -> any ConversationRuntime {
        #if canImport(SQACPHost)
        NativeConversationRuntime()
        #else
        UnavailableConversationRuntime()
        #endif
    }

    static func confirmedDelivery(_ session: Session, commandID: String) async throws -> Bool {
        #if canImport(SQACPHost)
        guard session.machineID == "local", ["claude", "codex"].contains(session.source) else { return false }
        return try await Task.detached {
            let target = try InAppResumeTarget.resolve(session)
            return try confirmedDelivery(databaseURL: target.storageURL.appendingPathComponent("runtime.sqlite"),
                sessionID: "memex-" + InAppResumeTarget.digest(session.id), commandID: commandID)
        }.value
        #else
        return false
        #endif
    }

    #if canImport(SQACPHost)
    static func confirmedDelivery(databaseURL: URL, sessionID: String, commandID: String) throws -> Bool {
        guard FileManager.default.fileExists(atPath: databaseURL.path) else { return false }
        let runtime = try AgentRuntimeClient(databaseURL: databaseURL)
        func read(_ method: String, _ params: [String: RawTranscriptJSON]) throws -> RawTranscriptJSON {
            let request = RawTranscriptJSON.object(["id": .string(UUID().uuidString),
                "method": .string(method), "params": .object(params)])
            let response = try runtime.requestJSON(request.prettyPrinted())
            let value = try JSONDecoder().decode(RawTranscriptJSON.self, from: Data(response.utf8))
            if let error = value["error"]["message"].string { throw ConversationRuntimeError(message: error) }
            return value["result"]
        }
        let operations = try read("provider_operation.list", ["threadId": .string(sessionID), "includeTerminal": .bool(true)])
        guard let operation = operations.array.first(where: {
            $0["command"]["commandId"].string == commandID && $0["command"]["threadId"].string == sessionID
                && ["thread.turn.start", "thread.turn.steer"].contains($0["command"]["type"].string ?? "")
        }), operation["status"].string == "completed",
              let turnID = operation["command"]["turnId"].string else { return false }
        let thread = try read("thread.snapshot", ["threadId": .string(sessionID)])
        return thread["turns"].array.contains {
            $0["turnId"].string == turnID && $0["nativeTurnId"].string?.isEmpty == false
        }
    }
    #endif
}

private actor UnavailableConversationRuntime: ConversationRuntime {
    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) throws {
        throw ConversationRuntimeError(message: "This build does not include the local agent runtime.")
    }
    func perform(_ command: ConversationCommand) throws {
        throw ConversationRuntimeError(message: "The local agent runtime is unavailable.")
    }
    func disconnect() {}
}

#if canImport(SQACPHost)
func conversationProviderError(_ error: Error) -> ConversationRuntimeError {
    if case AgentConversationServiceError.settingsRejected(let detail) = error {
        return ConversationRuntimeError(message: detail, kind: .settingsRejected)
    }
    let message: String
    if case AgentConversationServiceError.runtime(let detail) = error { message = detail }
    else { message = error.localizedDescription }
    if message.contains("already has an active writer") {
        return ConversationRuntimeError(message: "Open elsewhere", kind: .openElsewhere)
    }
    return ConversationRuntimeError(message: message)
}

/// Owns blocking runtime calls off the main actor. One private archive tracks only
/// the explicitly resumed source; the native provider continues to own its file.
actor NativeConversationRuntime: ConversationRuntime {
    private var creation: AgentConversationCreation?
    private var runtime: AgentRuntimeClient?
    private var service: AgentConversationService?
    private var sessionID: String?
    private var subscription: UUID?
    private var poll: Task<Void, Never>?
    private var drain: Task<Void, Never>?
    private var receive: (@Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void)?
    private var target: InAppResumeTarget?
    private var fileVersion: FileVersion?
    private var connectedAt = Date()
    private var wasReady = false
    private var warning: String?
    private var sentPromptIDs: Set<String> = []
    private var newPromptIDs: Set<String> = []
    private var changingHistory = false
    private let latestPayload = LatestPayload()
    private var control: ControlState?
    private var lastPublish: ContinuousClock.Instant?
    private var publishInterval = minimumPublishInterval
    private var lastSourceRefresh: ContinuousClock.Instant?
    private var running = false

    /// Streaming publishes at most this often, and less often when one publish
    /// is expensive, so reading a long conversation cannot occupy this actor.
    private static let minimumPublishInterval = Duration.milliseconds(50)
    private static let maximumPublishInterval = Duration.milliseconds(250)
    /// The provider's transcript file changes constantly during a turn. Each
    /// refresh re-reads and re-normalizes all of it, and live output already
    /// arrives through the subscription, so mid-turn refreshes are spaced out.
    private static let runningSourceRefreshInterval = Duration.seconds(5)

    init(creation: AgentConversationCreation? = nil) { self.creation = creation }

    /// The newest subscription payload. Written on the serial subscription queue
    /// so arrival order is preserved without depending on task scheduling.
    private final class LatestPayload: @unchecked Sendable {
        private let lock = NSLock()
        private var value: String?
        private var stored = 0
        private var superseded = 0
        func store(_ payload: String) { lock.withLock { value = payload; stored += 1 } }
        /// A direct read is about to happen; payloads stored before it are older.
        func supersede() { lock.withLock { superseded = stored } }
        /// The newest payload, unless a direct read already covered it.
        func take() -> String? {
            lock.withLock {
                defer { value = nil }
                return stored > superseded ? value : nil
            }
        }
        func reset() { lock.withLock { value = nil; superseded = stored } }
    }

    /// Everything a snapshot needs besides the conversation content. It changes
    /// at command and turn boundaries, not with each streamed token.
    private struct ControlState {
        struct Fingerprint: Equatable {
            let connected: Bool
            let running: Bool
            let pendingInteractions: [String]
            init(_ conversation: RawTranscriptJSON) {
                connected = conversation["connected"].bool ?? false
                running = conversation["state"]["running"].bool ?? false
                pendingInteractions = conversation["state"]["pending_interactions"].array.compactMap(\.string)
            }
        }
        let fingerprint: Fingerprint
        let actions: Set<AgentConversationAction>
        let thread: RawTranscriptJSON
        let operations: RawTranscriptJSON
        let deliveries: [ConversationDelivery]
        let controls: ConversationControls?
        let canMutateHistory: Bool
        let children: [ConversationChildHistory]
        let mcpAppConnection: NativeMcpAppConnection?

        /// A prompt, steer or settings change is still waiting for its outcome.
        func awaitsOutcome(sentPromptIDs: Set<String>) -> Bool {
            if controls?.pendingChanges == true { return true }
            let settled = Set(deliveries.filter { ["completed", "failed"].contains($0.status) }.map(\.commandID))
            if !sentPromptIDs.isSubset(of: settled) { return true }
            return operations.array.contains {
                ["thread.turn.start", "thread.turn.steer"].contains($0["command"]["type"].string ?? "")
                    && !["completed", "failed"].contains($0["status"].string ?? "")
            }
        }
    }

    private struct FileVersion: Equatable {
        let size: Int
        let modified: Date?
        init(_ url: URL) throws {
            let values = try url.resourceValues(forKeys: [.fileSizeKey, .contentModificationDateKey])
            size = values.fileSize ?? 0
            modified = values.contentModificationDate
        }
    }

    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) throws {
        let created = creation
        creation = nil
        disconnect()
        self.target = target
        self.receive = receive
        connectedAt = Date()
        wasReady = false
        warning = nil
        try FileManager.default.createDirectory(at: target.storageURL, withIntermediateDirectories: true,
                                               attributes: [.posixPermissions: 0o700])
        let runtime = try AgentRuntimeClient(databaseURL: target.storageURL.appendingPathComponent("runtime.sqlite"))
        let service = try AgentConversationService(runtime: runtime,
            archiveURL: target.storageURL.appendingPathComponent("history"), executionHostID: "local")
        self.runtime = runtime
        self.service = service
        let id = "memex-" + InAppResumeTarget.digest(target.session.id)
        let source: String?
        if created == nil && target.configuredProvider == nil {
            let imported = try service.addSource(agent: target.session.source, format: "jsonl", url: target.sourceURL,
                nativeNamespace: target.providerInstanceID, nativeSessionID: target.session.sessionID, sessionID: id)
            guard let conversation = try ConversationProjection.conversation(in: imported, sessionID: id),
              let sessionEntity = conversation["persisted"].array.first(where: { $0["body"]["kind"].string == "session" }),
              let sourceID = sessionEntity["body"]["data"]["source_ids"].array.first?.string else {
                throw ConversationRuntimeError(message: "The native session could not be identified in its transcript.")
            }
            source = sourceID
        } else {
            source = nil
        }
        sessionID = id
        let binding = AgentConversationBinding(sessionID: id, sourceID: source,
            nativeSessionID: target.session.sessionID, providerInstanceID: target.providerInstanceID,
            executionHostID: "local", workspaceID: target.workspaceID, cwd: target.workingDirectory.path)
        let payloads = latestPayload
        subscription = service.subscribe(on: DispatchQueue(label: "memex.conversation.events")) { [weak self] result in
            // Never carry Foundation Any across actors. Only the newest payload
            // is kept, so a burst cannot project stale events.
            let error: ConversationRuntimeError?
            switch result {
            case .success(let payload): payloads.store(payload); error = nil
            case .failure(let failure): error = conversationProviderError(failure)
            }
            Task { await self?.changed(error: error) }
        }
        do {
            if let created {
                try service.connectCreated(binding, creation: created)
            } else if let provider = target.configuredProvider {
                try service.registerACP(binding, provider: target.session.source)
                try service.connectACP(binding, configuration: provider.acpConfiguration)
            } else if target.session.source == "codex" {
                try service.connectCodex(binding, executablePath: target.executableURL.path, environment: target.environment)
            } else if let helper = target.helperURL {
                try service.connectClaude(binding, hostExecutablePath: helper.path,
                    claudeExecutablePath: target.executableURL.path, environment: target.environment,
                    pluginLocalPaths: ProviderLocalPlugins.paths(home: target.providerHome.path),
                    permissionMode: ConversationComposerPreferences.claudePermissionMode(sessionID: target.session.id))
            }
        } catch {
            throw conversationProviderError(error)
        }
        fileVersion = created == nil && target.configuredProvider == nil ? try? FileVersion(target.sourceURL) : nil
        try publish()
        poll = Task { [weak self] in
            while !Task.isCancelled {
                do { try await Task.sleep(for: .seconds(1)) } catch { return }
                await self?.tick()
            }
        }
    }

    private func changed(error: ConversationRuntimeError?) {
        if let error {
            if error.kind == .settingsRejected { receive?(.failure(error)) }
            else { fail(error); return }
        }
        guard drain == nil, service != nil else { return }
        // Leading edge: the first change after a quiet period publishes at once,
        // and a burst then publishes at most once per interval.
        let wait = lastPublish.map { $0 + publishInterval - .now } ?? .zero
        drain = Task { [weak self] in
            if wait > .zero { do { try await Task.sleep(for: wait) } catch { return } }
            await self?.flush()
        }
    }

    private func flush() {
        drain = nil
        // No payload means a direct read has already published this change.
        guard let payload = latestPayload.take() else { return }
        do { try publish(payload: payload, refreshControl: false) } catch { fail(conversationProviderError(error)) }
    }

    private func shouldRefreshSource() -> Bool {
        guard running, let lastSourceRefresh else { return true }
        return .now - lastSourceRefresh >= Self.runningSourceRefreshInterval
    }

    private func tick() {
        guard let target, let service, let sessionID else { return }
        if target.configuredProvider == nil {
            do {
                let version = try FileVersion(target.sourceURL)
                // A skipped refresh leaves `fileVersion` stale, so the first tick
                // after the turn ends still picks the change up.
                if version != fileVersion, shouldRefreshSource() {
                    lastSourceRefresh = .now
                    if fileVersion == nil {
                        _ = try service.addSource(agent: target.session.source, format: "jsonl", url: target.sourceURL,
                            nativeNamespace: target.providerInstanceID, nativeSessionID: target.session.sessionID, sessionID: sessionID)
                    } else {
                        _ = try service.refresh(sessionID: sessionID)
                    }
                    fileVersion = version
                    warning = nil
                }
            } catch {
                if fileVersion != nil || FileManager.default.fileExists(atPath: target.sourceURL.path) {
                    warning = "Transcript refresh failed: \(error.localizedDescription). Live output is retained."
                }
            }
        }
        if !wasReady && Date().timeIntervalSince(connectedAt) > 45 {
            fail(ConversationRuntimeError(message: "The agent did not finish loading this session. Check its CLI and sign-in, then reconnect."))
            return
        }
        do { try publish() } catch { fail(conversationProviderError(error)) }
    }

    /// A subscription payload is the same envelope `conversation.read` returns
    /// for this session; anything else (the initial list, another session) is read.
    private func conversation(from payload: String?, service: AgentConversationService, sessionID: String) throws -> RawTranscriptJSON? {
        if let payload, let value = try? JSONDecoder().decode(RawTranscriptJSON.self, from: Data(payload.utf8)),
           value["conversation"]["session_id"].string == sessionID {
            return value["conversation"]
        }
        latestPayload.supersede()
        return try ConversationProjection.conversation(in: service.read(sessionID: sessionID), sessionID: sessionID)
    }

    private func readControl(_ fingerprint: ControlState.Fingerprint, service: AgentConversationService, sessionID: String) throws -> ControlState {
        let deliveries = try service.commandStatuses(sessionID: sessionID).filter { $0.action == .prompt || $0.action == .steer }.map {
            ConversationDelivery(commandID: $0.commandID, status: $0.status, error: $0.error,
                                 nativeTurnID: $0.nativeTurnID, nativeMessageID: $0.nativeMessageID)
        }
        return ControlState(fingerprint: fingerprint, actions: service.supportedActions(sessionID: sessionID),
            thread: try request("thread.snapshot", params: ["threadId": .string(sessionID)]),
            operations: try request("provider_operation.list", params: ["threadId": .string(sessionID), "includeTerminal": .bool(false)]),
            deliveries: deliveries,
            controls: try service.settings(sessionID: sessionID).map(ConversationControls.init),
            canMutateHistory: service.supportsConversationMutation(sessionID: sessionID),
            children: service.children(sessionID: sessionID).map(ConversationChildHistory.init),
            mcpAppConnection: service.mcpAppTransportIdentity(sessionID: sessionID).map {
                NativeMcpAppConnection(identity: $0, sessionID: sessionID, service: service)
            })
    }

    /// `refreshControl` is false only for streamed content. Commands, the slow
    /// tick and connection always re-read the control state.
    private func publish(payload: String? = nil, refreshControl: Bool = true) throws {
        guard !changingHistory, let service, let sessionID else { return }
        let started = ContinuousClock.now
        let interval = ChatSignpost.signposter.beginInterval("runtime.publish")
        defer {
            ChatSignpost.signposter.endInterval("runtime.publish", interval)
            let finished = ContinuousClock.now
            lastPublish = finished
            // Spend at most a quarter of this actor's time publishing.
            publishInterval = min(Self.maximumPublishInterval, max(Self.minimumPublishInterval, (finished - started) * 4))
        }
        guard let conversation = try conversation(from: payload, service: service, sessionID: sessionID) else { return }
        let fingerprint = ControlState.Fingerprint(conversation)
        let current: ControlState
        if let cached = control, !refreshControl, wasReady, cached.fingerprint == fingerprint,
           !cached.awaitsOutcome(sentPromptIDs: sentPromptIDs) {
            current = cached
        } else {
            current = try readControl(fingerprint, service: service, sessionID: sessionID)
            control = current
        }
        let actions = current.actions
        let thread = current.thread
        let operations = current.operations
        let deliveries = current.deliveries
        // We observed the complete native stream for turns started and accepted
        // through this connection. Resumed or unconfirmed turns have no such guarantee.
        let ownedLiveUserTurns: Set<String> = target?.session.source == "codex"
            ? Set(deliveries.filter {
                newPromptIDs.contains($0.commandID) && $0.status == "completed"
            }.compactMap(\.nativeTurnID)) : []
        var snapshot = ConversationProjection.snapshot(conversation, thread: thread, operations: operations,
            ready: actions.contains(.prompt), canCancel: actions.contains(.cancel), sentPromptIDs: sentPromptIDs,
            ownedLiveUserTurns: ownedLiveUserTurns)
        snapshot.controls = current.controls
        snapshot.canSteer = actions.contains(.steer)
        snapshot.canMutateHistory = current.canMutateHistory
        snapshot.deliveries = deliveries
        snapshot.childHistories = current.children
        snapshot.mcpAppConnection = current.mcpAppConnection
        snapshot.warning = snapshot.warning ?? warning
        running = snapshot.running
        // Saved at boundaries and on the slow tick, not for every streamed token.
        if snapshot.ready, let target, target.configuredProvider != nil, refreshControl || !snapshot.running {
            try ConfiguredConversationHistory.save(conversation: conversation, target: target)
        }
        if snapshot.ready { wasReady = true }
        receive?(.success(snapshot))
        if wasReady && !snapshot.connected {
            fail(ConversationRuntimeError(message: "The agent process disconnected. Reconnect to reload the native session."))
        }
    }

    private func request(_ method: String, params: [String: RawTranscriptJSON]) throws -> RawTranscriptJSON {
        guard let runtime else { throw ConversationRuntimeError(message: "The agent is disconnected.") }
        let encoded = try RawTranscriptJSON.object(["id": .string(UUID().uuidString), "method": .string(method), "params": .object(params)]).prettyPrinted()
        let response = try runtime.requestJSON(encoded)
        let value = try JSONDecoder().decode(RawTranscriptJSON.self, from: Data(response.utf8))
        if let error = value["error"]["message"].string { throw ConversationRuntimeError(message: error) }
        return value["result"]
    }

    func mutateHistory(_ request: ConversationHistoryMutation) async throws -> Session {
        guard !changingHistory, let service, let sessionID, let target else {
            throw ConversationRuntimeError(message: "The agent is disconnected or already changing history.")
        }
        changingHistory = true
        defer { changingHistory = false }
        let boundary = request.boundary
        let nativeID: String = try await withCheckedThrowingContinuation { continuation in
            do {
                try service.mutateConversation(sessionID: sessionID,
                    boundary: .init(userMessageID: boundary.userMessageID, turnID: boundary.turnID,
                                    nextTurnUserMessageID: boundary.nextUserMessageID, throughEnd: boundary.throughEnd),
                    operation: request.operation == .fork ? .fork : .revert) { result in
                        continuation.resume(with: result)
                    }
            } catch { continuation.resume(throwing: error) }
        }
        let session = try NativeConversationLocation.session(nativeID: nativeID, source: target)
        if request.operation == .revert { disconnect() }
        return session
    }

    func readChild(_ id: String) async throws -> ConversationChildHistory {
        guard let service, let sessionID else { throw ConversationRuntimeError(message: "The parent conversation is disconnected.") }
        try service.readChild(sessionID: sessionID, childID: id)
        guard let child = service.children(sessionID: sessionID).first(where: { $0.id == id }) else {
            throw ConversationRuntimeError(message: "The provider did not identify this child in the parent conversation.")
        }
        return ConversationChildHistory(child)
    }

    func perform(_ command: ConversationCommand) async throws {
        guard let service, let sessionID else { throw ConversationRuntimeError(message: "The agent is disconnected.") }
        let action: AgentConversationAction
        switch command.action {
        case .prompt: action = .prompt
        case .steer: action = .steer
        case .cancel: action = .cancel
        case .approval: action = .approval
        case .userInput: action = .userInput
        case .model: action = .model
        case .configuration: action = .configuration
        }
        if command.action == .prompt || command.action == .steer { sentPromptIDs.insert(command.id) }
        let content: [AcpPromptContentBlock]? = command.attachments.isEmpty ? nil
            : [.text(command.text)] + (try command.attachments.map { try $0.promptContent() })
        let result = try service.perform(action, sessionID: sessionID, commandID: command.id,
            issuedAt: command.issuedAt, text: command.text,
            optionID: command.action == .configuration ? command.requestID : nil,
            requestID: command.requestID, promptContent: content)
        if command.action == .cancel, target?.session.source == "codex" || target?.session.source == "claude" {
            let deadline = ContinuousClock.now.advanced(by: .seconds(10))
            while ContinuousClock.now < deadline {
                guard self.service === service else {
                    throw ConversationRuntimeError(message: "The agent disconnected before acknowledging Stop.")
                }
                if let status = try service.commandStatuses(sessionID: sessionID).first(where: { $0.commandID == command.id }) {
                    if status.status == "completed" { break }
                    if status.status == "failed" {
                        throw ConversationRuntimeError(message: status.error ?? "The agent rejected Stop.")
                    }
                }
                try await Task.sleep(for: .milliseconds(50))
                if ContinuousClock.now >= deadline {
                    throw ConversationRuntimeError(message: "The agent did not acknowledge Stop. Queued work remains held.")
                }
            }
        }
        if command.action == .prompt {
            let receipt = try JSONDecoder().decode(RawTranscriptJSON.self, from: Data(result.utf8))
            if receipt["replayed"].bool == false { newPromptIDs.insert(command.id) }
        }
        try publish()
    }

    private func fail(_ error: ConversationRuntimeError) {
        let callback = receive
        disconnect()
        callback?(.failure(error))
    }

    func disconnect() {
        creation = nil
        sentPromptIDs.removeAll()
        newPromptIDs.removeAll()
        poll?.cancel(); poll = nil
        drain?.cancel(); drain = nil
        latestPayload.reset()
        control = nil
        lastPublish = nil
        publishInterval = Self.minimumPublishInterval
        lastSourceRefresh = nil
        running = false
        if let subscription { service?.unsubscribe(subscription) }
        subscription = nil
        if let sessionID { try? service?.disconnect(sessionID: sessionID) }
        service = nil
        runtime = nil
        sessionID = nil
        receive = nil
        target = nil
    }
}
#endif
