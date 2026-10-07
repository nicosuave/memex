import Foundation
import MemexExecutionHostCore
#if canImport(SQACPHost)
import SQACPHost
#endif

/// Detaching only cancels this viewer's subscription. The paired host owns the
/// provider process and native receipts independently of the desktop lifecycle.
actor RemoteConversationRuntime: ConversationRuntime {
    private let connection: ExecutionHostConnection
    private let client: ExecutionHostHTTPClient
    private var conversationID: String?
    private var poll: Task<Void, Never>?
    private var receive: (@Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void)?
    private var sentCommandIDs: Set<String> = []

    init(connection: ExecutionHostConnection, token: String, conversationID: String? = nil, outbox: URL? = nil) throws {
        self.connection = connection
        client = try ExecutionHostHTTPClient(endpoint: connection.endpoint, token: token, hostID: connection.id, outbox: outbox)
        self.conversationID = conversationID
    }

    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) async throws {
        poll?.cancel()
        self.receive = receive
        if let ssh = connection.ssh {
            _ = try await MainActor.run { try ExecutionHostSSHTunnels.shared.start(ssh) }
        }
        guard target.session.machineID == connection.machineID else {
            throw ConversationRuntimeError(message: "This conversation belongs to a different paired machine.")
        }
        if conversationID == nil {
            let list = try await client.call("conversation.list")
            conversationID = list.array.first {
                $0["nativeSessionID"].string == target.session.sessionID && $0["provider"].string == target.session.source
                    && $0["transcriptPath"].string == target.session.sourcePath
                    && $0["workspaceID"].string == target.workingDirectory.path
                    && $0["cwd"].string == target.workingDirectory.path
            }?["id"].string
        }
        if conversationID == nil {
            let imported = try await client.call("conversation.import", params: [
                "provider": .string(target.session.source), "nativeSessionId": .string(target.session.sessionID),
                "sourcePath": .string(target.session.sourcePath), "workspaceId": .string(target.workingDirectory.path),
                "title": .string(target.session.title)
            ], mutation: true)
            conversationID = imported["conversation"]["id"].string
        }
        guard let id = conversationID else { throw ConversationRuntimeError(message: "The host did not identify the native conversation.") }
        let identity = try await client.call("conversation.read", params: ["conversationId": .string(id)])["conversation"]
        let expectedPath = identity["transcriptPath"].string ?? "host://\(connection.id)/\(target.session.sessionID)"
        guard identity["nativeSessionID"].string == target.session.sessionID,
              identity["provider"].string == target.session.source,
              identity["workspaceID"].string == target.workingDirectory.path,
              identity["cwd"].string == target.workingDirectory.path,
              expectedPath == target.session.sourcePath else {
            throw ConversationRuntimeError(message: "The hosted conversation's provider, transcript, or workspace identity does not match this session.")
        }
        var resumeParameters: [String: HostValue] = ["conversationId": .string(id)]
        if target.session.source == "claude" {
            resumeParameters["claudePermissionMode"] = .string(
                ConversationComposerPreferences.claudePermissionMode(sessionID: target.session.id,
                    hostedMode: identity["claudePermissionMode"].string))
        }
        _ = try await client.call("conversation.resume", params: resumeParameters, mutation: true)
        try await publish()
        poll = Task { [weak self] in
            while !Task.isCancelled {
                do {
                    try await Task.sleep(for: .seconds(1))
                    try await self?.publish()
                } catch is CancellationError { return }
                catch {
                    await self?.report(error)
                    return
                }
            }
        }
    }

    func perform(_ command: ConversationCommand) async throws {
        guard let conversationID else { throw ConversationRuntimeError(message: "The execution host is disconnected.") }
        let operation: String
        switch command.action {
        case .prompt: operation = "send"
        case .steer: operation = "steer"
        case .cancel: operation = "interrupt"
        case .approval: operation = "approval"
        case .userInput: operation = "userInput"
        case .model: operation = "model"
        case .configuration: operation = "configuration"
        }
        var params: [String: HostValue] = ["conversationId": .string(conversationID), "commandId": .string(command.id),
            "issuedAt": .string(command.issuedAt), "text": .string(command.text)]
        if let id = command.requestID {
            params[command.action == .configuration ? "optionId" : "requestId"] = .string(id)
        }
        if !command.attachments.isEmpty {
            params["promptContent"] = .array([.object(["type": .string("text"), "text": .string(command.text)])]
                + (try command.attachments.map { try JSONDecoder().decode(HostValue.self, from: $0.content) }))
        }
        if command.action == .prompt || command.action == .steer { sentCommandIDs.insert(command.id) }
        do {
            _ = try await client.call("conversation." + operation, params: params, mutation: true)
            if command.action == .cancel {
                let deadline = ContinuousClock.now.advanced(by: .seconds(10))
                while true {
                    let state = try await client.call("conversation.read", params: ["conversationId": .string(conversationID)])
                    let delivery = state["deliveries"].array.first { $0["commandId"].string == command.id }
                    if delivery?["status"].string == "completed" { break }
                    if delivery?["status"].string == "failed" {
                        throw HostFailure("interrupt_failed", delivery?["error"].string ?? "The host's agent rejected Stop.")
                    }
                    guard ContinuousClock.now < deadline else {
                        throw HostFailure("interrupt_timeout", "The host's agent did not acknowledge Stop. Queued work remains held.")
                    }
                    try await Task.sleep(for: .milliseconds(100))
                }
            }
            try await publish()
        } catch { throw ConversationRuntimeError(message: error.localizedDescription) }
    }

    func disconnect() {
        poll?.cancel(); poll = nil
        receive = nil
    }

    func readChild(_ id: String) async throws -> ConversationChildHistory {
        #if canImport(SQACPHost)
        guard let conversationID else { throw ConversationRuntimeError(message: "The execution host is disconnected.") }
        let value = try await client.call("conversation.child.read", params: ["conversationId": .string(conversationID), "childId": .string(id)])
        let child = try JSONDecoder().decode(AgentChildConversation.self, from: JSONEncoder().encode(value))
        guard child.id == id else { throw ConversationRuntimeError(message: "The host returned a different native child.") }
        return ConversationChildHistory(child)
        #else
        throw ConversationRuntimeError(message: "Child history requires the agent runtime.")
        #endif
    }

    private func publish() async throws {
        guard let conversationID, let receive else { return }
        let response = try await client.call("conversation.read", params: ["conversationId": .string(conversationID)])
        let raw = try JSONDecoder().decode(RawTranscriptJSON.self, from: JSONEncoder().encode(response))
        let presentation = raw["presentation"]["conversation"]
        let actions = Set(response["actions"].array.compactMap(\.string))
        let deliveries = raw["deliveries"].array.compactMap { item -> ConversationDelivery? in
            guard let id = item["commandId"].string, let status = item["status"].string else { return nil }
            return ConversationDelivery(commandID: id, status: status, error: item["error"].string,
                nativeTurnID: item["nativeTurnId"].string, nativeMessageID: item["nativeMessageId"].string)
        }
        let owned = Set(deliveries.filter { sentCommandIDs.contains($0.commandID) && $0.status == "completed" }.compactMap(\.nativeTurnID))
        var snapshot = ConversationProjection.snapshot(presentation, thread: raw["thread"], operations: raw["operations"],
            ready: response["ready"].bool ?? false, canCancel: actions.contains("cancel"), sentPromptIDs: sentCommandIDs,
            ownedLiveUserTurns: owned)
        snapshot.canSteer = actions.contains("steer")
        snapshot.hostConversationID = conversationID
        #if canImport(SQACPHost)
        snapshot.childHistories = try response["children"].array.map { value in
            ConversationChildHistory(try JSONDecoder().decode(AgentChildConversation.self, from: JSONEncoder().encode(value)))
        }
        #endif
        snapshot.deliveries = deliveries
        snapshot.warning = snapshot.warning ?? response["warning"].string
        snapshot.controls = Self.controls(response["controls"])
        receive(.success(snapshot))
    }

    private func report(_ error: Error) {
        receive?(.failure(ConversationRuntimeError(message: "Execution host connection lost. Work remains on \(connection.name). \(error.localizedDescription)")))
    }

    private static func controls(_ value: HostValue) -> ConversationControls? {
        guard value != .null else { return nil }
        var result = ConversationControls()
        result.models = value["models"].array.compactMap { item in
            guard let id = item["id"].string else { return nil }
            return .init(id: id, title: item["name"].string ?? id)
        }
        result.selectedModelID = value["selectedModelId"].string
        result.configurations = value["configOptions"].array.compactMap { option in
            guard let id = option["id"].string else { return nil }
            return .init(id: id, title: option["name"].string ?? id, category: option["category"].string,
                selectedID: option["currentValue"].string, choices: option["choices"].array.compactMap { choice in
                    guard let id = choice["value"].string else { return nil }
                    return .init(id: id, title: choice["name"].string ?? id)
                })
        }
        result.supportsImages = value["promptCapabilities"]["image"].bool ?? false
        result.supportsAudio = value["promptCapabilities"]["audio"].bool ?? false
        result.supportsFileContents = value["promptCapabilities"]["embeddedContext"].bool ?? false
        result.pendingChanges = !value["pendingControlCommandIds"].array.isEmpty
        result.appliesToNextTurn = value["appliesToNextTurn"].bool ?? false
        result.slashCommands = value["slashCommands"].array.compactMap { command in
            guard let name = command["name"].string else { return nil }
            return .init(name: name, description: command["description"].string ?? "", hint: command["hint"].string)
        }
        return result
    }

    /// Metadata paths remain host-local. They are never checked, opened, or executed
    /// on the viewing machine; only draft persistence uses a local directory.
    nonisolated static func target(for session: Session, connection: ExecutionHostConnection) throws -> InAppResumeTarget {
        guard session.machineID == connection.machineID, let cwd = session.cwd, cwd.hasPrefix("/") else {
            throw ConversationRuntimeError(message: "The paired host does not match this conversation's machine and workspace.")
        }
        let support = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("dev.memex.app/RemoteResume/" + InAppResumeTarget.digest(session.id))
        return InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
            workingDirectory: URL(fileURLWithPath: cwd), providerHome: URL(fileURLWithPath: "/"),
            executableURL: URL(fileURLWithPath: "/"), helperURL: nil, storageURL: support)
    }

    nonisolated static func session(_ value: HostValue, connection: ExecutionHostConnection) throws -> Session {
        guard let provider = value["provider"].string, let native = value["nativeSessionID"].string,
              let cwd = value["cwd"].string else { throw HostFailure("response", "Incomplete hosted conversation identity") }
        return Session(source: provider, sessionID: native, sourcePath: value["transcriptPath"].string ?? "host://\(connection.id)/\(native)",
            project: URL(fileURLWithPath: cwd).lastPathComponent, label: value["title"].string,
            lastAt: value["createdAt"].string, cwd: cwd, machine: connection.machineID, messageCount: 0, conversationKind: "main")
    }
}
