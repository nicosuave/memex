import Foundation

#if canImport(SQACPHost)
import SQACP
import SQACPHost

/// A headless owner of the existing native transports and Rust receipt store.
/// The application and browser are clients; dropping either never stops these objects.
public final class NativeExecutionProvider: ExecutionProvider {
    private let runtime: AgentRuntimeClient
    private let service: AgentConversationService
    private let hostID: String
    private let environment: [String: String]
    private let executables: [String: String]
    private let claudeHelper: String?
    private let retirement: NativeConversationRetirement
    private let importDirectory: URL
    private var connected: Set<String> = []
    private var attachedSources: [String: String] = [:]
    private var handoffFences: [String: NativeHandoffWriterFence] = [:]
    public var providers: [String] {
        ["codex", "claude"].filter { executables[$0] != nil && ($0 != "claude" || claudeHelper != nil) }
    }

    public init(directory: URL, hostID: String, environment: [String: String] = ProcessInfo.processInfo.environment) throws {
        self.hostID = hostID
        self.environment = environment
        retirement = NativeConversationRetirement(directory: directory.appendingPathComponent("retired-native-history"))
        importDirectory = directory.appendingPathComponent("handoff-imports")
        executables = Dictionary(uniqueKeysWithValues: ["codex", "claude"].compactMap { name in
            Self.executable(name, environment: environment).map { (name, $0) }
        })
        claudeHelper = environment["MEMEX_CLAUDE_HELPER"].flatMap { FileManager.default.isExecutableFile(atPath: $0) ? $0 : nil }
        runtime = try AgentRuntimeClient(databaseURL: directory.appendingPathComponent("runtime.sqlite"))
        service = try AgentConversationService(runtime: runtime, archiveURL: directory.appendingPathComponent("history"), executionHostID: hostID)
    }

    public func create(id: String, provider: String, workspaceID: String, cwd: String, title: String) throws -> HostedConversation {
        guard let executable = executables[provider] else { throw HostFailure("provider_unavailable", "Provider executable is unavailable") }
        let creation: AgentConversationCreation
        if provider == "codex" {
            creation = try .codex(executablePath: executable, cwd: cwd, environment: environment)
        } else if provider == "claude", let claudeHelper {
            creation = try .claude(hostExecutablePath: claudeHelper, claudeExecutablePath: executable, cwd: cwd, environment: environment,
                permissionMode: "auto")
        } else { throw HostFailure("provider_unavailable", "Claude helper is not configured on the execution host") }
        let home = providerHome(provider)
        let transcript = creation.transcriptPath ?? (provider == "claude"
            ? home.appendingPathComponent("projects").appendingPathComponent(Self.claudeProjectDirectory(cwd))
                .appendingPathComponent(creation.nativeSessionID + ".jsonl").path : nil)
        let value = HostedConversation(id: id, nativeSessionID: creation.nativeSessionID, provider: provider,
            providerInstanceID: provider + ":" + home.path, workspaceID: workspaceID, cwd: cwd,
            transcriptPath: transcript, title: title, createdAt: Date().ISO8601Format())
        try service.connectCreated(binding(value), creation: creation)
        connected.insert(id)
        return value
    }

    public func resume(_ conversation: HostedConversation) throws {
        guard handoffFences[conversation.id] == nil else { throw HostFailure("handoff_ownership", "This conversation is fenced for handoff; reconcile ownership before resuming") }
        // Retain the original namespace and path; a changed provider home must never
        // silently cause a remote/native session to be recreated under another account.
        guard conversation.providerInstanceID == conversation.provider + ":" + providerHome(conversation.provider).path else {
            throw HostFailure("provider_identity", "Provider home differs from the conversation's original host binding")
        }
        guard let executable = executables[conversation.provider] else { throw HostFailure("provider_unavailable", "Provider executable is unavailable") }
        if let path = conversation.transcriptPath,
           try NativeConversationOwnership.isOpenElsewhere(provider: conversation.provider,
                nativeSessionID: conversation.nativeSessionID, sourceURL: URL(fileURLWithPath: path),
                providerHome: providerHome(conversation.provider)) {
            throw HostFailure("open_elsewhere", "The native conversation has an active writer on this execution host")
        }
        try attachSource(conversation)
        if conversation.provider == "codex" {
            try service.connectCodex(binding(conversation), executablePath: executable, environment: environment,
                                     resumePath: conversation.handoffResume == true ? conversation.transcriptPath : nil)
        } else if conversation.provider == "claude", let claudeHelper {
            try service.connectClaude(binding(conversation), hostExecutablePath: claudeHelper, claudeExecutablePath: executable, environment: environment,
                permissionMode: conversation.claudePermissionMode ?? "auto")
        } else { throw HostFailure("provider_unavailable", "Provider is not configured on this host") }
        connected.insert(conversation.id)
    }

    public func importConversation(id: String, provider: String, nativeSessionID: String, sourcePath: String,
                                   workspaceID: String, cwd: String, title: String) throws -> HostedConversation {
        guard providers.contains(provider) else { throw HostFailure("unsupported_provider", "Provider is not configured on this host") }
        let home = providerHome(provider)
        let source = URL(fileURLWithPath: sourcePath).standardizedFileURL.resolvingSymlinksInPath()
        let root = home.appendingPathComponent(provider == "codex" ? "sessions" : "projects").path + "/"
        guard source.path.hasPrefix(root), source.pathExtension == "jsonl", FileManager.default.fileExists(atPath: source.path) else {
            throw HostFailure("source_denied", "Import requires an existing native transcript under this host's provider home")
        }
        let c = HostedConversation(id: id, nativeSessionID: nativeSessionID, provider: provider,
            providerInstanceID: provider + ":" + home.path, workspaceID: workspaceID, cwd: cwd,
            transcriptPath: source.path, title: title, createdAt: Date().ISO8601Format())
        try attachSource(c)
        let imported = try decode(service.read(sessionID: id))["conversation"]
        let session = imported["persisted"].array.first { $0["body"]["kind"].string == "session" }?["body"]["data"]
        guard session?["native_id"].string == nativeSessionID,
              session?["agent"].string == provider,
              let originalCWD = session?["workspace"].string,
              URL(fileURLWithPath: originalCWD).standardizedFileURL.resolvingSymlinksInPath().path == cwd else {
            throw HostFailure("identity_conflict", "The native transcript does not confirm this provider session and registered workspace")
        }
        return c
    }

    public func read(_ conversation: HostedConversation) throws -> HostValue {
        var warning: HostValue = .null
        do { try attachSource(conversation) } catch { warning = .string(error.localizedDescription) }
        let presentation = try decode(service.read(sessionID: conversation.id))
        if presentation["conversation"]["connected"].bool == false { connected.remove(conversation.id) }
        let thread: HostValue
        do { thread = try request("thread.snapshot", ["threadId": .string(conversation.id)]) }
        catch let error as HostFailure where error.code == "thread_not_found" { thread = .null }
        let actions = service.supportedActions(sessionID: conversation.id)
        let running = thread["turns"].array.contains { ["queued", "running", "cancelling"].contains($0["status"].string ?? "") }
        let statuses = try (thread == .null ? [] : service.commandStatuses(sessionID: conversation.id)).map { status -> HostValue in
            .object(["commandId": .string(status.commandID), "action": .string(status.action.rawValue),
                "status": .string(status.status), "error": status.error.map(HostValue.string) ?? .null,
                "nativeTurnId": status.nativeTurnID.map(HostValue.string) ?? .null,
                "nativeMessageId": status.nativeMessageID.map(HostValue.string) ?? .null])
        }
        var settings: HostValue = .null
        if let controls = try service.settings(sessionID: conversation.id) {
            settings = .object([
                "models": .array(controls.models.map { .object(["id": .string($0.id), "name": .string($0.name)]) }),
                "selectedModelId": controls.selectedModelID.map(HostValue.string) ?? .null,
                "selectedReasoningId": controls.selectedReasoningID.map(HostValue.string) ?? .null,
                "configOptions": .array(controls.configOptions.map { option in .object([
                    "id": .string(option.id), "name": .string(option.name),
                    "description": option.description.map(HostValue.string) ?? .null,
                    "category": option.category.map(HostValue.string) ?? .null,
                    "type": option.type.map(HostValue.string) ?? .null,
                    "currentValue": option.currentValue.map(HostValue.string) ?? .null,
                    "choices": .array(option.choices.map { choice in .object([
                        "value": .string(choice.value), "name": .string(choice.name),
                        "description": choice.description.map(HostValue.string) ?? .null
                    ]) })
                ]) }),
                "promptCapabilities": try .encoded(controls.promptCapabilities),
                "pendingControlCommandIds": .array(controls.pendingControlCommandIDs.sorted().map(HostValue.string)),
                "appliesToNextTurn": .bool(controls.controlsApplyToNextTurn),
                "slashCommands": .array(controls.slashCommands.map { .object(["name": .string($0.name), "description": .string($0.description), "hint": $0.hint.map(HostValue.string) ?? .null]) })
            ])
        }
        let operations = thread == .null ? HostValue.array([]) : (try request("provider_operation.list", ["threadId": .string(conversation.id), "includeTerminal": .bool(false)]))
        return .object(["thread": thread, "presentation": presentation, "deliveries": .array(statuses), "operations": operations,
            "children": try .encoded(service.children(sessionID: conversation.id)),
            "ready": .bool(actions.contains(.prompt)), "connected": .bool(connected.contains(conversation.id)), "running": .bool(running),
            "actions": .array(actions.map { .string($0.rawValue) }), "controls": settings, "warning": warning])
    }

    public func perform(_ command: HostedCommand) throws -> HostValue {
        guard let action = AgentConversationAction(rawValue: command.action) else { throw HostFailure("unsupported_action", command.action) }
        let content: [AcpPromptContentBlock]? = try command.promptContent.map {
            try JSONDecoder().decode([AcpPromptContentBlock].self, from: JSONEncoder().encode($0))
        }
        return try decode(service.perform(action, sessionID: command.conversationID, commandID: command.id,
            issuedAt: command.issuedAt, text: command.text, optionID: command.optionID, requestID: command.requestID,
            promptContent: content))
    }

    public func readChild(_ conversation: HostedConversation, childID: String) throws -> HostValue {
        try service.readChild(sessionID: conversation.id, childID: childID)
        guard let child = service.children(sessionID: conversation.id).first(where: { $0.id == childID }) else {
            throw HostFailure("child_identity", "The provider did not identify this child under the selected native parent")
        }
        return try .encoded(child)
    }

    public func isConnected(_ id: String) -> Bool { connected.contains(id) }

    public func handoffUnavailableReason(_ conversation: HostedConversation) -> String? {
        guard conversation.provider == "codex" else {
            return "This provider has no verified portable native-history and exclusive writer-fence implementation. Claude history currently loads by its original project directory."
        }
        guard executables["codex"] != nil, UUID(uuidString: conversation.nativeSessionID) != nil,
              conversation.transcriptPath != nil else { return "Codex handoff requires an installed provider and its exact persisted native rollout" }
        return nil
    }

    public func detachForHandoff(_ conversation: HostedConversation) throws {
        if let reason = handoffUnavailableReason(conversation) { throw HostFailure("handoff_unsupported", reason) }
        guard conversation.providerInstanceID == "codex:" + providerHome("codex").path else {
            throw HostFailure("provider_identity", "The source provider home no longer matches its original binding")
        }
        if handoffFences[conversation.id] != nil { return }
        if connected.contains(conversation.id) {
            let current = try read(conversation)
            guard current["running"].bool == false, current["operations"].array.isEmpty,
                  current["thread"]["pendingRequests"].array.isEmpty else {
                throw HostFailure("handoff_busy", "Finish active work and pending requests before moving this conversation")
            }
            try service.disconnect(sessionID: conversation.id)
            connected.remove(conversation.id)
        }
        handoffFences[conversation.id] = try NativeHandoffWriterFence(home: providerHome("codex"), nativeSessionID: conversation.nativeSessionID)
    }

    public func exportForHandoff(_ conversation: HostedConversation) throws -> NativeConversationTransfer {
        guard handoffFences[conversation.id] != nil, let path = conversation.transcriptPath else {
            throw HostFailure("handoff_ownership", "Detach and fence the source before exporting native history")
        }
        let source = URL(fileURLWithPath: path).standardizedFileURL
        let root = providerHome("codex").appendingPathComponent("sessions").path + "/"
        guard source.resolvingSymlinksInPath().path == source.path, source.path.hasPrefix(root) else {
            throw HostFailure("source_denied", "Native history must remain under the source provider's canonical session home")
        }
        let transcript = try NativeConversationTransferValidation.read(source)
        let transfer = NativeConversationTransfer(conversation: conversation, transcript: transcript,
            transcriptSHA256: NativeConversationTransferValidation.digest(transcript))
        try NativeConversationTransferValidation.validate(transfer)
        return transfer
    }

    public func retireHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws {
        try detachForHandoff(transfer.conversation)
        try retirement.retire(transfer, operationID: operationID, home: providerHome("codex"))
    }

    public func restoreRetiredHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws {
        try detachForHandoff(transfer.conversation)
        try retirement.restore(transfer, operationID: operationID, home: providerHome("codex"))
    }

    public func adoptHandoff(_ transfer: NativeConversationTransfer, id: String, workspaceID: String, cwd: String) throws -> HostedConversation {
        try NativeConversationTransferValidation.validate(transfer)
        guard id == transfer.conversation.id, executables["codex"] != nil, !connected.contains(id) else {
            throw HostFailure("handoff_identity", "Handoff must retain an idle native identity and requires Codex on the destination")
        }
        let home = providerHome("codex")
        if handoffFences[id] == nil { handoffFences[id] = try NativeHandoffWriterFence(home: home, nativeSessionID: transfer.conversation.nativeSessionID) }
        // Before source commitment this history must not be discoverable by a
        // native client even if this host exits and releases its process lock.
        let staged = importDirectory.appendingPathComponent(transfer.conversation.nativeSessionID + "-" + transfer.transcriptSHA256 + ".jsonl")
        try persistNativeHistory(transfer.transcript, at: staged)
        var result = transfer.conversation
        result.providerInstanceID = "codex:" + home.path
        result.workspaceID = workspaceID; result.cwd = cwd; result.transcriptPath = staged.path
        result.handoffResume = true
        return result
    }

    public func activateHandoff(_ transfer: NativeConversationTransfer, conversation: HostedConversation) throws -> HostedConversation {
        try NativeConversationTransferValidation.validate(transfer)
        try detachForHandoff(conversation)
        let home = providerHome("codex")
        let destination = home.appendingPathComponent("sessions/memex-handoffs")
            .appendingPathComponent(transfer.conversation.nativeSessionID + "-" + transfer.transcriptSHA256 + ".jsonl")
        // This is called only after verified source commitment is durable in
        // the host catalog. Exact-byte retries recover a crash during publish.
        try persistNativeHistory(transfer.transcript, at: destination)
        var result = conversation; result.transcriptPath = destination.path
        try attachSource(result)
        do {
            let snapshot = try request("thread.snapshot", ["threadId": .string(result.id)])
            if let previousWorkspace = snapshot["workspaceId"].string, previousWorkspace != result.workspaceID {
                try service.relocateWorkspace(sessionID: result.id, expectedWorkspaceID: previousWorkspace, workspaceID: result.workspaceID)
            }
        } catch let error as HostFailure where error.code == "thread_not_found" {}
        return result
    }

    private func persistNativeHistory(_ bytes: Data, at destination: URL) throws {
        let directory = destination.deletingLastPathComponent()
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        guard directory.resolvingSymlinksInPath().path == directory.path else { throw HostFailure("source_denied", "Native history directory contains a symbolic link") }
        if FileManager.default.fileExists(atPath: destination.path) {
            guard try NativeConversationTransferValidation.read(destination) == bytes else { throw HostFailure("handoff_identity", "A different native history already exists at the transfer destination") }
        } else {
            try bytes.write(to: destination, options: .withoutOverwriting)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: destination.path)
        }
        let file = try FileHandle(forWritingTo: destination); try file.synchronize(); try file.close()
        try NativeConversationRetirement.synchronizeDirectory(directory)
        try NativeConversationRetirement.synchronizeDirectory(directory.deletingLastPathComponent())
        try NativeConversationRetirement.synchronizeDirectory(directory.deletingLastPathComponent().deletingLastPathComponent())
    }

    public func relocate(_ conversation: HostedConversation, workspaceID: String, cwd: String) throws -> HostedConversation {
        try detachForHandoff(conversation)
        // Imported history may not have a runtime thread yet; its first resume
        // creates one at the destination. Existing projections use the idle CAS.
        do {
            let snapshot = try request("thread.snapshot", ["threadId": .string(conversation.id)])
            if snapshot["workspaceId"].string != workspaceID {
                try service.relocateWorkspace(sessionID: conversation.id, expectedWorkspaceID: conversation.workspaceID, workspaceID: workspaceID)
            }
        } catch let error as HostFailure where error.code == "thread_not_found" {}
        var result = conversation
        result.workspaceID = workspaceID; result.cwd = cwd
        result.handoffResume = true
        return result
    }

    public func releaseHandoffFence(_ conversation: HostedConversation) throws {
        guard conversation.providerInstanceID == conversation.provider + ":" + providerHome(conversation.provider).path else {
            throw HostFailure("provider_identity", "Cannot release another provider home's native writer fence")
        }
        handoffFences.removeValue(forKey: conversation.id)
    }

    private func binding(_ c: HostedConversation) -> AgentConversationBinding {
        AgentConversationBinding(sessionID: c.id, sourceID: nil, nativeSessionID: c.nativeSessionID,
            providerInstanceID: c.providerInstanceID, executionHostID: hostID, workspaceID: c.workspaceID, cwd: c.cwd)
    }
    private func attachSource(_ c: HostedConversation) throws {
        guard let path = c.transcriptPath, FileManager.default.fileExists(atPath: path) else { return }
        if attachedSources[c.id] == path { _ = try service.refresh(sessionID: c.id) }
        else {
            _ = try service.addSource(agent: c.provider, format: "jsonl", url: URL(fileURLWithPath: path),
                nativeNamespace: c.providerInstanceID, nativeSessionID: c.nativeSessionID, sessionID: c.id)
            attachedSources[c.id] = path
        }
    }
    private func request(_ method: String, _ params: [String: HostValue]) throws -> HostValue {
        let request = HostRequest(id: .string(UUID().uuidString), method: method, params: params)
        let data = try JSONEncoder().encode(request)
        let result = try decode(runtime.requestJSON(String(decoding: data, as: UTF8.self)))
        if let message = result["error"]["message"].string { throw HostFailure(result["error"]["code"].string ?? "runtime", message) }
        return result["result"]
    }
    private func decode(_ value: String) throws -> HostValue { try JSONDecoder().decode(HostValue.self, from: Data(value.utf8)) }
    private func providerHome(_ provider: String) -> URL {
        let key = provider == "codex" ? "CODEX_HOME" : "CLAUDE_CONFIG_DIR"
        return URL(fileURLWithPath: environment[key] ?? FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(provider == "codex" ? ".codex" : ".claude").path)
            .standardizedFileURL.resolvingSymlinksInPath()
    }
    private static func executable(_ name: String, environment: [String: String]) -> String? {
        (environment["PATH"] ?? "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin").split(separator: ":").map {
            URL(fileURLWithPath: String($0)).appendingPathComponent(name).path
        }.first { FileManager.default.isExecutableFile(atPath: $0) }
    }
    private static func claudeProjectDirectory(_ path: String) -> String {
        let units = Array(path.utf16)
        let encoded = units.map { unit -> Character in
            if (65...90).contains(unit) || (97...122).contains(unit) || (48...57).contains(unit) { return Character(UnicodeScalar(unit)!) }
            return "-"
        }
        guard encoded.count > 200 else { return String(encoded) }
        let hash = units.reduce(Int32(0)) { ($0 &* 31) &+ Int32($1) }
        return String(encoded.prefix(200)) + "-" + String(abs(Int64(hash)), radix: 36)
    }
}
#endif
