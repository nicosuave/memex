import Foundation

#if canImport(SQACPHost)
import SQACP
import SQACPHost
#endif

struct NewConversationRequest: Sendable {
    let provider: String
    let workingDirectory: URL
    var providerHome: URL? = nil
}

struct CreatedConversation: Sendable {
    let session: Session
    let runtime: any ConversationRuntime
    let target: InAppResumeTarget
}

enum NewConversationRuntime {
    /// Read a newly created native transcript before the search index catches up.
    /// This temporary private runtime never connects a provider or claims a writer.
    static func records(for session: Session) async throws -> [TranscriptRecord] {
        if session.source.hasPrefix("acp:"), session.machineID == "local" {
            return try await Task.detached { try ConfiguredConversationHistory.records(for: session) }.value
        }
        #if canImport(SQACPHost)
        return try await Task.detached {
            let source = URL(fileURLWithPath: session.sourcePath)
            guard session.machineID == "local", ["codex", "claude"].contains(session.source),
                  session.sourcePath.hasPrefix("/") else {
                throw ConversationRuntimeError(message: "The native transcript is not available on this machine.")
            }
            guard FileManager.default.fileExists(atPath: source.path) else { return [] }
            let home = try InAppResumeTarget.providerHome(for: source, provider: session.source)
            let temporary = FileManager.default.temporaryDirectory.appendingPathComponent("memex-native-read-" + UUID().uuidString)
            try FileManager.default.createDirectory(at: temporary, withIntermediateDirectories: true,
                                                   attributes: [.posixPermissions: 0o700])
            defer { try? FileManager.default.removeItem(at: temporary) }
            let runtime = try AgentRuntimeClient(databaseURL: temporary.appendingPathComponent("runtime.sqlite"))
            let service = try AgentConversationService(runtime: runtime, archiveURL: temporary.appendingPathComponent("history"), executionHostID: "local")
            let id = "memex-" + InAppResumeTarget.digest(session.id)
            let imported = try service.addSource(agent: session.source, format: "jsonl", url: source,
                nativeNamespace: session.source + ":" + InAppResumeTarget.digest(home.resolvingSymlinksInPath().path),
                nativeSessionID: session.sessionID, sessionID: id)
            guard let conversation = try ConversationProjection.conversation(in: imported, sessionID: id) else {
                throw ConversationRuntimeError(message: "The native transcript could not be read.")
            }
            return ConversationProjection.snapshot(conversation, ready: false, canCancel: false).records
        }.value
        #else
        throw ConversationRuntimeError(message: "This build does not include the local agent runtime.")
        #endif
    }

    static func create(_ request: NewConversationRequest,
                       environment: [String: String] = ProcessInfo.processInfo.environment,
                       applicationSupport: URL? = nil, helperURL: URL? = nil) async throws -> CreatedConversation {
        #if canImport(SQACPHost)
        do {
            return try await createNative(request, environment: environment,
                applicationSupport: applicationSupport, helperURL: helperURL)
        } catch {
            throw conversationProviderError(error)
        }
        #else
        throw ConversationRuntimeError(message: "This build does not include the local agent runtime.")
        #endif
    }

    #if canImport(SQACPHost)
    private static func createNative(_ request: NewConversationRequest, environment: [String: String],
                                     applicationSupport: URL?, helperURL: URL?) async throws -> CreatedConversation {
        try await Task.detached {
            if request.provider.hasPrefix("acp:") {
                return try createConfigured(request, applicationSupport: applicationSupport)
            }
            guard ["codex", "claude"].contains(request.provider) else {
                throw ConversationRuntimeError(message: "Choose an installed conversation provider.")
            }
            let manager = FileManager.default
            let key = request.provider == "codex" ? "CODEX_HOME" : "CLAUDE_CONFIG_DIR"
            var environment = environment
            let userHome = environment["HOME"]?.nilIfBlank.map { URL(fileURLWithPath: $0) }
                ?? manager.homeDirectoryForCurrentUser
            let providerHome = request.providerHome
                ?? environment[key]?.nilIfBlank.map { URL(fileURLWithPath: $0) }
                ?? userHome.appendingPathComponent(request.provider == "codex" ? ".codex" : ".claude")
            if request.provider == "claude" {
                environment.merge(ClaudeNativeConfiguration.resolve(providerHome: providerHome,
                    environment: environment).environment) { _, selected in selected }
            } else if let home = request.providerHome { environment[key] = home.path }
            let cwd = request.workingDirectory.standardizedFileURL.resolvingSymlinksInPath()
            // Resolve configuration before starting a process. This is only a
            // location for validation; no transcript is written at this path.
            let validationPath = providerHome.appendingPathComponent(request.provider == "codex"
                ? "sessions/new.jsonl" : "projects/new/new.jsonl")
            var session = Session(source: request.provider, sessionID: "new", sourcePath: validationPath.path,
                project: cwd.lastPathComponent, cwd: cwd.path)
            let config = try InAppResumeTarget.resolve(session, environment: environment,
                applicationSupport: applicationSupport, helperURL: helperURL, requiresTranscript: false)
            let creation: AgentConversationCreation
            if request.provider == "codex" {
                creation = try .codex(executablePath: config.executableURL.path, cwd: cwd.path, environment: config.environment)
            } else {
                guard let helper = config.helperURL else {
                    throw ConversationRuntimeError(message: "The Claude session helper is missing from this build.")
                }
                creation = try .claude(hostExecutablePath: helper.path, claudeExecutablePath: config.executableURL.path,
                    cwd: cwd.path, environment: config.environment,
                    pluginLocalPaths: ProviderLocalPlugins.paths(home: config.providerHome.path),
                    permissionMode: ConversationComposerPreferences.claudePermissionMode())
            }
            let sourcePath: String
            if request.provider == "codex" {
                guard let path = creation.transcriptPath, path.hasPrefix("/") else {
                    throw ConversationRuntimeError(message: "Codex created a conversation but did not return its native transcript path.")
                }
                sourcePath = path
            } else {
                sourcePath = config.providerHome.appendingPathComponent("projects")
                    .appendingPathComponent(claudeProjectDirectory(cwd.path))
                    .appendingPathComponent(creation.nativeSessionID + ".jsonl").path
            }
            session = Session(source: request.provider, sessionID: creation.nativeSessionID, sourcePath: sourcePath,
                project: cwd.lastPathComponent, lastAt: Date().ISO8601Format(), cwd: cwd.path,
                machine: "local", messageCount: 0, conversationKind: "main")
            let target = try InAppResumeTarget.resolve(session, environment: environment,
                applicationSupport: applicationSupport, helperURL: helperURL, requiresTranscript: false)
            return CreatedConversation(session: session, runtime: NativeConversationRuntime(creation: creation), target: target)
        }.value
    }
    private static func createConfigured(_ request: NewConversationRequest, applicationSupport: URL?) throws -> CreatedConversation {
        guard let profile = try ConversationProviderCatalog.load().configured.first(where: { $0.id == request.provider }) else {
            throw ConversationRuntimeError(message: "This provider configuration is missing. Add it in Conversation providers.")
        }
        try profile.validate()
        let cwd = request.workingDirectory.standardizedFileURL.resolvingSymlinksInPath()
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: cwd.path, isDirectory: &isDirectory), isDirectory.boolValue else {
            throw ConversationRuntimeError(message: "The conversation's working directory is unavailable.")
        }
        let creation = try AgentConversationCreation.acp(provider: profile.id, configuration: profile.acpConfiguration, cwd: cwd.path)
        let support = try applicationSupport ?? FileManager.default.url(for: .applicationSupportDirectory,
            in: .userDomainMask, appropriateFor: nil, create: true).appendingPathComponent("dev.memex.app/Resume")
        let manifestURL = support.appendingPathComponent("Providers").appendingPathComponent(UUID().uuidString + ".json")
        let manifest = ConfiguredConversationManifest(version: 1, provider: profile,
            nativeSessionID: creation.nativeSessionID, workingDirectory: cwd.path, supportsResume: creation.supportsResume)
        try manifest.write(to: manifestURL)
        let session = Session(source: profile.id, sessionID: creation.nativeSessionID, sourcePath: manifestURL.path,
            project: cwd.lastPathComponent, lastAt: Date().ISO8601Format(), cwd: cwd.path,
            machine: "local", messageCount: 0, conversationKind: "main")
        let target = try InAppResumeTarget.resolve(session, applicationSupport: applicationSupport, requiresTranscript: false)
        return CreatedConversation(session: session, runtime: NativeConversationRuntime(creation: creation), target: target)
    }
    #endif

    /// Matches the native SDK's project-directory encoding, including its UTF-16
    /// hash suffix for paths over 200 characters (Claude Agent SDK 0.3.270).
    static func claudeProjectDirectory(_ path: String) -> String {
        let units = Array(path.utf16)
        let encoded = units.map { unit -> Character in
            if (65...90).contains(unit) || (97...122).contains(unit) || (48...57).contains(unit) {
                return Character(UnicodeScalar(unit)!)
            }
            return "-"
        }
        guard encoded.count > 200 else { return String(encoded) }
        let hash = units.reduce(Int32(0)) { ($0 &* 31) &+ Int32($1) }
        return String(encoded.prefix(200)) + "-" + String(abs(Int64(hash)), radix: 36)
    }
}
