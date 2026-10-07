import CryptoKit
import Foundation

/// The transcript's owning installation, not the GUI process's default agent home.
struct InAppResumeTarget: Sendable, Equatable {
    let session: Session
    let sourceURL: URL
    let workingDirectory: URL
    let providerHome: URL
    let executableURL: URL
    let helperURL: URL?
    let storageURL: URL
    var configuredProvider: ConfiguredConversationProvider? = nil
    var claudeNativeConfiguration: ClaudeNativeConfiguration? = nil

    var environment: [String: String] {
        if let configuredProvider { return [configuredProvider.homeEnvironmentKey: providerHome.path] }
        if session.source == "claude" {
            return (claudeNativeConfiguration ?? .resolve(providerHome: providerHome)).environment
        }
        return ["CODEX_HOME": providerHome.path]
    }
    var providerInstanceID: String { "\(session.source):\(Self.digest(providerHome.path))" }
    var workspaceID: String { Self.digest(workingDirectory.path) }

    static func unavailableReason(for session: Session) -> String? {
        guard session.machineID == "local" else { return "Resume this conversation on \(session.machineID)." }
        guard ["codex", "claude"].contains(session.source) || session.source.hasPrefix("acp:") else { return "Configure an ACP provider to create live conversations with this agent. Imported history alone does not identify an executable provider." }
        guard !isArchived(session) else { return "Unarchive this conversation in Codex to continue here." }
        guard !session.isSubagent, session.conversationKind != "guardian_review" else {
            return "Resume the parent conversation to continue this agent's work."
        }
        guard session.cwd?.nilIfBlank != nil else { return "The original working directory is unavailable." }
        return nil
    }

    static func isArchived(_ session: Session) -> Bool {
        session.source == "codex" && URL(fileURLWithPath: session.sourcePath).pathComponents.contains("archived_sessions")
    }

    static func resolve(_ session: Session, environment: [String: String] = ProcessInfo.processInfo.environment,
                        applicationSupport: URL? = nil, helperURL: URL? = nil,
                        requiresTranscript: Bool = true) throws -> Self {
        if let reason = unavailableReason(for: session) { throw ConversationRuntimeError(message: reason) }
        if session.source.hasPrefix("acp:") {
            return try resolveConfigured(session, applicationSupport: applicationSupport, requiresResume: requiresTranscript)
        }
        guard session.sessionID.nilIfBlank != nil, !session.sessionID.contains("\0"),
              session.sourcePath.hasPrefix("/"), !session.sourcePath.contains("\0"),
              let cwd = session.cwd, cwd.hasPrefix("/"), !cwd.contains("\0") else {
            throw ConversationRuntimeError(message: "This conversation has invalid resume metadata.")
        }
        let manager = FileManager.default
        let nativeSourceURL = URL(fileURLWithPath: session.sourcePath).standardizedFileURL
        let sourceURL = nativeSourceURL.resolvingSymlinksInPath()
        let workingDirectory = URL(fileURLWithPath: cwd).standardizedFileURL.resolvingSymlinksInPath()
        var directory: ObjCBool = false
        guard manager.fileExists(atPath: workingDirectory.path, isDirectory: &directory), directory.boolValue else {
            throw ConversationRuntimeError(message: "The original working directory no longer exists: \(cwd)")
        }
        guard !requiresTranscript || (manager.fileExists(atPath: sourceURL.path, isDirectory: &directory) && !directory.boolValue) else {
            throw ConversationRuntimeError(message: "The original agent session file is unavailable: \(session.sourcePath)")
        }
        // A session directory can be a symlink onto another volume. Its parent
        // installation still owns configuration and auth, not the storage volume.
        let providerHome = try providerHome(for: nativeSourceURL, provider: session.source).resolvingSymlinksInPath()
        let override = environment[session.source == "codex" ? "MEMEX_CODEX_EXECUTABLE" : "MEMEX_CLAUDE_EXECUTABLE"]
        let home = manager.homeDirectoryForCurrentUser
        let searchDirectories = (environment["PATH"] ?? "").split(separator: ":").map(String.init)
            + [home.appendingPathComponent(".local/bin").path, home.appendingPathComponent(".cargo/bin").path,
               "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"]
        let executable = override.map { [$0] } ?? searchDirectories.map { URL(fileURLWithPath: $0).appendingPathComponent(session.source).path }
        guard let executablePath = executable.first(where: { $0.hasPrefix("/") && manager.isExecutableFile(atPath: $0) }) else {
            throw ConversationRuntimeError(message: "\(session.source == "codex" ? "Codex" : "Claude Code") is not installed. Install its CLI or set the corresponding MEMEX executable override.")
        }
        let helper: URL?
        if session.source == "claude" {
            helper = helperURL ?? environment["MEMEX_CLAUDE_HELPER"].map { URL(fileURLWithPath: $0) }
                ?? Bundle.main.bundleURL.appendingPathComponent("Contents/Helpers/claude-agent-sdk-host")
            guard let helper, manager.isExecutableFile(atPath: helper.path) else {
                throw ConversationRuntimeError(message: "The Claude session helper is missing from this build. Rebuild Memex with its local agent runtime.")
            }
        } else { helper = nil }
        let support = try applicationSupport ?? manager.url(for: .applicationSupportDirectory, in: .userDomainMask,
                                                             appropriateFor: nil, create: true)
            .appendingPathComponent("dev.memex.app/Resume", isDirectory: true)
        return Self(session: session, sourceURL: sourceURL, workingDirectory: workingDirectory,
                    providerHome: providerHome, executableURL: URL(fileURLWithPath: executablePath), helperURL: helper,
                    storageURL: support.appendingPathComponent(digest(session.id), isDirectory: true),
                    claudeNativeConfiguration: session.source == "claude"
                        ? .resolve(providerHome: providerHome, environment: environment) : nil)
    }

    private static func resolveConfigured(_ session: Session, applicationSupport: URL?, requiresResume: Bool) throws -> Self {
        let manifest = try ConfiguredConversationManifest.read(for: session)
        guard !requiresResume || manifest.supportsResume else {
            throw ConversationRuntimeError(message: "This ACP provider did not negotiate session resume. Its saved conversation remains readable.")
        }
        try manifest.provider.validate()
        let cwd = URL(fileURLWithPath: manifest.workingDirectory)
        var directory: ObjCBool = false
        guard manifest.workingDirectory.hasPrefix("/"),
              FileManager.default.fileExists(atPath: cwd.path, isDirectory: &directory), directory.boolValue else {
            throw ConversationRuntimeError(message: "The original working directory is unavailable.")
        }
        let support = try applicationSupport ?? FileManager.default.url(for: .applicationSupportDirectory,
            in: .userDomainMask, appropriateFor: nil, create: true).appendingPathComponent("dev.memex.app/Resume")
        return Self(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath), workingDirectory: cwd,
            providerHome: URL(fileURLWithPath: manifest.provider.homePath),
            executableURL: URL(fileURLWithPath: manifest.provider.executablePath), helperURL: nil,
            storageURL: support.appendingPathComponent(digest(session.id)), configuredProvider: manifest.provider)
    }

    static func providerHome(for sourceURL: URL, provider: String) throws -> URL {
        var directory = sourceURL.deletingLastPathComponent()
        if provider == "claude" {
            // Claude stores <config>/projects/<encoded workspace>/<session>.jsonl.
            directory.deleteLastPathComponent()
            if directory.lastPathComponent == "projects" { return directory.deletingLastPathComponent() }
        } else if provider == "codex" {
            while directory.path != "/" {
                if ["sessions", "archived_sessions"].contains(directory.lastPathComponent) {
                    return directory.deletingLastPathComponent()
                }
                directory.deleteLastPathComponent()
            }
        }
        throw ConversationRuntimeError(message: "This transcript is outside its agent's native session store. Resume requires the original installation and session data.")
    }

    static func digest(_ value: String) -> String {
        SHA256.hash(data: Data(value.utf8)).map { String(format: "%02x", $0) }.joined()
    }
}
