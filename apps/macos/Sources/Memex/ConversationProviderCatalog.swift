import Foundation
#if canImport(SQACPHost)
import SQACP
#endif

/// Executable providers are explicitly configured. Ingestion formats alone never
/// become launchable agents, and ACP capabilities are negotiated by the server.
struct ConversationProviderDescriptor: Identifiable, Equatable, Sendable {
    enum Capability: String, Sendable { case supported, negotiated, unavailable }
    let id: String
    let name: String
    let transport: String
    let create: Capability
    let resume: Capability
    let steer: Capability
    let configuration: Capability
    var executablePath: String? = nil

    var capabilitySummary: String {
        if transport == "ACP over stdio" {
            return "Create via ACP · resume, models and permissions negotiated at connection · steering unavailable"
        }
        return "Native create, resume and steering · models and permissions discovered from the provider"
    }
}

struct ConfiguredConversationProvider: Codable, Equatable, Identifiable, Sendable {
    var id: String = "acp:" + UUID().uuidString.lowercased()
    var name: String
    var executablePath: String
    var arguments: [String] = []
    var homePath: String
    var homeEnvironmentKey: String = "HOME"

    var descriptor: ConversationProviderDescriptor {
        .init(id: id, name: name, transport: "ACP over stdio", create: .supported,
              resume: .negotiated, steer: .unavailable, configuration: .negotiated,
              executablePath: executablePath)
    }

    /// Configuration stays visible when a binary or home disappears; history is
    /// still readable and retains the original launch receipt.
    var availabilityIssue: String? {
        do { try validate(); return nil } catch { return error.localizedDescription }
    }

    func validate(requireExecutable: Bool = true) throws {
        guard id.hasPrefix("acp:"), UUID(uuidString: String(id.dropFirst(4))) != nil,
              name.nilIfBlank != nil, executablePath.hasPrefix("/"), homePath.hasPrefix("/"),
              !([name, executablePath, homePath] + arguments).contains(where: { $0.contains("\0") }),
              homeEnvironmentKey.range(of: "^[A-Za-z_][A-Za-z0-9_]*$", options: .regularExpression) != nil else {
            throw ConversationRuntimeError(message: "Choose a name, absolute executable and home paths, and a valid home environment variable.")
        }
        if requireExecutable && !FileManager.default.isExecutableFile(atPath: executablePath) {
            throw ConversationRuntimeError(message: "The configured ACP executable is unavailable: \(executablePath)")
        }
        var isDirectory: ObjCBool = false
        guard FileManager.default.fileExists(atPath: homePath, isDirectory: &isDirectory), isDirectory.boolValue else {
            throw ConversationRuntimeError(message: "The provider's original home directory is unavailable: \(homePath)")
        }
    }

    #if canImport(SQACPHost)
    var acpConfiguration: AcpClient.Configuration {
        // exec argv, not a shell command. The ACP client currently has no explicit
        // environment API; env preserves inherited auth while pinning this home.
        .init(program: "/usr/bin/env", args: [homeEnvironmentKey + "=" + homePath, executablePath] + arguments)
    }
    #endif
}

struct ConversationProviderCatalog: Sendable {
    var configured: [ConfiguredConversationProvider] = []
    static let defaultsKey = "memex.conversation.providers.v1"
    static let builtins: [ConversationProviderDescriptor] = [
        .init(id: "codex", name: "Codex", transport: "App server", create: .supported,
              resume: .supported, steer: .supported, configuration: .negotiated),
        .init(id: "claude", name: "Claude Code", transport: "Agent SDK", create: .supported,
              resume: .supported, steer: .supported, configuration: .negotiated),
    ]
    var providers: [ConversationProviderDescriptor] { Self.builtins + configured.map(\.descriptor) }
    var creatableProviders: [ConversationProviderDescriptor] { providers.filter { $0.create == .supported } }

    static func load(defaults: UserDefaults = .standard) throws -> Self {
        guard let data = defaults.data(forKey: defaultsKey) else { return Self() }
        let providers = try JSONDecoder().decode([ConfiguredConversationProvider].self, from: data)
        guard Set(providers.map(\.id)).count == providers.count else {
            throw ConversationRuntimeError(message: "The saved provider catalog contains duplicate identities.")
        }
        // Missing tools/homes remain visible so users can repair configuration.
        return Self(configured: providers)
    }

    func save(defaults: UserDefaults = .standard) throws {
        guard Set(configured.map(\.id)).count == configured.count else {
            throw ConversationRuntimeError(message: "Provider identities must be unique.")
        }
        defaults.set(try JSONEncoder().encode(configured), forKey: Self.defaultsKey)
    }

    static func name(for id: String) -> String {
        guard let catalog = try? load() else { return id }
        return catalog.providers.first(where: { $0.id == id })?.name ?? id
    }
}

/// A durable launch receipt, not a native transcript. An existing chat retains
/// its exact provider installation even if the editable catalog changes later.
struct ConfiguredConversationManifest: Codable, Sendable {
    let version: Int
    let provider: ConfiguredConversationProvider
    let nativeSessionID: String
    let workingDirectory: String
    let supportsResume: Bool

    func write(to url: URL) throws {
        try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true,
                                                attributes: [.posixPermissions: 0o700])
        try JSONEncoder().encode(self).write(to: url, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
    }

    static func read(for session: Session) throws -> Self {
        let manifest = try JSONDecoder().decode(Self.self, from: Data(contentsOf: URL(fileURLWithPath: session.sourcePath)))
        guard manifest.version == 1, manifest.provider.id == session.source,
              manifest.nativeSessionID == session.sessionID, manifest.workingDirectory == session.cwd else {
            throw ConversationRuntimeError(message: "The saved provider identity does not match this conversation.")
        }
        return manifest
    }
}
