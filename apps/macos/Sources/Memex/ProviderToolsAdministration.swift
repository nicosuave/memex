import Foundation

/// A local installation, never an alias for an imported/remote history source.
struct ProviderToolsInstallation: Equatable, Sendable {
    enum Provider: String, CaseIterable, Sendable { case codex, claude }
    var provider: Provider
    var executable: String
    var home: String
    var workingDirectory: String
    var claudeNativeConfiguration: ClaudeNativeConfiguration? = nil
    var resolvedClaudeConfiguration: ClaudeNativeConfiguration {
        if let claudeNativeConfiguration,
           claudeNativeConfiguration.providerHome == URL(fileURLWithPath: home).resolvingSymlinksInPath() {
            return claudeNativeConfiguration
        }
        return .resolve(providerHome: URL(fileURLWithPath: home))
    }
    var environment: [String: String] {
        provider == .codex ? ["CODEX_HOME": home] : resolvedClaudeConfiguration.environment
    }
    var configurationURL: URL {
        provider == .codex ? URL(fileURLWithPath: home).appendingPathComponent("config.toml")
            : resolvedClaudeConfiguration.configurationURL
    }
    func configurationURL(scope: String) -> URL {
        provider == .claude && scope == "project"
            ? URL(fileURLWithPath: workingDirectory).appendingPathComponent(".mcp.json") : configurationURL
    }
    func validate() throws {
        guard [executable, home, workingDirectory].allSatisfy({ $0.hasPrefix("/") && !$0.contains("\0") }),
              FileManager.default.isExecutableFile(atPath: executable) else {
            throw ConversationRuntimeError(message: "Select an installed provider executable and absolute home/workspace paths.")
        }
        for path in [home, workingDirectory] {
            var directory: ObjCBool = false
            guard FileManager.default.fileExists(atPath: path, isDirectory: &directory), directory.boolValue else {
                throw ConversationRuntimeError(message: "Directory unavailable: \(path)")
            }
        }
    }
    static func local(_ provider: Provider) -> Self {
        let environment = ProcessInfo.processInfo.environment
        let userHome = environment["HOME"]?.nilIfBlank ?? FileManager.default.homeDirectoryForCurrentUser.path
        let key = provider == .codex ? "CODEX_HOME" : "CLAUDE_CONFIG_DIR"
        let override = provider == .codex ? "MEMEX_CODEX_EXECUTABLE" : "MEMEX_CLAUDE_EXECUTABLE"
        let directories = (environment["PATH"] ?? "").split(separator: ":").map(String.init)
            + [userHome + "/.local/bin", userHome + "/.cargo/bin", "/opt/homebrew/bin", "/usr/local/bin"]
        let candidates = environment[override].map { [$0] } ?? directories.map { $0 + "/" + provider.rawValue }
        return .init(provider: provider,
                     executable: candidates.first(where: { FileManager.default.isExecutableFile(atPath: $0) }) ?? "",
                     home: environment[key]?.nilIfBlank ?? userHome + "/." + provider.rawValue, workingDirectory: userHome)
    }
}

struct ProviderToolEntry: Identifiable, Equatable, Sendable {
    var name: String
    var kind: String
    var status: String
    var scope: String = "user"
    var id: String { kind + ":" + scope + ":" + name }
    var canRemove: Bool { ["user", "project", "local"].contains(scope) }
}

struct ProviderMCPDraft: Sendable {
    enum Transport: String, CaseIterable, Sendable { case stdio, http }
    var name = ""
    var transport: Transport = .http
    var endpoint = ""
    var arguments: [String] = []
    var bearerEnvironmentVariable = ""

    func validate() throws {
        guard name.range(of: "^[A-Za-z0-9_][A-Za-z0-9_.-]*$", options: .regularExpression) != nil,
              !endpoint.contains("\0"), !arguments.contains(where: { $0.contains("\0") }) else {
            throw ConversationRuntimeError(message: "Use a server name containing letters, numbers, underscores, dots or hyphens.")
        }
        if transport == .http {
            guard let url = URLComponents(string: endpoint), ["https", "http"].contains(url.scheme ?? ""),
                  url.host?.isEmpty == false, url.user == nil, url.password == nil else {
                throw ConversationRuntimeError(message: "Enter an HTTP(S) endpoint without embedded credentials.")
            }
        } else if endpoint.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
            throw ConversationRuntimeError(message: "Enter the server executable.")
        }
        guard bearerEnvironmentVariable.isEmpty || bearerEnvironmentVariable.range(
            of: "^[A-Za-z_][A-Za-z0-9_]*$", options: .regularExpression) != nil else {
            throw ConversationRuntimeError(message: "Enter an environment variable name, not a token.")
        }
    }
}

struct ProviderToolsCommand: Sendable {
    var arguments: [String]
    var input: Data? = nil
    var timeout: TimeInterval = 60
}

/// Native CLI contracts are intentionally explicit; ACP does not define plugin administration.
enum ProviderToolsCommands {
    static func addMCP(_ draft: ProviderMCPDraft, provider: ProviderToolsInstallation.Provider) throws -> ProviderToolsCommand {
        try draft.validate()
        var arguments = ["mcp", "add"]
        switch provider {
        case .codex:
            arguments += [draft.name]
            if draft.transport == .http {
                arguments += ["--url", draft.endpoint]
                if !draft.bearerEnvironmentVariable.isEmpty { arguments += ["--bearer-token-env-var", draft.bearerEnvironmentVariable] }
            } else { arguments += ["--", draft.endpoint] + draft.arguments }
        case .claude:
            guard draft.bearerEnvironmentVariable.isEmpty else {
                throw ConversationRuntimeError(message: "Claude uses its native MCP login or header configuration; Codex token-variable options do not apply.")
            }
            arguments += ["--scope", "user", "--transport", draft.transport.rawValue, draft.name]
            if draft.transport == .stdio { arguments += ["--"] }
            arguments += [draft.endpoint] + (draft.transport == .stdio ? draft.arguments : [])
        }
        return .init(arguments: arguments)
    }
    static func plugin(install: Bool, name: String, provider: ProviderToolsInstallation.Provider, scope: String = "user") throws -> ProviderToolsCommand {
        guard !name.isEmpty, !name.hasPrefix("-"), !name.contains("\0"), ["user", "project", "local"].contains(scope) else {
            throw ConversationRuntimeError(message: "Enter the exact plugin@marketplace identifier and a supported scope.")
        }
        switch provider {
        case .codex: return .init(arguments: ["plugin", install ? "add" : "remove", name, "--json"])
        case .claude: return .init(arguments: ["plugin", install ? "install" : "uninstall", name, "--scope", scope, "--json"])
        }
    }
    private static func validateServerName(_ name: String) throws {
        guard !name.isEmpty, !name.hasPrefix("-"), !name.contains("\0") else {
            throw ConversationRuntimeError(message: "This server name cannot be passed safely to the provider CLI. Edit its native configuration.")
        }
    }
    static func removeMCP(_ entry: ProviderToolEntry, provider: ProviderToolsInstallation.Provider) throws -> ProviderToolsCommand {
        try validateServerName(entry.name)
        guard entry.canRemove else { throw ConversationRuntimeError(message: "This server's scope is managed by its provider.") }
        return .init(arguments: ["mcp", "remove", entry.name] + (provider == .claude ? ["--scope", entry.scope] : []))
    }
    static func loginMCP(_ entry: ProviderToolEntry) throws -> ProviderToolsCommand {
        try validateServerName(entry.name)
        return .init(arguments: ["mcp", "login", entry.name], timeout: 180)
    }
    static func configureClaudePlugin(name: String, key: String, value: String) throws -> ProviderToolsCommand {
        guard !name.isEmpty, !name.hasPrefix("-"), !name.contains("\0"), !key.isEmpty,
              !value.contains("\n"), !value.contains("\r") else {
            throw ConversationRuntimeError(message: "Enter a plugin, option name and a single-line value.")
        }
        return .init(arguments: ["plugin", "configure", name, "--values-stdin", "--json"],
                     input: try JSONSerialization.data(withJSONObject: [key: value]))
    }
}

/// Run without a shell, drain output to private files, and cap both duration and returned output.
/// Errors intentionally omit native output, which can echo user-supplied secret values.
enum ProviderToolsProcess {
    static func run(_ command: ProviderToolsCommand, installation: ProviderToolsInstallation) async throws -> Data {
        try installation.validate()
        return try await Task.detached(priority: .userInitiated) {
            let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
            defer { try? FileManager.default.removeItem(at: directory) }
            let outputURL = directory.appendingPathComponent("output")
            _ = FileManager.default.createFile(atPath: outputURL.path, contents: nil, attributes: [.posixPermissions: 0o600])
            let output = try FileHandle(forWritingTo: outputURL)
            defer { try? output.close() }
            let process = Process()
            process.executableURL = URL(fileURLWithPath: installation.executable)
            process.arguments = command.arguments
            process.currentDirectoryURL = URL(fileURLWithPath: installation.workingDirectory)
            process.environment = ClaudeNativeConfiguration.mergedEnvironment(installation.environment,
                inherited: ProcessInfo.processInfo.environment)
            process.standardOutput = output
            process.standardError = FileHandle.nullDevice
            let inputURL = directory.appendingPathComponent("input")
            try (command.input ?? Data()).write(to: inputURL)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: inputURL.path)
            let input = try FileHandle(forReadingFrom: inputURL)
            defer { try? input.close() }
            process.standardInput = input
            try process.run()
            let deadline = Date().addingTimeInterval(command.timeout)
            while process.isRunning && Date() < deadline { try await Task.sleep(for: .milliseconds(100)) }
            if process.isRunning {
                process.terminate()
                throw ConversationRuntimeError(message: "The provider command timed out. Check its native CLI before retrying; a change may already have applied.")
            }
            guard process.terminationStatus == 0 else {
                throw ConversationRuntimeError(message: "The provider rejected this command (exit \(process.terminationStatus)). Check the selected installation and its CLI version. Plugins requiring interactive command approval must be installed through that CLI.")
            }
            let reader = try FileHandle(forReadingFrom: outputURL)
            defer { try? reader.close() }
            let data = try reader.read(upToCount: 2_000_001) ?? Data()
            guard data.count <= 2_000_000 else { throw ConversationRuntimeError(message: "Provider output exceeds the inventory limit.") }
            return data
        }.value
    }
}

struct ProviderToolsInventory: Sendable {
    var servers: [ProviderToolEntry]
    var plugins: [ProviderToolEntry]

    static func plugins(from data: Data, provider: ProviderToolsInstallation.Provider, cwd: String) throws -> [ProviderToolEntry] {
        let json = try JSONSerialization.jsonObject(with: data)
        let rows: [[String: Any]]?
        switch provider {
        case .codex: rows = (json as? [String: Any])?["installed"] as? [[String: Any]]
        case .claude: rows = json as? [[String: Any]]
        }
        guard let rows else { throw ConversationRuntimeError(message: "This provider CLI returned an unsupported plugin inventory format.") }
        return rows.compactMap { row in
            guard let name = row[provider == .codex ? "pluginId" : "id"] as? String else { return nil }
            let scope = row["scope"] as? String ?? "user"
            if let project = row["projectPath"] as? String, project != cwd { return nil }
            if ["project", "local"].contains(scope), row["projectPath"] as? String != cwd { return nil }
            var status = (row["enabled"] as? Bool == true) ? "Enabled" : "Disabled"
            if let projectEnabled = row["projectEnabled"] as? Bool {
                status += projectEnabled ? " · workspace enabled" : " · workspace disabled"
            }
            return .init(name: name, kind: "plugin", status: status, scope: scope)
        }
    }
    static func codexServers(from data: Data) throws -> [ProviderToolEntry] {
        guard let rows = try JSONSerialization.jsonObject(with: data) as? [[String: Any]] else {
            throw ConversationRuntimeError(message: "This Codex CLI returned an unsupported MCP inventory format.")
        }
        return rows.compactMap { row in
            guard let name = row["name"] as? String else { return nil }
            let transport = (row["transport"] as? [String: Any])?["type"] as? String ?? "unknown"
            return .init(name: name, kind: transport, status: row["auth_status"] as? String ?? "unknown")
        }
    }
    /// Read configuration only: `claude mcp list` health-checks and starts servers.
    static func claudeServers(installation: ProviderToolsInstallation) throws -> [ProviderToolEntry] {
        func read(_ url: URL) throws -> [String: Any] {
            guard FileManager.default.fileExists(atPath: url.path) else { return [:] }
            guard let json = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as? [String: Any] else {
                throw ConversationRuntimeError(message: "Invalid Claude MCP configuration: \(url.path)")
            }
            return json
        }
        let config = try read(installation.configurationURL)
        let project = try read(URL(fileURLWithPath: installation.workingDirectory).appendingPathComponent(".mcp.json"))
        let local = ((config["projects"] as? [String: Any])?[installation.workingDirectory] as? [String: Any]) ?? [:]
        return [("user", config), ("project", project), ("local", local)].flatMap { scope, json in
            (json["mcpServers"] as? [String: [String: Any]] ?? [:]).map { name, server in
                ProviderToolEntry(name: name, kind: server["type"] as? String ?? "stdio", status: "Configured · authentication not checked", scope: scope)
            }.sorted { $0.name < $1.name }
        }
    }
}

/// Optional local SDK plugins are scoped by native home, not by a global provider label.
enum ProviderLocalPlugins {
    static func key(home: String) -> String {
        "memex.claude.localPlugins." + InAppResumeTarget.digest(URL(fileURLWithPath: home).resolvingSymlinksInPath().path)
    }
    static func paths(home: String, defaults: UserDefaults = .standard) -> [String] { defaults.stringArray(forKey: key(home: home)) ?? [] }
    static func save(_ paths: [String], home: String, defaults: UserDefaults = .standard) throws {
        for path in paths {
            guard path.hasPrefix("/"), !path.contains("\0"), FileManager.default.fileExists(atPath: path + "/.claude-plugin/plugin.json") else {
                throw ConversationRuntimeError(message: "Each local plugin must contain .claude-plugin/plugin.json: \(path)")
            }
        }
        defaults.set(Array(Set(paths)).sorted(), forKey: key(home: home))
    }
}
