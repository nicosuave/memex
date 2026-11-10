import Foundation
import Testing
@testable import Memex

struct ProviderToolsAdministrationTests {
    private func temporaryDirectory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory
    }

    @Test func nativeCommandsKeepProviderContractsAndLiteralArguments() throws {
        let draft = ProviderMCPDraft(name: "fixture", transport: .stdio, endpoint: "/tool with spaces",
            arguments: ["$(touch /tmp/never)", "", "--flag"])
        #expect(try ProviderToolsCommands.addMCP(draft, provider: .codex).arguments ==
            ["mcp", "add", "fixture", "--", "/tool with spaces", "$(touch /tmp/never)", "", "--flag"])
        #expect(try ProviderToolsCommands.addMCP(draft, provider: .claude).arguments ==
            ["mcp", "add", "--scope", "user", "--transport", "stdio", "fixture", "--", "/tool with spaces", "$(touch /tmp/never)", "", "--flag"])
        #expect(try ProviderToolsCommands.plugin(install: true, name: "x@market", provider: .codex).arguments ==
            ["plugin", "add", "x@market", "--json"])
        #expect(try ProviderToolsCommands.plugin(install: false, name: "x@market", provider: .claude, scope: "project").arguments ==
            ["plugin", "uninstall", "x@market", "--scope", "project", "--json"])
    }

    @Test func rejectsOptionInjectionAndCrossProviderConfiguration() throws {
        #expect(throws: (any Error).self) {
            try ProviderToolsCommands.addMCP(.init(name: "--help", endpoint: "https://example.com"), provider: .codex)
        }
        #expect(throws: (any Error).self) {
            try ProviderToolsCommands.addMCP(.init(name: "safe", endpoint: "https://secret@example.com"), provider: .codex)
        }
        #expect(throws: (any Error).self) {
            try ProviderToolsCommands.addMCP(.init(name: "safe", endpoint: "https://example.com", bearerEnvironmentVariable: "TOKEN"), provider: .claude)
        }
        #expect(throws: (any Error).self) {
            try ProviderToolsCommands.plugin(install: true, name: "--help", provider: .claude)
        }
    }

    @Test func inventoryProjectsOnlySafeFieldsAndExactWorkspaceScope() throws {
        let codex = Data(#"[{"name":"one","transport":{"type":"streamable_http","http_headers":{"Authorization":"secret"}},"auth_status":"oAuth"}]"#.utf8)
        #expect(try ProviderToolsInventory.codexServers(from: codex) == [.init(name: "one", kind: "streamable_http", status: "oAuth")])
        let claude = Data(#"[{"id":"one@market","scope":"user","enabled":true},{"id":"two@market","scope":"project","projectPath":"/other","enabled":true},{"id":"three@market","scope":"project","projectPath":"/exact","enabled":false}]"#.utf8)
        let plugins = try ProviderToolsInventory.plugins(from: claude, provider: .claude, cwd: "/exact")
        #expect(plugins.map(\.name) == ["one@market", "three@market"])
        #expect(plugins.last?.scope == "project")
        #expect(plugins.last?.status == "Disabled")
        let codexPlugins = Data(#"{"installed":[{"pluginId":"one@market","enabled":true}]}"#.utf8)
        #expect(try ProviderToolsInventory.plugins(from: codexPlugins, provider: .codex, cwd: "/exact").first?.name == "one@market")
        #expect(throws: (any Error).self) { try ProviderToolsInventory.plugins(from: Data("{}".utf8), provider: .claude, cwd: "/") }
    }

    @Test func claudeInventoryReadsSelectedHomeAndAllNativeScopesWithoutLaunchingServers() throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let workspace = directory.appendingPathComponent("workspace")
        try FileManager.default.createDirectory(at: workspace, withIntermediateDirectories: true)
        let installation = ProviderToolsInstallation(provider: .claude, executable: "/usr/bin/true", home: directory.path, workingDirectory: workspace.path)
        let config: [String: Any] = ["mcpServers": ["user": ["type": "http", "url": "https://example.invalid", "headers": ["Authorization": "secret"]]],
            "projects": [workspace.path: ["mcpServers": ["local": ["command": "/must/not/run"]]]]]
        try JSONSerialization.data(withJSONObject: config).write(to: installation.configurationURL)
        try Data(#"{"mcpServers":{"project":{"command":"/must/not/run"}}}"#.utf8).write(to: workspace.appendingPathComponent(".mcp.json"))
        let entries = try ProviderToolsInventory.claudeServers(installation: installation)
        #expect(entries.map(\.scope) == ["user", "project", "local"])
        #expect(entries.map(\.name) == ["user", "project", "local"])
        #expect(installation.configurationURL(scope: "project") == workspace.appendingPathComponent(".mcp.json"))
        #expect(try ProviderToolsCommands.removeMCP(entries[1], provider: .claude).arguments ==
            ["mcp", "remove", "project", "--scope", "project"])
    }

    @Test func pluginOptionSecretUsesStdinNotProcessArguments() throws {
        let command = try ProviderToolsCommands.configureClaudePlugin(name: "one@market", key: "apiKey", value: "secret")
        #expect(!command.arguments.contains("secret"))
        let values = try JSONSerialization.jsonObject(with: #require(command.input)) as? [String: String]
        #expect(values == ["apiKey": "secret"])
    }

    @Test func localSDKPluginsRemainScopedToTheOriginalProviderHome() throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let manifest = directory.appendingPathComponent(".claude-plugin")
        try FileManager.default.createDirectory(at: manifest, withIntermediateDirectories: true)
        try Data("{}".utf8).write(to: manifest.appendingPathComponent("plugin.json"))
        let defaults = UserDefaults(suiteName: "provider-tools-" + UUID().uuidString)!
        defer { defaults.removeObject(forKey: ProviderLocalPlugins.key(home: "/original")) }
        try ProviderLocalPlugins.save([directory.path], home: "/original", defaults: defaults)
        #expect(ProviderLocalPlugins.paths(home: "/original", defaults: defaults) == [directory.path])
        #expect(ProviderLocalPlugins.paths(home: "/another", defaults: defaults).isEmpty)
    }

    @Test func processUsesExactHomeWorkingDirectoryAndArgv() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let executable = directory.appendingPathComponent("fake-provider")
        try Data("#!/bin/sh\nprintf '%s\\n' \"$CODEX_HOME\" \"$PWD\" \"$1\"\ncat\n".utf8).write(to: executable)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        let installation = ProviderToolsInstallation(provider: .codex, executable: executable.path,
            home: directory.path, workingDirectory: directory.path)
        let literal = "$(touch /tmp/memex-provider-not-executed)"
        let output = try await ProviderToolsProcess.run(.init(arguments: [literal], input: Data("stdin".utf8)), installation: installation)
        let lines = String(decoding: output, as: UTF8.self).components(separatedBy: "\n")
        #expect(lines[0] == directory.path)
        #expect(URL(fileURLWithPath: lines[1]).resolvingSymlinksInPath() == directory.resolvingSymlinksInPath())
        #expect(lines[2] == literal)
        #expect(lines[3] == "stdin")
    }

    @Test func processFailureDoesNotLeakNativeOutput() async throws {
        let directory = try temporaryDirectory()
        defer { try? FileManager.default.removeItem(at: directory) }
        let executable = directory.appendingPathComponent("fake-provider")
        try Data("#!/bin/sh\nprintf 'private-secret'\nexit 7\n".utf8).write(to: executable)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        do {
            _ = try await ProviderToolsProcess.run(.init(arguments: []), installation: .init(provider: .claude,
                executable: executable.path, home: directory.path, workingDirectory: directory.path))
            Issue.record("Expected native failure")
        } catch {
            #expect(error.localizedDescription.contains("exit 7"))
            #expect(!error.localizedDescription.contains("private-secret"))
        }
    }
}
