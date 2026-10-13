import Foundation
import Testing
@testable import Memex

struct ClaudeNativeConfigurationTests {
    @Test func defaultNativeHomeKeepsUserConfigOutsideTranscriptDirectory() {
        let user = URL(fileURLWithPath: "/fixture/user")
        let home = user.appendingPathComponent(".claude")
        let selection = ClaudeNativeConfiguration.resolve(providerHome: home, environment: ["HOME": user.path])
        #expect(selection.explicitConfigDirectory == nil)
        #expect(selection.configurationURL == user.appendingPathComponent(".claude.json"))
        #expect(selection.environment == ["HOME": user.path, "CLAUDE_CONFIG_DIR": ""])
        let installation = ProviderToolsInstallation(provider: .claude, executable: "/usr/bin/true",
            home: home.path, workingDirectory: user.path, claudeNativeConfiguration: selection)
        #expect(installation.configurationURL == selection.configurationURL)
        #expect(installation.environment == selection.environment)
    }

    @Test func intentionalExplicitDefaultPathRemainsExplicit() {
        let user = URL(fileURLWithPath: "/fixture/user")
        let home = user.appendingPathComponent(".claude")
        let environment = ["HOME": user.path, "CLAUDE_CONFIG_DIR": home.path]
        let selection = ClaudeNativeConfiguration.resolve(providerHome: home, environment: environment)
        #expect(selection.explicitConfigDirectory == home.path)
        #expect(selection.configurationURL == home.appendingPathComponent(".claude.json"))
        #expect(selection.environment == environment)
    }

    @Test func customTranscriptHomeOverridesUnrelatedCurrentInstallation() {
        let custom = URL(fileURLWithPath: "/fixture/custom")
        let selection = ClaudeNativeConfiguration.resolve(providerHome: custom,
            environment: ["HOME": "/fixture/user", "CLAUDE_CONFIG_DIR": "/other"])
        #expect(selection.explicitConfigDirectory == custom.path)
        #expect(selection.configurationURL == custom.appendingPathComponent(".claude.json"))
        #expect(selection.environment["CLAUDE_CONFIG_DIR"] == custom.path)
    }

    @Test func defaultResumeClearsInheritedCustomOverrideAfterMerge() {
        let inherited = ["HOME": "/fixture/user", "CLAUDE_CONFIG_DIR": "/other", "PATH": "/usr/bin"]
        let selection = ClaudeNativeConfiguration.resolve(providerHome: URL(fileURLWithPath: "/fixture/user/.claude"),
            environment: inherited)
        let child = ClaudeNativeConfiguration.mergedEnvironment(selection.environment, inherited: inherited)
        #expect(child["CLAUDE_CONFIG_DIR"] == nil)
        #expect(child["HOME"] == "/fixture/user")
        #expect(child["PATH"] == "/usr/bin")
        #expect(inherited["CLAUDE_CONFIG_DIR"] == "/other")
    }

    @Test func emptyConfiguredHomeUsesNativeDefault() {
        let selection = ClaudeNativeConfiguration.resolve(providerHome: URL(fileURLWithPath: "/fixture/user/.claude"),
            environment: ["HOME": "/fixture/user", "CLAUDE_CONFIG_DIR": ""])
        #expect(selection.explicitConfigDirectory == nil)
        #expect(selection.configurationURL.path == "/fixture/user/.claude.json")
    }

    @Test func resolvedTargetCapturesDefaultVersusExplicitConfigurationChoice() throws {
        let user = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let home = user.appendingPathComponent(".claude")
        let project = home.appendingPathComponent("projects/work")
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: user) }
        let source = project.appendingPathComponent("session.jsonl")
        try Data().write(to: source)
        let session = Session(source: "claude", sessionID: "session", sourcePath: source.path,
            project: "work", cwd: user.path, machine: "local")
        var environment = ["HOME": user.path, "MEMEX_CLAUDE_EXECUTABLE": "/usr/bin/true"]
        let native = try InAppResumeTarget.resolve(session, environment: environment,
            applicationSupport: user, helperURL: URL(fileURLWithPath: "/usr/bin/true"))
        #expect(native.environment["CLAUDE_CONFIG_DIR"] == "")
        #expect(native.claudeNativeConfiguration?.configurationURL == user.appendingPathComponent(".claude.json"))
        environment["CLAUDE_CONFIG_DIR"] = home.path
        let explicit = try InAppResumeTarget.resolve(session, environment: environment,
            applicationSupport: user, helperURL: URL(fileURLWithPath: "/usr/bin/true"))
        #expect(explicit.environment["CLAUDE_CONFIG_DIR"] == home.path)
        #expect(explicit.claudeNativeConfiguration?.configurationURL == home.appendingPathComponent(".claude.json"))
        #expect(native.environment["CLAUDE_CONFIG_DIR"] == "")
        #expect(native.session.sessionID == explicit.session.sessionID)
        #expect(native.providerHome == explicit.providerHome)
    }
}
