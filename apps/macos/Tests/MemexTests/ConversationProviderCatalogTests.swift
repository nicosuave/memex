import Foundation
import Testing
@testable import Memex

struct ConversationProviderCatalogTests {
    @Test func catalogDoesNotConfuseIngestionWithExecution() throws {
        let defaults = UserDefaults(suiteName: "provider-test-" + UUID().uuidString)!
        defer { defaults.removeObject(forKey: ConversationProviderCatalog.defaultsKey) }
        var catalog = try ConversationProviderCatalog.load(defaults: defaults)
        #expect(catalog.creatableProviders.map(\.id) == ["codex", "claude"])
        let profile = ConfiguredConversationProvider(name: "Local ACP", executablePath: "/usr/bin/true",
            arguments: ["--acp", "argument with spaces"], homePath: FileManager.default.temporaryDirectory.path)
        try profile.validate()
        catalog.configured.append(profile)
        try catalog.save(defaults: defaults)
        let restored = try ConversationProviderCatalog.load(defaults: defaults)
        #expect(restored.configured == [profile])
        #expect(restored.providers.last?.resume == .negotiated)
        #expect(restored.providers.last?.steer == .unavailable)
    }

    @Test func configuredSessionRetainsOriginalHomeAfterCatalogChanges() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let profile = ConfiguredConversationProvider(name: "Agent", executablePath: "/usr/bin/true",
            homePath: directory.path, homeEnvironmentKey: "AGENT_HOME")
        let manifestURL = directory.appendingPathComponent("receipt.json")
        try ConfiguredConversationManifest(version: 1, provider: profile, nativeSessionID: "native-id",
            workingDirectory: directory.path, supportsResume: true).write(to: manifestURL)
        let session = Session(source: profile.id, sessionID: "native-id", sourcePath: manifestURL.path,
            project: "test", cwd: directory.path, machine: "local")
        let target = try InAppResumeTarget.resolve(session, applicationSupport: directory)
        #expect(target.configuredProvider == profile)
        #expect(target.environment == ["AGENT_HOME": directory.path])
        #expect(target.session.sessionID == "native-id")
        let wrong = Session(source: profile.id, sessionID: "different", sourcePath: manifestURL.path,
            project: "test", cwd: directory.path, machine: "local")
        #expect(throws: (any Error).self) { try InAppResumeTarget.resolve(wrong, applicationSupport: directory) }
    }

    @Test func unnegotiatedResumeIsRejectedButFreshCreationCanConnect() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let profile = ConfiguredConversationProvider(name: "Agent", executablePath: "/usr/bin/true", homePath: directory.path)
        let receipt = directory.appendingPathComponent("receipt.json")
        try ConfiguredConversationManifest(version: 1, provider: profile, nativeSessionID: "new-id",
            workingDirectory: directory.path, supportsResume: false).write(to: receipt)
        let session = Session(source: profile.id, sessionID: "new-id", sourcePath: receipt.path,
            project: "test", cwd: directory.path, machine: "local")
        #expect(throws: (any Error).self) { try InAppResumeTarget.resolve(session, applicationSupport: directory) }
        #expect(try InAppResumeTarget.resolve(session, applicationSupport: directory, requiresTranscript: false).configuredProvider == profile)
    }

    @Test func observedACPHistorySurvivesWithoutExecutableOrProviderHome() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let profile = ConfiguredConversationProvider(name: "Offline Agent", executablePath: "/missing/agent",
            homePath: "/missing/original-home")
        let receipt = directory.appendingPathComponent("receipt.json")
        try ConfiguredConversationManifest(version: 1, provider: profile, nativeSessionID: "native-id",
            workingDirectory: "/missing/workspace", supportsResume: false).write(to: receipt)
        let session = Session(source: profile.id, sessionID: "native-id", sourcePath: receipt.path,
            project: "test", cwd: "/missing/workspace", machine: "local")
        let target = InAppResumeTarget(session: session, sourceURL: receipt,
            workingDirectory: URL(fileURLWithPath: "/missing/workspace"), providerHome: URL(fileURLWithPath: profile.homePath),
            executableURL: URL(fileURLWithPath: profile.executablePath), helperURL: nil,
            storageURL: directory.appendingPathComponent(InAppResumeTarget.digest(session.id)), configuredProvider: profile)
        let message: RawTranscriptJSON = .object([
            "role": .string("assistant"),
            "parts": .array([.object(["type": .string("text"), "data": .string("Retained output")])]),
        ])
        let entity: RawTranscriptJSON = .object([
            "item_id": .string("observed-stable-item"), "source_order": .number(1),
            "body": .object(["kind": .string("message"), "data": message]),
        ])
        let conversation: RawTranscriptJSON = .object([
            "session_id": .string("memex-" + InAppResumeTarget.digest(session.id)),
            "key": .object(["native_session_id": .string(session.sessionID), "namespace": .string(target.providerInstanceID)]),
            "ephemeral": .array([entity]),
        ])
        try ConfiguredConversationHistory.save(conversation: conversation, target: target)
        let records = try ConfiguredConversationHistory.records(for: session, applicationSupport: directory)
        #expect(records.count == 1)
        #expect(records.first?.record.text == "Retained output")
    }

    #if canImport(SQACPHost)
    @Test func acpArgumentsArePassedWithoutShellEvaluation() {
        let profile = ConfiguredConversationProvider(name: "Agent", executablePath: "/tmp/agent path",
            arguments: ["$(touch /tmp/not-run)", "--acp"], homePath: "/tmp/home with spaces", homeEnvironmentKey: "AGENT_HOME")
        #expect(profile.acpConfiguration.program == "/usr/bin/env")
        #expect(profile.acpConfiguration.args == ["AGENT_HOME=/tmp/home with spaces", "/tmp/agent path", "$(touch /tmp/not-run)", "--acp"])
    }
    #endif
}
