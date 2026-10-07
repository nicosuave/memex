import Foundation
import XCTest
@testable import MemexExecutionHostCore

#if canImport(SQACPHost)
import SQACPHost

/// These checks use the real archive and Rust runtime. The only executable in
/// PATH is a disposable sentinel, so a validation failure cannot launch a user agent.
final class NativeExecutionProviderTests: XCTestCase {
    private func fixture() throws -> (NativeExecutionProvider, URL, URL, URL) {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-native-host-" + UUID().uuidString)
            .resolvingSymlinksInPath()
        let home = root.appendingPathComponent("provider-home")
        let workspace = root.appendingPathComponent("workspace")
        let binary = root.appendingPathComponent("bin")
        let runtime = root.appendingPathComponent("runtime")
        for directory in [home.appendingPathComponent("sessions"), workspace, binary, runtime] {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        }
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        let sentinel = binary.appendingPathComponent("codex")
        try "#!/bin/sh\n/usr/bin/touch \"$CODEX_HOME/provider-was-launched\"\nexit 1\n".write(to: sentinel, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: sentinel.path)
        let provider = try NativeExecutionProvider(directory: runtime, hostID: "fixture-host",
            environment: ["PATH": binary.path, "CODEX_HOME": home.path])
        return (provider, home, workspace, root)
    }

    private func transcript(home: URL, nativeID: String, cwd: URL) throws -> URL {
        let file = home.appendingPathComponent("sessions/rollout-" + UUID().uuidString + ".jsonl")
        let metadata: [String: Any] = ["type": "session_meta", "timestamp": "2026-10-05T12:00:00Z",
            "payload": ["id": nativeID, "cwd": cwd.path, "originator": "codex_cli_rs", "timestamp": "2026-10-05T12:00:00Z"]]
        var data = try JSONSerialization.data(withJSONObject: metadata)
        data.append(10)
        try data.write(to: file)
        return file
    }

    func testNativeTransferStagesOutsideLookupAndReturnRebindsExactPathWithoutPrompt() throws {
        let (sourceProvider, sourceHome, sourceWorkspace, _) = try fixture()
        let (targetProvider, targetHome, targetWorkspace, _) = try fixture()
        let native = UUID().uuidString.lowercased()
        let sourcePath = try transcript(home: sourceHome, nativeID: native, cwd: sourceWorkspace)
        let source = try sourceProvider.importConversation(id: "same-native-chat", provider: "codex", nativeSessionID: native,
            sourcePath: sourcePath.path, workspaceID: sourceWorkspace.path, cwd: sourceWorkspace.path, title: "Move")
        try sourceProvider.detachForHandoff(source)
        let exported = try sourceProvider.exportForHandoff(source)
        let staged = try targetProvider.adoptHandoff(exported, id: source.id, workspaceID: targetWorkspace.path, cwd: targetWorkspace.path)
        XCTAssertFalse(staged.transcriptPath!.hasPrefix(targetHome.path + "/"))
        XCTAssertTrue(try FileManager.default.contentsOfDirectory(atPath: targetHome.appendingPathComponent("sessions").path).isEmpty)
        XCTAssertEqual(try Data(contentsOf: URL(fileURLWithPath: staged.transcriptPath!)), exported.transcript)
        try sourceProvider.retireHandoff(exported, operationID: UUID().uuidString)
        XCTAssertFalse(FileManager.default.fileExists(atPath: sourcePath.path))
        let active = try targetProvider.activateHandoff(exported, conversation: staged)
        XCTAssertTrue(active.transcriptPath!.hasPrefix(targetHome.appendingPathComponent("sessions").path + "/"))
        XCTAssertEqual(try Data(contentsOf: URL(fileURLWithPath: active.transcriptPath!)), exported.transcript)
        let returning = try targetProvider.exportForHandoff(active)
        let stagedReturn = try sourceProvider.adoptHandoff(returning, id: source.id, workspaceID: sourceWorkspace.path, cwd: sourceWorkspace.path)
        try targetProvider.retireHandoff(returning, operationID: UUID().uuidString)
        let returned = try sourceProvider.activateHandoff(returning, conversation: stagedReturn)
        XCTAssertEqual(returned.nativeSessionID, source.nativeSessionID)
        XCTAssertNotEqual(returned.transcriptPath, source.transcriptPath)
        let state = try sourceProvider.read(returned)
        XCTAssertEqual(state["warning"], .null)
        let nativeFiles = FileManager.default.enumerator(atPath: sourceHome.appendingPathComponent("sessions").path)!.allObjects.compactMap { $0 as? String }.filter { $0.hasSuffix(".jsonl") }
        XCTAssertEqual(nativeFiles.count, 1)
        for home in [sourceHome, targetHome] {
            XCTAssertFalse(FileManager.default.fileExists(atPath: home.appendingPathComponent("provider-was-launched").path))
        }
    }

    func testNativeImportConfirmsOriginalIdentityWithoutStartingProvider() throws {
        let (provider, home, workspace, _) = try fixture()
        let source = try transcript(home: home, nativeID: "native-original", cwd: workspace)
        let conversation = try provider.importConversation(id: "hosted-original", provider: "codex",
            nativeSessionID: "native-original", sourcePath: source.path, workspaceID: workspace.path,
            cwd: workspace.path, title: "Imported")
        XCTAssertEqual(conversation.nativeSessionID, "native-original")
        XCTAssertEqual(conversation.providerInstanceID, "codex:" + home.path)
        XCTAssertEqual(conversation.transcriptPath, source.path)
        XCTAssertEqual(conversation.cwd, workspace.path)
        XCTAssertFalse(provider.isConnected(conversation.id))
        let state = try provider.read(conversation)
        XCTAssertFalse(state["ready"].bool ?? true)
        XCTAssertFalse(FileManager.default.fileExists(atPath: home.appendingPathComponent("provider-was-launched").path))
    }

    func testNativeImportRejectsMismatchedSessionWorkspaceAndOutsideHome() throws {
        let (provider, home, workspace, root) = try fixture()
        let source = try transcript(home: home, nativeID: "native-original", cwd: workspace)
        XCTAssertThrowsError(try provider.importConversation(id: "wrong-session", provider: "codex",
            nativeSessionID: "wrong-native", sourcePath: source.path, workspaceID: workspace.path,
            cwd: workspace.path, title: "Wrong session")) { error in
            guard case AgentConversationServiceError.runtime(let message) = error else {
                return XCTFail("Expected native parser identity rejection, got \(String(reflecting: error))")
            }
            XCTAssertEqual(message, "invalid conversation request: Codex session metadata identity mismatch")
        }
        XCTAssertThrowsError(try provider.importConversation(id: "wrong-workspace", provider: "codex",
            nativeSessionID: "native-original", sourcePath: source.path, workspaceID: root.path,
            cwd: root.path, title: "Wrong workspace")) { error in
            XCTAssertEqual((error as? HostFailure)?.code, "identity_conflict")
        }
        let outside = root.appendingPathComponent("outside.jsonl")
        try FileManager.default.copyItem(at: source, to: outside)
        XCTAssertThrowsError(try provider.importConversation(id: "outside", provider: "codex",
            nativeSessionID: "native-original", sourcePath: outside.path, workspaceID: workspace.path,
            cwd: workspace.path, title: "Outside provider home")) { error in
            XCTAssertEqual((error as? HostFailure)?.code, "source_denied")
        }
        XCTAssertFalse(FileManager.default.fileExists(atPath: home.appendingPathComponent("provider-was-launched").path))
    }

    func testResumeRejectsChangedProviderHomeBeforeLaunchingProcess() throws {
        let (provider, home, workspace, _) = try fixture()
        let source = try transcript(home: home, nativeID: "native-original", cwd: workspace)
        var conversation = try provider.importConversation(id: "original", provider: "codex",
            nativeSessionID: "native-original", sourcePath: source.path, workspaceID: workspace.path,
            cwd: workspace.path, title: "Imported")
        conversation.providerInstanceID = "codex:/different-provider-home"
        XCTAssertThrowsError(try provider.resume(conversation)) { error in
            XCTAssertEqual((error as? HostFailure)?.code, "provider_identity")
        }
        XCTAssertFalse(provider.isConnected(conversation.id))
        XCTAssertFalse(FileManager.default.fileExists(atPath: home.appendingPathComponent("provider-was-launched").path))
    }
}
#endif
