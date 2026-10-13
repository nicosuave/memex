import XCTest
import Darwin
@testable import MemexExecutionHostCore

final class NativeConversationRetirementTests: XCTestCase {
    private func fixture() throws -> (URL, URL, NativeConversationTransfer, NativeConversationRetirement) {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("native-retirement-" + UUID().uuidString).resolvingSymlinksInPath()
        let home = root.appendingPathComponent("codex"), source = home.appendingPathComponent("sessions/arbitrary.jsonl")
        try FileManager.default.createDirectory(at: source.deletingLastPathComponent(), withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        let id = UUID().uuidString.lowercased()
        let c = HostedConversation(id: "retained-chat", nativeSessionID: id, provider: "codex", providerInstanceID: "codex:" + home.path,
            workspaceID: root.path, cwd: root.path, transcriptPath: source.path, title: "Retire", createdAt: "2026-10-05T12:00:00Z")
        let metadata: HostValue = .object(["type": .string("session_meta"), "payload": .object(["id": .string(id), "cwd": .string(root.path)])])
        let bytes = try JSONEncoder().encode(metadata) + Data("\n".utf8)
        try bytes.write(to: source)
        return (root, home, NativeConversationTransfer(conversation: c, transcript: bytes,
            transcriptSHA256: NativeConversationTransferValidation.digest(bytes)), NativeConversationRetirement(directory: root.appendingPathComponent("private-recovery")))
    }

    func testRetirementSurvivesFenceReleaseAndRetryAndAbortRestoresExactBytes() throws {
        let (_, home, transfer, retirement) = try fixture(), operation = UUID().uuidString
        var fence: NativeHandoffWriterFence? = try NativeHandoffWriterFence(home: home, nativeSessionID: transfer.conversation.nativeSessionID)
        XCTAssertNotNil(fence)
        XCTAssertThrowsError(try NativeHandoffWriterFence(home: home, nativeSessionID: transfer.conversation.nativeSessionID))
        try retirement.retire(transfer, operationID: operation, home: home)
        fence = nil
        XCTAssertFalse(FileManager.default.fileExists(atPath: transfer.conversation.transcriptPath!))
        XCTAssertEqual(try NativeConversationTransferValidation.read(retirement.directory.appendingPathComponent(operation).appendingPathComponent(transfer.conversation.nativeSessionID + ".jsonl")), transfer.transcript)
        try retirement.retire(transfer, operationID: operation, home: home)
        try retirement.restore(transfer, operationID: operation, home: home)
        XCTAssertEqual(try Data(contentsOf: URL(fileURLWithPath: transfer.conversation.transcriptPath!)), transfer.transcript)
    }

    func testChangedSourceAndDuplicateArchivedHistoryCannotCommit() throws {
        let (_, home, transfer, retirement) = try fixture(), operation = UUID().uuidString
        let source = URL(fileURLWithPath: transfer.conversation.transcriptPath!)
        let newer = transfer.transcript + Data("{\"type\":\"event_msg\",\"payload\":{}}\n".utf8)
        try newer.write(to: source)
        XCTAssertThrowsError(try retirement.retire(transfer, operationID: operation, home: home))
        XCTAssertEqual(try Data(contentsOf: source), newer)
        try transfer.transcript.write(to: source)
        let duplicate = home.appendingPathComponent("archived_sessions/nonstandard-name.jsonl")
        try FileManager.default.createDirectory(at: duplicate.deletingLastPathComponent(), withIntermediateDirectories: true)
        try transfer.transcript.write(to: duplicate)
        XCTAssertThrowsError(try retirement.retire(transfer, operationID: operation, home: home)) {
            XCTAssertEqual(($0 as? HostFailure)?.code, "handoff_identity")
        }
        XCTAssertEqual(try Data(contentsOf: source), transfer.transcript)
    }

    func testNativeStateIndexAlternatePathCannotBeSilentlyLeftResumable() throws {
        let (root, home, transfer, retirement) = try fixture()
        let alternate = root.appendingPathComponent("outside-lookup.jsonl")
        try transfer.transcript.write(to: alternate)
        _ = try CommandRun().execute(executable: URL(fileURLWithPath: "/usr/bin/sqlite3"),
            arguments: [home.appendingPathComponent("state_5.sqlite").path,
                "create table threads(id text, rollout_path text); insert into threads values ('\(transfer.conversation.nativeSessionID)', '\(alternate.path)');"], timeout: 5)
        XCTAssertThrowsError(try retirement.retire(transfer, operationID: UUID().uuidString, home: home)) {
            XCTAssertEqual(($0 as? HostFailure)?.code, "handoff_identity")
        }
        XCTAssertTrue(FileManager.default.fileExists(atPath: transfer.conversation.transcriptPath!))
    }
}
