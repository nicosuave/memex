import XCTest
@testable import MemexExecutionHostCore

final class RemoteWorkspaceAccessTests: XCTestCase {
    private func fixture() throws -> URL {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("remote-workspace-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        return root.resolvingSymlinksInPath()
    }
    private func request(_ method: String, _ fields: [String: HostValue] = [:]) -> HostRequest { .init(method: method, params: fields) }

    func testConflictCheckedSavePreservesChangedFileAndMode() throws {
        let root = try fixture(), file = root.appendingPathComponent("file.txt")
        try Data("original".utf8).write(to: file)
        try FileManager.default.setAttributes([.posixPermissions: 0o640], ofItemAtPath: file.path)
        let access = RemoteWorkspaceAccess()
        let first = try access.handle(request("workspace.file.read", ["path": .string("file.txt")]), root: root)
        try Data("external edit".utf8).write(to: file)
        XCTAssertThrowsError(try access.handle(request("workspace.file.write", ["path": .string("file.txt"), "revision": first["revision"], "text": .string("draft")]), root: root)) {
            XCTAssertEqual(($0 as? HostFailure)?.code, "file_conflict")
        }
        XCTAssertEqual(try String(contentsOf: file, encoding: .utf8), "external edit")
        let current = try access.handle(request("workspace.file.read", ["path": .string("file.txt")]), root: root)
        let saved = try access.handle(request("workspace.file.write", ["path": .string("file.txt"), "revision": current["revision"], "text": .string("ok")]), root: root)
        XCTAssertNotEqual(saved["revision"], current["revision"])
        XCTAssertEqual(try String(contentsOf: file, encoding: .utf8), "ok")
        XCTAssertEqual((try FileManager.default.attributesOfItem(atPath: file.path)[.posixPermissions] as? NSNumber)?.intValue, 0o640)
    }

    func testTraversalSymlinksGitMetadataAndHardlinksDenied() throws {
        let root = try fixture(), outside = try fixture()
        let target = outside.appendingPathComponent("secret")
        try Data("outside".utf8).write(to: target)
        try FileManager.default.createSymbolicLink(at: root.appendingPathComponent("link"), withDestinationURL: outside)
        try FileManager.default.linkItem(at: target, to: root.appendingPathComponent("hardlink"))
        let access = RemoteWorkspaceAccess()
        for path in ["../secret", target.path, "link/secret", "hardlink", ".git/config", "dir/../secret", "dir//file"] {
            XCTAssertThrowsError(try access.handle(request("workspace.file.read", ["path": .string(path)]), root: root), path)
        }
        XCTAssertThrowsError(try access.handle(request("workspace.files", ["path": .string("link")]), root: root))
        XCTAssertTrue(try access.handle(request("workspace.files"), root: root).array.isEmpty)
    }

    func testFailedAtomicSaveRetainsOriginalBytesAndRemovesTemporaryFile() throws {
        let root = try fixture(), file = root.appendingPathComponent("file.txt")
        try Data("original bytes".utf8).write(to: file)
        let access = RemoteWorkspaceAccess(beforeFileReplace: { throw HostFailure("injected_io", "Injected replacement failure") })
        let current = try access.handle(request("workspace.file.read", ["path": .string("file.txt")]), root: root)
        XCTAssertThrowsError(try access.handle(request("workspace.file.write", ["path": .string("file.txt"), "revision": current["revision"], "text": .string("replacement")]), root: root))
        XCTAssertEqual(try String(contentsOf: file, encoding: .utf8), "original bytes")
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: root.path), ["file.txt"])
    }

    func testBinaryAndOversizedFilesAreNotPresentedAsEditableText() throws {
        let root = try fixture(), access = RemoteWorkspaceAccess()
        try Data([0, 1, 2]).write(to: root.appendingPathComponent("binary"))
        try Data(repeating: 65, count: 2 * 1024 * 1024 + 1).write(to: root.appendingPathComponent("large"))
        for path in ["binary", "large"] {
            XCTAssertThrowsError(try access.handle(request("workspace.file.read", ["path": .string(path)]), root: root))
        }
    }

    func testPTYRemainsHostOwnedAndRejectsOtherWorkspace() throws {
        let root = try fixture(), other = try fixture(), access = RemoteWorkspaceAccess()
        let fields: [String: HostValue] = ["terminalId": .string("test-terminal")]
        _ = try access.handle(request("workspace.terminal.open", fields), root: root)
        defer { _ = try? access.handle(request("workspace.terminal.close", fields), root: root) }
        XCTAssertThrowsError(try access.handle(request("workspace.terminal.read", fields), root: other)) {
            XCTAssertEqual(($0 as? HostFailure)?.code, "terminal_identity")
        }
        var input = fields
        input["input"] = .string("printf 'memex_remote_%s' 'ready'\n")
        _ = try access.handle(request("workspace.terminal.input", input), root: root)
        let deadline = Date().addingTimeInterval(5)
        var output = ""
        while Date() < deadline {
            let response = try access.handle(request("workspace.terminal.read", fields), root: root)
            output = String(decoding: Data(base64Encoded: response["data"].string ?? "") ?? Data(), as: UTF8.self)
            if output.contains("memex_remote_ready") { break }
            Thread.sleep(forTimeInterval: 0.05)
        }
        XCTAssertTrue(output.contains("memex_remote_ready"), output)
        let reconnect = try access.handle(request("workspace.terminal.open", fields), root: root)
        XCTAssertEqual(reconnect["terminalId"].string, "test-terminal")
        _ = try access.handle(request("workspace.terminal.close", fields), root: root)
        XCTAssertThrowsError(try access.handle(request("workspace.terminal.read", fields), root: root))
    }

    func testReadsIgnoreAdvisoryLocksAndLockedWritesFailWithinBound() throws {
        let root = try fixture(), file = root.appendingPathComponent("Cargo.lock")
        try Data("original".utf8).write(to: file)
        let holder = open(file.path, O_RDONLY)
        XCTAssertGreaterThanOrEqual(holder, 0)
        defer { close(holder) }
        XCTAssertEqual(flock(holder, LOCK_EX), 0)
        let access = RemoteWorkspaceAccess()
        let current = try access.handle(request("workspace.file.read", ["path": .string("Cargo.lock")]), root: root)
        XCTAssertEqual(current["text"].string, "original")
        let started = Date()
        XCTAssertThrowsError(try access.handle(request("workspace.file.write", ["path": .string("Cargo.lock"), "revision": current["revision"], "text": .string("draft")]), root: root)) {
            XCTAssertEqual(($0 as? HostFailure)?.code, "file_locked")
        }
        XCTAssertLessThan(Date().timeIntervalSince(started), 5)
        XCTAssertEqual(try String(contentsOf: file, encoding: .utf8), "original")
        flock(holder, LOCK_UN)
        _ = try access.handle(request("workspace.file.write", ["path": .string("Cargo.lock"), "revision": current["revision"], "text": .string("saved")]), root: root)
        XCTAssertEqual(try String(contentsOf: file, encoding: .utf8), "saved")
    }

    func testTerminalInputNeverBlocksWhenTheShellStopsReading() throws {
        let root = try fixture(), access = RemoteWorkspaceAccess()
        let fields: [String: HostValue] = ["terminalId": .string("stalled")]
        _ = try access.handle(request("workspace.terminal.open", fields), root: root)
        defer { _ = try? access.handle(request("workspace.terminal.close", fields), root: root) }
        var stop = fields; stop["input"] = .string("kill -STOP $PPID\n")
        _ = try access.handle(request("workspace.terminal.input", stop), root: root)
        Thread.sleep(forTimeInterval: 1)
        var chunk = fields; chunk["input"] = .string(String(repeating: "x", count: 60_000))
        let started = Date()
        var limited = false
        for _ in 0..<8 {
            do { _ = try access.handle(request("workspace.terminal.input", chunk), root: root) }
            catch let error as HostFailure where error.code == "terminal_limit" { limited = true; break }
        }
        XCTAssertTrue(limited, "Unread input is bounded instead of blocking")
        _ = try access.handle(request("workspace.terminal.close", fields), root: root)
        XCTAssertLessThan(Date().timeIntervalSince(started), 3)
    }

    func testExitedTerminalsAreReapedWhenCapacityIsNeeded() throws {
        let root = try fixture(), access = RemoteWorkspaceAccess()
        for index in 0..<RemoteWorkspaceTerminals.sessionLimit {
            let fields: [String: HostValue] = ["terminalId": .string("shell-\(index)")]
            _ = try access.handle(request("workspace.terminal.open", fields), root: root)
            var exit = fields; exit["input"] = .string("exit\n")
            _ = try access.handle(request("workspace.terminal.input", exit), root: root)
        }
        let deadline = Date().addingTimeInterval(10)
        while Date() < deadline, try (0..<RemoteWorkspaceTerminals.sessionLimit).contains(where: {
            try access.handle(request("workspace.terminal.read", ["terminalId": .string("shell-\($0)")]), root: root)["running"].bool == true
        }) { Thread.sleep(forTimeInterval: 0.05) }
        let fresh: [String: HostValue] = ["terminalId": .string("fresh")]
        XCTAssertEqual(try access.handle(request("workspace.terminal.open", fresh), root: root)["running"].bool, true)
        _ = try access.handle(request("workspace.terminal.close", fresh), root: root)
        XCTAssertThrowsError(try access.handle(request("workspace.terminal.read", ["terminalId": .string("shell-0")]), root: root))
    }
}
