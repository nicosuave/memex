import XCTest
@testable import MemexExecutionHostCore

final class HostHandoffTests: XCTestCase {
    private final class Provider: ExecutionProvider {
        var providers = ["codex"]
        var connected: Set<String> = []
        var fenced: Set<String> = []
        var running = false
        var prompts = 0
        var failAdoption = false
        var failActivation = false
        var resumes = 0
        func create(id: String, provider: String, workspaceID: String, cwd: String, title: String) throws -> HostedConversation {
            connected.insert(id)
            return HostedConversation(id: id, nativeSessionID: UUID().uuidString.lowercased(), provider: provider,
                providerInstanceID: "codex:/native-home", workspaceID: workspaceID, cwd: cwd,
                transcriptPath: "/native-home/sessions/original.jsonl", title: title, createdAt: "2026-10-05T12:00:00Z")
        }
        func resume(_ c: HostedConversation) throws {
            guard !fenced.contains(c.id) else { throw HostFailure("handoff_ownership", "fenced") }
            connected.insert(c.id); resumes += 1
        }
        func read(_ c: HostedConversation) throws -> HostValue {
            .object(["ready": .bool(connected.contains(c.id)), "running": .bool(running), "operations": .array([]),
                "thread": .object(["pendingRequests": .array([]), "turns": .array([])])])
        }
        func perform(_ command: HostedCommand) throws -> HostValue { prompts += 1; return .bool(true) }
        func isConnected(_ id: String) -> Bool { connected.contains(id) }
        func handoffUnavailableReason(_ c: HostedConversation) -> String? { c.provider == "codex" ? nil : "unsupported" }
        func detachForHandoff(_ c: HostedConversation) throws { connected.remove(c.id); fenced.insert(c.id) }
        func exportForHandoff(_ c: HostedConversation) throws -> NativeConversationTransfer {
            guard fenced.contains(c.id) else { throw HostFailure("handoff_ownership", "not fenced") }
            let line: HostValue = .object(["type": .string("session_meta"), "payload": .object(["id": .string(c.nativeSessionID), "cwd": .string(c.cwd)])])
            let data = try JSONEncoder().encode(line)
            return .init(conversation: c, transcript: data, transcriptSHA256: NativeConversationTransferValidation.digest(data))
        }
        func retireHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws {
            guard fenced.contains(transfer.conversation.id) else { throw HostFailure("handoff_ownership", "not fenced") }
        }
        func restoreRetiredHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws {}
        func adoptHandoff(_ transfer: NativeConversationTransfer, id: String, workspaceID: String, cwd: String) throws -> HostedConversation {
            if failAdoption { throw HostFailure("provider_io", "injected adoption failure") }
            try NativeConversationTransferValidation.validate(transfer)
            var c = transfer.conversation; c.workspaceID = workspaceID; c.cwd = cwd
            c.providerInstanceID = "codex:/destination-home"; c.transcriptPath = "/destination-home/sessions/retained.jsonl"
            fenced.insert(id); return c
        }
        func activateHandoff(_ transfer: NativeConversationTransfer, conversation: HostedConversation) throws -> HostedConversation {
            if failActivation { throw HostFailure("publication_io", "injected activation failure") }
            return conversation
        }
        func relocate(_ c: HostedConversation, workspaceID: String, cwd: String) throws -> HostedConversation {
            var result = c; result.workspaceID = workspaceID; result.cwd = cwd; return result
        }
        func releaseHandoffFence(_ c: HostedConversation) throws { fenced.remove(c.id) }
    }

    private struct Fixture {
        let root: URL
        let source: URL
        let destination: URL
        let sourceProvider: Provider
        let targetProvider: Provider
        let sourceHost: ExecutionHost
        let targetHost: ExecutionHost
        let conversation: HostValue
    }
    private func fixture() throws -> Fixture {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("host-handoff-" + UUID().uuidString).resolvingSymlinksInPath()
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        let source = root.appendingPathComponent("source"), destination = root.appendingPathComponent("destination")
        try FileManager.default.createDirectory(at: source, withIntermediateDirectories: true)
        _ = try git(source, ["init", "--initial-branch=main"])
        try Data("original\n".utf8).write(to: source.appendingPathComponent("tracked.txt"))
        try Data(".ignored\n".utf8).write(to: source.appendingPathComponent(".gitignore"))
        _ = try git(source, ["add", "."])
        _ = try git(source, ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "Initial"])
        _ = try git(root, ["clone", "--no-local", source.path, destination.path])
        try Data("staged\n".utf8).write(to: source.appendingPathComponent("tracked.txt"))
        _ = try git(source, ["add", "tracked.txt"])
        try Data("unstaged\n".utf8).write(to: source.appendingPathComponent("tracked.txt"))
        try Data("ignored content\n".utf8).write(to: source.appendingPathComponent(".ignored"))
        try Data("untracked content\n".utf8).write(to: source.appendingPathComponent("new.txt"))
        try FileManager.default.createDirectory(at: source.appendingPathComponent("empty"), withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(atPath: source.appendingPathComponent("link").path, withDestinationPath: "tracked.txt")
        let a = Provider(), b = Provider()
        let sourceHost = try ExecutionHost(directory: root.appendingPathComponent("host-a"), workspaceRoots: [source, destination]) { _ in a }
        let targetHost = try ExecutionHost(directory: root.appendingPathComponent("host-b"), workspaceRoots: [destination]) { _ in b }
        let c = try ok(sourceHost, "conversation.create", ["provider": .string("codex"), "workspaceId": .string(source.path), "title": .string("Same chat")])["conversation"]
        return Fixture(root: root, source: source, destination: destination, sourceProvider: a, targetProvider: b,
            sourceHost: sourceHost, targetHost: targetHost, conversation: c)
    }
    private func git(_ root: URL, _ args: [String]) throws -> String { try WorkspaceGitCommand.text(root, args, command: CommandRun()) }
    private func response(_ host: ExecutionHost, _ method: String, _ fields: [String: HostValue] = [:], commandID: String = UUID().uuidString) -> HostResponse {
        var fields = fields; fields["hostId"] = .string(host.hostID); fields["commandId"] = .string(commandID); fields["issuedAt"] = .string("2026-10-05T12:00:00Z")
        return host.handle(.init(id: .string(commandID), method: method, params: fields))
    }
    private func ok(_ host: ExecutionHost, _ method: String, _ fields: [String: HostValue] = [:], commandID: String = UUID().uuidString, file: StaticString = #filePath, line: UInt = #line) throws -> HostValue {
        let reply = response(host, method, fields, commandID: commandID)
        XCTAssertNil(reply.error, reply.error?.message ?? "", file: file, line: line)
        return try XCTUnwrap(reply.result, file: file, line: line)
    }
    private func prepare(_ f: Fixture, id: String) throws -> HostValue {
        let targetInfo = try ok(f.targetHost, "host.info")
        return try ok(f.sourceHost, "conversation.handoff.prepare", ["handoffId": .string(id), "conversationId": f.conversation["id"],
            "destinationHostId": .string(f.targetHost.hostID), "destinationPublicKey": targetInfo["handoffPublicKey"],
            "destinationWorkspaceId": .string(f.destination.path)])
    }
    private func install(_ f: Fixture, id: String) throws -> HostValue {
        let exported = try ok(f.sourceHost, "conversation.handoff.export", ["handoffId": .string(id)])
        let sourceInfo = try ok(f.sourceHost, "host.info")
        return try ok(f.targetHost, "conversation.handoff.install", ["handoffId": .string(id), "package": exported["package"],
            "certificate": exported["certificate"], "sourcePublicKey": sourceInfo["handoffPublicKey"]])
    }

    func testTwoHostMovePreservesNativeSessionDirtyIgnoredAndIndexStateAndNeverPrompts() throws {
        let f = try fixture(), id = UUID().uuidString
        let sourceStatus = try git(f.source, ["status", "--porcelain=v1", "--ignored"])
        let indexBefore = try git(f.source, ["show", ":tracked.txt"])
        _ = try prepare(f, id: id)
        XCTAssertEqual(response(f.sourceHost, "conversation.resume", ["conversationId": f.conversation["id"]]).error?.code, "handoff_ownership")
        XCTAssertEqual(response(f.sourceHost, "workspace.file.write", ["workspaceId": .string(f.source.path)]).error?.code, "handoff_ownership")
        let installed = try install(f, id: id)
        XCTAssertEqual(installed["phase"].string, "installed")
        XCTAssertEqual(response(f.targetHost, "conversation.resume", ["conversationId": f.conversation["id"]]).error?.code, "handoff_ownership")
        let committed = try ok(f.sourceHost, "conversation.handoff.commit", ["handoffId": .string(id), "certificate": installed["certificate"]])
        let active = try ok(f.targetHost, "conversation.handoff.activate", ["handoffId": .string(id), "certificate": committed["certificate"]])
        XCTAssertEqual(active["destination"]["nativeSessionID"], f.conversation["nativeSessionID"])
        XCTAssertEqual(active["destination"]["id"], f.conversation["id"])
        XCTAssertEqual(active["destination"]["cwd"].string, f.destination.path)
        XCTAssertEqual(try git(f.destination, ["show", ":tracked.txt"]), indexBefore)
        XCTAssertEqual(try String(contentsOf: f.destination.appendingPathComponent("tracked.txt"), encoding: .utf8), "unstaged\n")
        XCTAssertEqual(try String(contentsOf: f.destination.appendingPathComponent(".ignored"), encoding: .utf8), "ignored content\n")
        XCTAssertEqual(try git(f.source, ["status", "--porcelain=v1", "--ignored"]), sourceStatus)
        XCTAssertEqual(try FileManager.default.destinationOfSymbolicLink(atPath: f.destination.appendingPathComponent("link").path), "tracked.txt")
        XCTAssertEqual(f.sourceProvider.prompts + f.targetProvider.prompts, 0)
        _ = try ok(f.targetHost, "conversation.handoff.activate", ["handoffId": .string(id), "certificate": committed["certificate"]])
        XCTAssertEqual(f.targetProvider.resumes, 1)
    }

    func testSourceAbortDecisionPreventsStaleCommitAndTargetTombstonePreventsLateInstall() throws {
        let f = try fixture(), id = UUID().uuidString
        _ = try prepare(f, id: id)
        let installed = try install(f, id: id)
        let aborting = try ok(f.sourceHost, "conversation.handoff.abort", ["handoffId": .string(id)])
        XCTAssertEqual(aborting["phase"].string, "aborting")
        XCTAssertEqual(response(f.sourceHost, "conversation.handoff.commit", ["handoffId": .string(id), "certificate": installed["certificate"]]).error?.code, "handoff_state")
        let aborted = try ok(f.targetHost, "conversation.handoff.abort", ["handoffId": .string(id), "certificate": aborting["certificate"]])
        _ = try ok(f.sourceHost, "conversation.handoff.abort", ["handoffId": .string(id), "certificate": aborted["certificate"]])
        XCTAssertEqual(try String(contentsOf: f.destination.appendingPathComponent("tracked.txt"), encoding: .utf8), "original\n")
        XCTAssertFalse(FileManager.default.fileExists(atPath: f.destination.appendingPathComponent(".ignored").path))
        XCTAssertTrue(try ok(f.targetHost, "conversation.list").array.isEmpty)
        _ = try ok(f.sourceHost, "conversation.resume", ["conversationId": f.conversation["id"]])
    }

    func testWrongPeerSignatureCannotCommitAndRestartRetainsSourceFence() throws {
        let f = try fixture(), id = UUID().uuidString
        _ = try prepare(f, id: id)
        let installed = try install(f, id: id)
        var certificate = installed["certificate"].object; certificate["destinationHostID"] = .string("another-host")
        XCTAssertEqual(response(f.sourceHost, "conversation.handoff.commit", ["handoffId": .string(id), "certificate": .object(certificate)]).error?.code, "handoff_identity")
        let restartedProvider = Provider()
        let restarted = try ExecutionHost(directory: f.root.appendingPathComponent("host-a"), workspaceRoots: [f.source, f.destination]) { _ in restartedProvider }
        XCTAssertTrue(restartedProvider.fenced.contains(f.conversation["id"].string!))
        XCTAssertEqual(response(restarted, "conversation.send", ["conversationId": f.conversation["id"], "text": .string("must not send")]).error?.code, "handoff_ownership")
        XCTAssertEqual(restartedProvider.prompts, 0)
    }

    func testSameHostMoveRetainsProviderHomeAndRejectsDirtyDestinationBeforeFencing() throws {
        let f = try fixture()
        try Data("preserve me".utf8).write(to: f.destination.appendingPathComponent("user-file"))
        let fields: [String: HostValue] = ["handoffId": .string(UUID().uuidString), "conversationId": f.conversation["id"], "destinationWorkspaceId": .string(f.destination.path)]
        XCTAssertEqual(response(f.sourceHost, "conversation.handoff.move", fields).error?.code, "handoff_conflict")
        XCTAssertTrue(f.sourceProvider.fenced.isEmpty)
        XCTAssertEqual(try String(contentsOf: f.destination.appendingPathComponent("user-file"), encoding: .utf8), "preserve me")
    }

    private func move(_ source: ExecutionHost, to target: ExecutionHost, conversation: HostValue, workspace: URL) throws -> (String, HostValue) {
        let id = UUID().uuidString
        _ = try ok(source, "conversation.handoff.prepare", ["handoffId": .string(id), "conversationId": conversation["id"],
            "destinationHostId": .string(target.hostID), "destinationPublicKey": try ok(target, "host.info")["handoffPublicKey"],
            "destinationWorkspaceId": .string(workspace.path)])
        let export = try ok(source, "conversation.handoff.export", ["handoffId": .string(id)])
        let installed = try ok(target, "conversation.handoff.install", ["handoffId": .string(id), "package": export["package"],
            "certificate": export["certificate"], "sourcePublicKey": try ok(source, "host.info")["handoffPublicKey"]])
        let committed = try ok(source, "conversation.handoff.commit", ["handoffId": .string(id), "certificate": installed["certificate"]])
        _ = try ok(target, "conversation.handoff.activate", ["handoffId": .string(id), "certificate": committed["certificate"]])
        return (id, committed["certificate"])
    }

    func testThreeHostMoveRejectsStaleActivationAndReturnsToExactRetainedDirtyWorkspace() throws {
        let f = try fixture(), thirdRoot = f.root.appendingPathComponent("third")
        _ = try git(f.root, ["clone", "--no-local", f.source.path, thirdRoot.path])
        let thirdProvider = Provider()
        let thirdHost = try ExecutionHost(directory: f.root.appendingPathComponent("host-c"), workspaceRoots: [thirdRoot]) { _ in thirdProvider }
        let (firstID, firstReceipt) = try move(f.sourceHost, to: f.targetHost, conversation: f.conversation, workspace: f.destination)
        _ = try move(f.targetHost, to: thirdHost, conversation: f.conversation, workspace: thirdRoot)
        XCTAssertEqual(response(f.targetHost, "conversation.handoff.activate", ["handoffId": .string(firstID), "certificate": firstReceipt]).error?.code, "handoff_ownership")
        XCTAssertTrue(f.targetProvider.fenced.contains(f.conversation["id"].string!))
        try Data("third host edit\n".utf8).write(to: thirdRoot.appendingPathComponent("new.txt"))
        _ = try move(thirdHost, to: f.sourceHost, conversation: f.conversation, workspace: f.source)
        XCTAssertEqual(try String(contentsOf: f.source.appendingPathComponent("new.txt"), encoding: .utf8), "third host edit\n")
        XCTAssertEqual(try ok(f.sourceHost, "conversation.list").array.count, 1)
        _ = try ok(f.sourceHost, "conversation.resume", ["conversationId": f.conversation["id"]])
        XCTAssertEqual(f.sourceProvider.prompts + f.targetProvider.prompts + thirdProvider.prompts, 0)
    }

    func testSameHostMoveAndReturnPreserveIdentityAndRetainedSource() throws {
        let f = try fixture()
        let first = try ok(f.sourceHost, "conversation.handoff.move", ["handoffId": .string(UUID().uuidString),
            "conversationId": f.conversation["id"], "destinationWorkspaceId": .string(f.destination.path)])
        XCTAssertEqual(first["phase"].string, "active")
        XCTAssertEqual(first["destination"]["providerInstanceID"], f.conversation["providerInstanceID"])
        try Data("moved workspace edit\n".utf8).write(to: f.destination.appendingPathComponent("new.txt"))
        let returned = try ok(f.sourceHost, "conversation.handoff.move", ["handoffId": .string(UUID().uuidString),
            "conversationId": f.conversation["id"], "destinationWorkspaceId": .string(f.source.path)])
        XCTAssertEqual(returned["destination"]["id"], f.conversation["id"])
        XCTAssertEqual(try String(contentsOf: f.source.appendingPathComponent("new.txt"), encoding: .utf8), "moved workspace edit\n")
    }

    func testAdoptionFailureRecoversDestinationBeforeSourceRelease() throws {
        let f = try fixture(), id = UUID().uuidString
        _ = try prepare(f, id: id); f.targetProvider.failAdoption = true
        let export = try ok(f.sourceHost, "conversation.handoff.export", ["handoffId": .string(id)])
        XCTAssertNotNil(response(f.targetHost, "conversation.handoff.install", ["handoffId": .string(id), "package": export["package"],
            "certificate": export["certificate"], "sourcePublicKey": try ok(f.sourceHost, "host.info")["handoffPublicKey"]]).error)
        let decision = try ok(f.sourceHost, "conversation.handoff.abort", ["handoffId": .string(id)])
        let stopped = try ok(f.targetHost, "conversation.handoff.abort", ["handoffId": .string(id), "certificate": decision["certificate"]])
        _ = try ok(f.sourceHost, "conversation.handoff.abort", ["handoffId": .string(id), "certificate": stopped["certificate"]])
        XCTAssertEqual(try String(contentsOf: f.destination.appendingPathComponent("tracked.txt"), encoding: .utf8), "original\n")
        XCTAssertFalse(f.sourceProvider.fenced.contains(f.conversation["id"].string!))
    }

    func testFailedPublicationRemainsActivatingAndCannotAbortCommittedOwnership() throws {
        let f = try fixture(), id = UUID().uuidString
        _ = try prepare(f, id: id)
        let installed = try install(f, id: id)
        let committed = try ok(f.sourceHost, "conversation.handoff.commit", ["handoffId": .string(id), "certificate": installed["certificate"]])
        f.targetProvider.failActivation = true
        XCTAssertEqual(response(f.targetHost, "conversation.handoff.activate", ["handoffId": .string(id), "certificate": committed["certificate"]]).error?.code, "publication_io")
        XCTAssertEqual(try ok(f.targetHost, "conversation.handoff.read", ["handoffId": .string(id)])["phase"].string, "activating")
        XCTAssertEqual(response(f.targetHost, "conversation.handoff.abort", ["handoffId": .string(id)]).error?.code, "handoff_state")
        XCTAssertEqual(response(f.sourceHost, "conversation.resume", ["conversationId": f.conversation["id"]]).error?.code, "handoff_ownership")
        f.targetProvider.failActivation = false
        _ = try ok(f.targetHost, "conversation.handoff.activate", ["handoffId": .string(id), "certificate": committed["certificate"]])
        XCTAssertEqual(f.targetProvider.resumes, 1)
    }

    func testNativeTransferRejectsWrongMetadataEvenWithMatchingChecksum() throws {
        let f = try fixture()
        var c = try JSONDecoder().decode(HostedConversation.self, from: JSONEncoder().encode(f.conversation))
        try f.sourceProvider.detachForHandoff(c)
        var transfer = try f.sourceProvider.exportForHandoff(c)
        c.nativeSessionID = UUID().uuidString
        transfer.conversation = c
        XCTAssertThrowsError(try NativeConversationTransferValidation.validate(transfer)) {
            XCTAssertEqual(($0 as? HostFailure)?.code, "handoff_identity")
        }
    }
}
