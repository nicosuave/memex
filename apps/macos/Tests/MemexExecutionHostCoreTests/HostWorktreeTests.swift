import XCTest
@testable import MemexExecutionHostCore

final class HostWorktreeTests: XCTestCase {
    private final class Provider: ExecutionProvider {
        var providers = ["codex"]
        func create(id: String, provider: String, workspaceID: String, cwd: String, title: String) throws -> HostedConversation {
            HostedConversation(id: id, nativeSessionID: "native-" + id, provider: provider, providerInstanceID: "fixture-home",
                workspaceID: workspaceID, cwd: cwd, title: title, createdAt: "2026-10-05T12:00:00Z")
        }
        func resume(_ conversation: HostedConversation) throws {}
        func read(_ conversation: HostedConversation) throws -> HostValue { .object(["ready": .bool(false), "running": .bool(false)]) }
        func perform(_ command: HostedCommand) throws -> HostValue { .bool(true) }
        func isConnected(_ id: String) -> Bool { false }
    }

    private func fixture() throws -> (ExecutionHost, URL, URL, Provider) {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-worktree-api-" + UUID().uuidString).resolvingSymlinksInPath()
        let repository = root.appendingPathComponent("repository")
        try FileManager.default.createDirectory(at: repository, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        _ = try git(repository, ["init", "--initial-branch=main"])
        try Data("initial\n".utf8).write(to: repository.appendingPathComponent("tracked.txt"))
        try Data("ignored.txt\n".utf8).write(to: repository.appendingPathComponent(".gitignore"))
        _ = try git(repository, ["add", "--", "tracked.txt", ".gitignore"])
        _ = try git(repository, ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "-m", "Initial fixture"])
        let provider = Provider()
        let host = try ExecutionHost(directory: root.appendingPathComponent("execution"), workspaceRoots: [repository]) { _ in provider }
        return (host, repository, root, provider)
    }

    private func git(_ directory: URL, _ arguments: [String]) throws -> String {
        try WorkspaceGitCommand.text(directory, arguments, command: CommandRun())
    }

    private func call(_ host: ExecutionHost, _ method: String, _ values: [String: HostValue] = [:], id: String = UUID().uuidString) -> HostResponse {
        var values = values
        values["hostId"] = .string(host.hostID)
        values["commandId"] = .string(id)
        return host.handle(.init(id: .string(id), method: method, params: values))
    }

    private func create(_ host: ExecutionHost, _ repository: URL, id: String = UUID().uuidString) throws -> HostValue {
        let response = call(host, "worktree.create", ["workspaceId": .string(repository.path), "baseRef": .string("main")], id: id)
        XCTAssertNil(response.error, response.error?.message ?? "")
        return try XCTUnwrap(response.result)
    }

    func testCreateHasStableReceiptAndDoesNotCopyOrModifySourceDirtyFiles() throws {
        let (host, repository, _, _) = try fixture()
        try Data("source changes\n".utf8).write(to: repository.appendingPathComponent("tracked.txt"))
        try Data("source private bytes".utf8).write(to: repository.appendingPathComponent("ignored.txt"))
        let first = try create(host, repository, id: "create-once")
        let again = try create(host, repository, id: "create-once")
        XCTAssertEqual(first, again)
        let checkout = URL(fileURLWithPath: try XCTUnwrap(first["path"].string))
        XCTAssertEqual(try String(contentsOf: checkout.appendingPathComponent("tracked.txt"), encoding: .utf8), "initial\n")
        XCTAssertFalse(FileManager.default.fileExists(atPath: checkout.appendingPathComponent("ignored.txt").path))
        XCTAssertEqual(try String(contentsOf: repository.appendingPathComponent("tracked.txt"), encoding: .utf8), "source changes\n")
        XCTAssertEqual(try String(contentsOf: repository.appendingPathComponent("ignored.txt"), encoding: .utf8), "source private bytes")
        XCTAssertEqual(call(host, "worktree.list").result?.array.count, 1)
        XCTAssertTrue(call(host, "workspace.list").result?.array.contains { $0["id"] == first["workspaceId"] } == true)
    }

    func testArchivePreservesDirtyAndIgnoredFilesAndCleanupRefusesThem() throws {
        let (host, repository, _, _) = try fixture()
        let tree = try create(host, repository)
        let id = try XCTUnwrap(tree["id"].string)
        let checkout = URL(fileURLWithPath: try XCTUnwrap(tree["path"].string))
        let ignored = checkout.appendingPathComponent("ignored.txt")
        try Data("retain ignored bytes".utf8).write(to: ignored)
        XCTAssertNil(call(host, "worktree.archive", ["worktreeId": .string(id)]).error)
        XCTAssertNotNil(call(host, "worktree.cleanup", ["worktreeId": .string(id)]).error)
        XCTAssertEqual(try String(contentsOf: ignored, encoding: .utf8), "retain ignored bytes")
        XCTAssertFalse(call(host, "workspace.list").result?.array.contains { $0["id"] == tree["workspaceId"] } == true)
        XCTAssertNil(call(host, "worktree.reattach", ["worktreeId": .string(id)]).error)
        XCTAssertEqual(try String(contentsOf: ignored, encoding: .utf8), "retain ignored bytes")
    }

    func testCleanupRetainsBranchAndReattachesExactOwnedCheckout() throws {
        let (host, repository, _, _) = try fixture()
        let tree = try create(host, repository)
        let id = try XCTUnwrap(tree["id"].string)
        let path = try XCTUnwrap(tree["path"].string)
        let branch = try XCTUnwrap(tree["branch"].string)
        let originalCommit = try git(URL(fileURLWithPath: path), ["rev-parse", "HEAD"])
        let removed = call(host, "worktree.cleanup", ["worktreeId": .string(id)], id: "cleanup-once")
        XCTAssertNil(removed.error)
        XCTAssertEqual(removed.result?["removed"].bool, true)
        XCTAssertFalse(FileManager.default.fileExists(atPath: path))
        XCTAssertEqual(try git(repository, ["rev-parse", "refs/heads/" + branch]), originalCommit)
        XCTAssertEqual(call(host, "worktree.cleanup", ["worktreeId": .string(id)], id: "cleanup-once").result, removed.result)
        let reattached = call(host, "worktree.reattach", ["worktreeId": .string(id)])
        XCTAssertNil(reattached.error)
        XCTAssertEqual(reattached.result?["removed"].bool, false)
        XCTAssertEqual(reattached.result?["path"].string, path)
        XCTAssertEqual(try git(URL(fileURLWithPath: path), ["rev-parse", "HEAD"]), originalCommit)
    }

    func testEvenDisconnectedRetainedConversationPreventsCleanup() throws {
        let (host, repository, _, _) = try fixture()
        let tree = try create(host, repository)
        let chat = call(host, "conversation.create", ["workspaceId": tree["workspaceId"], "provider": .string("codex")])
        XCTAssertNil(chat.error)
        XCTAssertNotNil(call(host, "worktree.cleanup", ["worktreeId": tree["id"]]).error)
        XCTAssertTrue(FileManager.default.fileExists(atPath: try XCTUnwrap(tree["path"].string)))
        let listed = call(host, "worktree.list").result?.array.first
        XCTAssertEqual(listed?["referencedBy"].array, [try XCTUnwrap(chat.result?["conversation"]["id"])])
    }

    func testRemovedRepositoryGrantAndArbitraryPathsCannotAuthorizeWorktrees() throws {
        let (originalHost, repository, root, provider) = try fixture()
        var host = originalHost
        let tree = try create(host, repository)
        XCTAssertEqual(call(host, "worktree.create", ["workspaceId": .string(root.path), "baseRef": .string("main")]).error?.code, "workspace_denied")
        host = try ExecutionHost(directory: root.appendingPathComponent("execution"), workspaceRoots: []) { _ in provider }
        XCTAssertEqual(call(host, "worktree.list").result?.array.count, 0)
        XCTAssertNotNil(call(host, "worktree.cleanup", ["worktreeId": tree["id"]]).error)
        XCTAssertEqual(call(host, "conversation.create", ["workspaceId": tree["workspaceId"], "provider": .string("codex")]).error?.code, "workspace_denied")
        XCTAssertTrue(FileManager.default.fileExists(atPath: try XCTUnwrap(tree["path"].string)))
    }

    func testSymlinkReplacementCannotTurnCleanupIntoUnownedDirectoryRemoval() throws {
        let (host, repository, root, _) = try fixture()
        let tree = try create(host, repository)
        let checkout = URL(fileURLWithPath: try XCTUnwrap(tree["path"].string))
        let moved = root.appendingPathComponent("original-checkout")
        let unowned = root.appendingPathComponent("unowned-folder")
        try FileManager.default.moveItem(at: checkout, to: moved)
        try FileManager.default.createDirectory(at: unowned, withIntermediateDirectories: true)
        let sentinel = unowned.appendingPathComponent("user-work.txt")
        try Data("preserve me".utf8).write(to: sentinel)
        try FileManager.default.createSymbolicLink(at: checkout, withDestinationURL: unowned)
        XCTAssertNotNil(call(host, "worktree.cleanup", ["worktreeId": tree["id"]]).error)
        XCTAssertEqual(try String(contentsOf: sentinel, encoding: .utf8), "preserve me")
    }

    func testNestedFolderGrantDoesNotAuthorizeItsEntireRepository() throws {
        let (_, repository, root, provider) = try fixture()
        let nested = repository.appendingPathComponent("only-this-project")
        try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)
        let host = try ExecutionHost(directory: root.appendingPathComponent("nested-execution"), workspaceRoots: [nested]) { _ in provider }
        XCTAssertEqual(call(host, "worktree.create", ["workspaceId": .string(nested.path), "baseRef": .string("main")]).error?.code, "workspace_denied")
        XCTAssertEqual(call(host, "worktree.list").result?.array.count, 0)
        XCTAssertEqual(try git(repository, ["branch", "--format=%(refname:short)"]), "main")
    }
}
