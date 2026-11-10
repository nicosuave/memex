import Foundation
import Testing
@testable import Memex

private struct ConversationWorkspaceFixture {
    let root: URL
    let repository: URL
    var managed: URL { root.appendingPathComponent("managed") }

    init() throws {
        root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-worktree-" + UUID().uuidString)
        repository = root.appendingPathComponent("repository")
        try FileManager.default.createDirectory(at: repository, withIntermediateDirectories: true)
        _ = try git(["init", "-q", "--initial-branch=trunk"])
        _ = try git(["config", "user.name", "Fixture"])
        _ = try git(["config", "user.email", "fixture@example.invalid"])
        try FileManager.default.createDirectory(at: repository.appendingPathComponent("nested"), withIntermediateDirectories: false)
        try write("nested/file.txt", "base\n")
        _ = try git(["add", "."])
        _ = try git(["commit", "-qm", "Initial"])
        _ = try git(["update-ref", "refs/remotes/origin/trunk", "HEAD"])
        _ = try git(["symbolic-ref", "refs/remotes/origin/HEAD", "refs/remotes/origin/trunk"])
    }

    func git(_ arguments: [String], at directory: URL? = nil) throws -> String {
        let data = try CommandRun().execute(executable: URL(fileURLWithPath: "/usr/bin/git"),
            arguments: ["-c", "core.hooksPath=/dev/null", "-C", (directory ?? repository).path] + arguments, timeout: 10)
        return String(decoding: data, as: UTF8.self).trimmingCharacters(in: .newlines)
    }
    func write(_ path: String, _ text: String) throws {
        try text.write(to: repository.appendingPathComponent(path), atomically: true, encoding: .utf8)
    }
    func clean() { try? FileManager.default.removeItem(at: root) }
}

@Test func newWorktreeUsesRecordedDefaultAndPreservesDirtyCheckout() async throws {
    let fixture = try ConversationWorkspaceFixture()
    defer { fixture.clean() }
    let base = try fixture.git(["rev-parse", "HEAD"])
    _ = try fixture.git(["checkout", "-qb", "feature"])
    try fixture.write("nested/file.txt", "feature\n")
    _ = try fixture.git(["commit", "-qam", "Feature"])
    try fixture.write("nested/file.txt", "staged\n")
    _ = try fixture.git(["add", "."])
    try fixture.write("nested/file.txt", "unstaged\n")
    try fixture.write("untracked.txt", "keep me\n")
    let index = try Data(contentsOf: fixture.repository.appendingPathComponent(".git/index"))
    let status = try fixture.git(["status", "--porcelain=v1", "-z"])
    let client = ConversationWorkspaceClient(managedRoot: fixture.managed)
    let info = try #require(try await client.inspect(directory: fixture.repository))
    #expect(info.defaultBaseRef == "refs/remotes/origin/trunk")
    #expect(Set(info.localBranches) == ["feature", "trunk"])
    let workspace = try await client.prepare(directory: fixture.repository, mode: .newWorktree)
    #expect(workspace.state == .ready)
    #expect(workspace.baseCommit == base)
    #expect(try fixture.git(["rev-parse", "HEAD"], at: workspace.workingDirectory) == base)
    #expect(try String(contentsOf: workspace.workingDirectory.appendingPathComponent("nested/file.txt"), encoding: .utf8) == "base\n")
    #expect(try Data(contentsOf: fixture.repository.appendingPathComponent(".git/index")) == index)
    #expect(try fixture.git(["status", "--porcelain=v1", "-z"]) == status)
    #expect(try fixture.git(["branch", "--show-current"]) == "feature")
    let manifest = try #require(workspace.metadataURL)
    #expect(try JSONDecoder().decode(ConversationWorkspace.self, from: Data(contentsOf: manifest)) == workspace)
}

@Test func worktreeExplicitBaseSupportsNestedProjectsAndMissingDefault() async throws {
    let fixture = try ConversationWorkspaceFixture()
    defer { fixture.clean() }
    _ = try fixture.git(["symbolic-ref", "--delete", "refs/remotes/origin/HEAD"])
    let client = ConversationWorkspaceClient(managedRoot: fixture.managed)
    #expect(try await client.inspect(directory: fixture.repository)?.defaultBaseRef == nil)
    await #expect(throws: ConversationWorkspacePreparationError.self) {
        try await client.prepare(directory: fixture.repository, mode: .newWorktree)
    }
    #expect(!FileManager.default.fileExists(atPath: fixture.managed.path))
    let nested = fixture.repository.appendingPathComponent("nested")
    let workspace = try await client.prepare(directory: nested, mode: .newWorktree, baseRef: "trunk")
    #expect(workspace.sourceDirectory == nested.resolvingSymlinksInPath())
    #expect(workspace.workingDirectory.lastPathComponent == "nested")
    #expect(try String(contentsOf: workspace.workingDirectory.appendingPathComponent("file.txt"), encoding: .utf8) == "base\n")
}

@Test func worktreeReservationCollisionsPreserveExistingResources() async throws {
    let fixture = try ConversationWorkspaceFixture()
    defer { fixture.clean() }
    let id = UUID()
    let client = ConversationWorkspaceClient(managedRoot: fixture.managed, makeID: { id })
    let workspace = try await client.prepare(directory: fixture.repository, mode: .newWorktree)
    let sentinel = workspace.workingDirectory.appendingPathComponent("user-work.txt")
    try Data("preserve this".utf8).write(to: sentinel)
    let manifest = try Data(contentsOf: #require(workspace.metadataURL))
    await #expect(throws: ConversationWorkspacePreparationError.self) {
        try await client.prepare(directory: fixture.repository, mode: .newWorktree)
    }
    #expect(try String(contentsOf: sentinel, encoding: .utf8) == "preserve this")
    #expect(try Data(contentsOf: #require(workspace.metadataURL)) == manifest)
}

@Test func worktreeGitFailureRetainsManifestWithoutDeletingExistingBranch() async throws {
    let fixture = try ConversationWorkspaceFixture()
    defer { fixture.clean() }
    let id = UUID()
    let branch = "memex-chat-" + id.uuidString.lowercased()
    _ = try fixture.git(["branch", branch])
    let before = try fixture.git(["rev-parse", branch])
    let client = ConversationWorkspaceClient(managedRoot: fixture.managed, makeID: { id })
    do {
        _ = try await client.prepare(directory: fixture.repository, mode: .newWorktree)
        Issue.record("Expected existing branch to prevent worktree creation")
    } catch let error as ConversationWorkspacePreparationError {
        let workspace = try #require(error.retainedWorkspace)
        #expect(workspace.state == .failed)
        #expect(workspace.branch == branch)
        #expect(workspace.failure != nil)
        let saved = try JSONDecoder().decode(ConversationWorkspace.self, from: Data(contentsOf: #require(workspace.metadataURL)))
        #expect(saved == workspace)
        #expect(error.localizedDescription.contains("retained"))
    }
    #expect(try fixture.git(["rev-parse", branch]) == before)
}

@Test func nonGitProjectsUseExistingDirectoryAndRejectWorktree() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
    defer { try? FileManager.default.removeItem(at: root) }
    let client = ConversationWorkspaceClient(managedRoot: root.appendingPathComponent("managed"))
    #expect(try await client.inspect(directory: root) == nil)
    let workspace = try await client.prepare(directory: root, mode: .existingDirectory)
    #expect(workspace.workingDirectory == root.resolvingSymlinksInPath())
    #expect(workspace.repositoryRoot == nil)
    #expect(workspace.metadataURL == nil)
    await #expect(throws: ConversationWorkspacePreparationError.self) {
        try await client.prepare(directory: root, mode: .newWorktree)
    }
    #expect(!FileManager.default.fileExists(atPath: client.managedRoot.path))
}

@Test func partialCheckoutFailureKeepsBranchAndRecoveryMetadata() async throws {
    let fixture = try ConversationWorkspaceFixture()
    defer { fixture.clean() }
    try fixture.write(".gitattributes", "nested/file.txt filter=broken\n")
    _ = try fixture.git(["add", ".gitattributes"])
    _ = try fixture.git(["commit", "-qm", "Require checkout filter"])
    _ = try fixture.git(["config", "filter.broken.smudge", "false"])
    _ = try fixture.git(["config", "filter.broken.required", "true"])
    let id = UUID()
    let branch = "memex-chat-" + id.uuidString.lowercased()
    let client = ConversationWorkspaceClient(managedRoot: fixture.managed, makeID: { id })
    do {
        _ = try await client.prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
        Issue.record("Expected required smudge filter to fail during checkout")
    } catch let error as ConversationWorkspacePreparationError {
        let retained = try #require(error.retainedWorkspace)
        #expect(retained.state == .failed)
        #expect(FileManager.default.fileExists(atPath: try #require(retained.metadataURL).path))
        #expect(try fixture.git(["rev-parse", "refs/heads/" + branch]) == retained.baseCommit)
        #expect(try fixture.git(["branch", "--show-current"]) == "trunk")
    }
}

@Test func invalidBaseDoesNotReserveOrModifyWorktreeResources() async throws {
    let fixture = try ConversationWorkspaceFixture()
    defer { fixture.clean() }
    let client = ConversationWorkspaceClient(managedRoot: fixture.managed)
    let before = try fixture.git(["worktree", "list", "--porcelain"])
    await #expect(throws: (any Error).self) {
        try await client.prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "missing-base")
    }
    #expect(try fixture.git(["worktree", "list", "--porcelain"]) == before)
    #expect(!FileManager.default.fileExists(atPath: fixture.managed.path))
}

@Test func projectlessFoldersArePrivateIsolatedAndRetainFiles() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    let client = ConversationWorkspaceClient(temporaryRoot: root)
    let first = try await client.prepareTemporaryDirectory()
    let file = first.workingDirectory.appendingPathComponent("created.txt")
    try Data("keep across chats".utf8).write(to: file)
    let second = try await client.prepareTemporaryDirectory()
    #expect(first.id != second.id)
    #expect(first.workingDirectory != second.workingDirectory)
    #expect(first.repositoryRoot == nil && first.branch == nil && first.worktreeRoot == nil)
    #expect(first.state == .ready)
    #expect(try FileManager.default.contentsOfDirectory(atPath: second.workingDirectory.path).isEmpty)
    #expect(try String(contentsOf: file, encoding: .utf8) == "keep across chats")
    #expect(try JSONDecoder().decode(ConversationWorkspace.self, from: Data(contentsOf: #require(first.metadataURL))) == first)
    let attributes = try FileManager.default.attributesOfItem(atPath: first.workingDirectory.path)
    #expect(attributes[.posixPermissions] as? Int == 0o700)
    #expect(ConversationWorkspaceClient().temporaryRoot.path.contains("/Library/Application Support/dev.memex.app/"))
}

@Test func projectlessFolderCollisionNeverReusesOrOverwritesFiles() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    let id = UUID()
    let client = ConversationWorkspaceClient(temporaryRoot: root, makeID: { id })
    let first = try await client.prepareTemporaryDirectory()
    let file = first.workingDirectory.appendingPathComponent("work.txt")
    try Data("preserved".utf8).write(to: file)
    let manifest = try Data(contentsOf: #require(first.metadataURL))
    await #expect(throws: ConversationWorkspacePreparationError.self) { try await client.prepareTemporaryDirectory() }
    #expect(try Data(contentsOf: file) == Data("preserved".utf8))
    #expect(try Data(contentsOf: #require(first.metadataURL)) == manifest)
}
