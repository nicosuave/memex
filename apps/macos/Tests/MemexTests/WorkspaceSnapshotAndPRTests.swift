import Foundation
import MemexExecutionHostCore
import Testing
@testable import Memex

private struct SnapshotFixture {
    let root: URL
    let repository: URL
    let store: ManagedWorkspaceStore
    init() throws {
        root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-snapshot-tests-" + UUID().uuidString)
        repository = root.appendingPathComponent("repo")
        store = ManagedWorkspaceStore(managedRoot: root.appendingPathComponent("managed"))
        try FileManager.default.createDirectory(at: repository, withIntermediateDirectories: true)
        _ = try git(["init", "-q", "--initial-branch=trunk"])
        _ = try git(["config", "user.name", "Fixture"])
        _ = try git(["config", "user.email", "fixture@example.invalid"])
        _ = try git(["config", "commit.gpgsign", "false"])
        try Data("base\n".utf8).write(to: repository.appendingPathComponent("tracked"))
        try Data("deleted\n".utf8).write(to: repository.appendingPathComponent("deleted"))
        try Data("ignored\n".utf8).write(to: repository.appendingPathComponent(".gitignore"))
        _ = try git(["add", "."])
        _ = try git(["commit", "-qm", "Initial"])
    }
    func git(_ args: [String], at directory: URL? = nil) throws -> String {
        try WorkspaceGitCommand.text(directory ?? repository, args, command: CommandRun())
    }
    func clean() { try? FileManager.default.removeItem(at: root) }
}

@Test func workspaceSnapshotRecoversRawBytesStagingIgnoredAndSymlinksWithoutOverwritingOriginal() throws {
    let fixture = try SnapshotFixture()
    defer { fixture.clean() }
    let workspace = try fixture.store.prepare(directory: fixture.repository, newWorktree: true, baseRef: "HEAD", command: CommandRun())
    let directory = workspace.workingDirectory
    try Data("staged\n".utf8).write(to: directory.appendingPathComponent("tracked"))
    _ = try fixture.git(["add", "tracked"], at: directory)
    try FileManager.default.removeItem(at: directory.appendingPathComponent("deleted"))
    let raw = Data([0, 255, 10, 13, 0, 128])
    try raw.write(to: directory.appendingPathComponent("tracked"))
    try raw.write(to: directory.appendingPathComponent("untracked"))
    try raw.write(to: directory.appendingPathComponent("ignored"))
    try FileManager.default.createSymbolicLink(atPath: directory.appendingPathComponent("link").path, withDestinationPath: "../outside")
    let snapshot = try fixture.store.captureSnapshot(workspace, isBusy: false, command: CommandRun())
    try Data("later".utf8).write(to: directory.appendingPathComponent("tracked"))
    let recovered = try fixture.store.recoverSnapshot(snapshot, workspace: workspace, command: CommandRun())
    #expect(recovered.id != workspace.id)
    #expect(!FileManager.default.fileExists(atPath: recovered.workingDirectory.appendingPathComponent("deleted").path))
    #expect(try Data(contentsOf: directory.appendingPathComponent("tracked")) == Data("later".utf8))
    for path in ["tracked", "untracked", "ignored"] {
        #expect(try Data(contentsOf: recovered.workingDirectory.appendingPathComponent(path)) == raw)
    }
    #expect(try fixture.git(["show", ":tracked"], at: recovered.workingDirectory) == "staged")
    #expect(try fixture.git(["rev-parse", "HEAD"], at: recovered.workingDirectory) == snapshot.head)
    #expect(try FileManager.default.destinationOfSymbolicLink(atPath: recovered.workingDirectory.appendingPathComponent("link").path) == "../outside")
    #expect(throws: (any Error).self) {
        try fixture.store.removeCleanCheckout(workspace, otherWorkspaceDirectories: [], isBusy: false, command: CommandRun())
    }
    #expect(FileManager.default.fileExists(atPath: directory.path))
}

@Test func workspaceCleanPruneLeavesRecoverableSnapshotAndRefusesUserCheckout() throws {
    let fixture = try SnapshotFixture()
    defer { fixture.clean() }
    let workspace = try fixture.store.prepare(directory: fixture.repository, newWorktree: true, baseRef: "HEAD", command: CommandRun())
    try fixture.store.removeCleanCheckout(workspace, otherWorkspaceDirectories: [], isBusy: false, command: CommandRun())
    #expect(!FileManager.default.fileExists(atPath: workspace.workingDirectory.path))
    let snapshots = try fixture.store.snapshots(workspace, command: CommandRun())
    #expect(snapshots.count == 1)
    let snapshot = try #require(snapshots.first)
    let recovered = try fixture.store.recoverSnapshot(snapshot, workspace: workspace, command: CommandRun())
    #expect(try fixture.git(["rev-parse", "HEAD"], at: recovered.workingDirectory) == snapshot.head)
    #expect(try String(contentsOf: recovered.workingDirectory.appendingPathComponent("tracked"), encoding: .utf8) == "base\n")
    let existing = try fixture.store.prepare(directory: fixture.repository, newWorktree: false, command: CommandRun())
    #expect(throws: (any Error).self) { try fixture.store.captureSnapshot(existing, isBusy: false, command: CommandRun()) }
    #expect(throws: (any Error).self) { try fixture.store.removeCleanCheckout(existing, otherWorkspaceDirectories: [], isBusy: false, command: CommandRun()) }
}

@Test func workspaceSnapshotCorruptionRefusesRecoveryBeforeCreatingCheckout() throws {
    let fixture = try SnapshotFixture()
    defer { fixture.clean() }
    let workspace = try fixture.store.prepare(directory: fixture.repository, newWorktree: true, baseRef: "HEAD", command: CommandRun())
    let snapshot = try fixture.store.captureSnapshot(workspace, isBusy: false, command: CommandRun())
    let manifest = try #require(workspace.metadataURL)
    let archive = manifest.deletingLastPathComponent().appendingPathComponent("snapshots").appendingPathComponent(snapshot.id + ".tar.gz")
    try Data("corrupt".utf8).write(to: archive)
    #expect(throws: (any Error).self) { try fixture.store.recoverSnapshot(snapshot, workspace: workspace, command: CommandRun()) }
    #expect(try fixture.store.managedWorkspaces().count == 1)
}

@Test func pullRequestReviewThreadsDecodeResolvedOutdatedAndPagination() throws {
    let json = #"{"data":{"node":{"reviewThreads":{"nodes":[{"id":"thread","path":"src/file.swift","line":null,"isResolved":true,"isOutdated":true,"comments":{"nodes":[{"id":"comment","body":"Preserve bytes","author":null,"url":"https://github.com/o/r/pull/1#discussion_r1","diffHunk":"@@ -1 +1 @@\n-old\n+new"}],"pageInfo":{"hasNextPage":true}}}],"pageInfo":{"hasNextPage":true}}}}}"#
    let threads = try WorkspacePullRequestClient.decodeThreads(Data(json.utf8))
    let thread = try #require(threads.nodes.first)
    #expect(thread.isResolved && thread.isOutdated && thread.line == nil)
    #expect(thread.comments.nodes.first?.body == "Preserve bytes")
    #expect(thread.comments.pageInfo.hasNextPage && threads.pageInfo.hasNextPage)
}

@Test func pullRequestReviewContextUsesGitHubIdentityInsteadOfLocalPath() {
    let context = WorkspaceReviewContext(directory: URL(fileURLWithPath: "/tmp/project"), path: "src/a.swift",
        scope: "PR #1 at abc123", patch: "+change", comment: "Review", sourceURL: URL(string: "https://github.com/o/r/pull/1"))
    #expect(context.promptText.contains("https://github.com/o/r/pull/1"))
    #expect(!context.promptText.contains("/tmp/project"))
}
