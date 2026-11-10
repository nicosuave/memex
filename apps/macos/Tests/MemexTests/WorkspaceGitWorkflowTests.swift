import Foundation
import Testing
@testable import Memex

private struct WorkspaceGitFixture {
    let root: URL
    let repository: URL
    var managed: URL { root.appendingPathComponent("managed") }
    var records: URL { root.appendingPathComponent("checkpoints") }

    init() throws {
        root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-git-tests-" + UUID().uuidString)
        repository = root.appendingPathComponent("repo")
        try FileManager.default.createDirectory(at: repository, withIntermediateDirectories: true)
        _ = try git(["init", "-q", "--initial-branch=trunk"])
        _ = try git(["config", "user.name", "Fixture"])
        _ = try git(["config", "user.email", "fixture@example.invalid"])
        _ = try git(["config", "commit.gpgsign", "false"])
        try Data("base\n".utf8).write(to: repository.appendingPathComponent("tracked.txt"))
        _ = try git(["add", "."])
        _ = try git(["commit", "-qm", "Initial"])
    }

    func git(_ args: [String], at directory: URL? = nil) throws -> String {
        try WorkspaceGitCommand.text(directory ?? repository, args, command: CommandRun())
    }
    func write(_ name: String, _ contents: String, at directory: URL? = nil) throws {
        try Data(contents.utf8).write(to: (directory ?? repository).appendingPathComponent(name), options: .atomic)
    }
    func clean() { try? FileManager.default.removeItem(at: root) }
}

@Test func gitCommitIncludesOnlyExplicitlyStagedFiles() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    try fixture.write("tracked.txt", "staged\n")
    let client = WorkspaceGitClient()
    try await client.stage(directory: fixture.repository, paths: ["tracked.txt"])
    try fixture.write("tracked.txt", "unstaged\n")
    try fixture.write("untracked.txt", "retain\n")
    _ = try await client.commit(directory: fixture.repository, message: "Only the index")
    #expect(try fixture.git(["show", "HEAD:tracked.txt"]) == "staged")
    #expect(try String(contentsOf: fixture.repository.appendingPathComponent("tracked.txt"), encoding: .utf8) == "unstaged\n")
    #expect(try String(contentsOf: fixture.repository.appendingPathComponent("untracked.txt"), encoding: .utf8) == "retain\n")
}

@Test func checkpointsPreserveIndexAndRestoreOnlyOwnedIsolatedWorktree() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let workspaces = ConversationWorkspaceClient(managedRoot: fixture.managed)
    let workspace = try await workspaces.prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
    let checkout = workspace.workingDirectory
    try fixture.write("tracked.txt", "staged\n", at: checkout)
    _ = try fixture.git(["add", "tracked.txt"], at: checkout)
    try fixture.write("tracked.txt", "working\n", at: checkout)
    try fixture.write("created.txt", "captured\n", at: checkout)
    let beforeIndex = try fixture.git(["write-tree"], at: checkout)
    let head = try fixture.git(["rev-parse", "HEAD"], at: checkout)
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let captured = try await checkpoints.capture(directory: checkout, conversationID: "chat", turnID: "turn-1", label: "Turn 1")
    #expect(try fixture.git(["write-tree"], at: checkout) == beforeIndex)
    #expect(try fixture.git(["rev-parse", "HEAD"], at: checkout) == head)
    #expect(captured.turnID == "turn-1")
    try fixture.write("tracked.txt", "later\n", at: checkout)
    try fixture.write("later.txt", "recoverable\n", at: checkout)
    let shared = WorkspaceIsolation(workspace: workspace, otherWorkspaceDirectories: [checkout.appendingPathComponent("nested")], isBusy: false)
    await #expect(throws: (any Error).self) { try await checkpoints.restore(captured, isolation: shared) }
    #expect(try String(contentsOf: checkout.appendingPathComponent("tracked.txt"), encoding: .utf8) == "later\n")
    let restored = try await checkpoints.restore(captured, isolation: .init(workspace: workspace, otherWorkspaceDirectories: [], isBusy: false))
    #expect(try String(contentsOf: checkout.appendingPathComponent("tracked.txt"), encoding: .utf8) == "working\n")
    #expect(try fixture.git(["show", ":tracked.txt"], at: checkout) == "staged")
    #expect(!FileManager.default.fileExists(atPath: checkout.appendingPathComponent("later.txt").path))
    #expect(try fixture.git(["show", restored.recovery.tree + ":later.txt"], at: checkout) == "recoverable")
    #expect(try fixture.git(["rev-parse", "HEAD"], at: checkout) == head)
    #expect(try await checkpoints.list(directory: checkout, conversationID: "chat").count == 2)
    _ = try await checkpoints.restore(restored.recovery, isolation: .init(workspace: workspace, otherWorkspaceDirectories: [], isBusy: false))
    #expect(try String(contentsOf: checkout.appendingPathComponent("later.txt"), encoding: .utf8) == "recoverable\n")
}

@Test func checkpointRestoreRejectsUnmanagedCheckoutAndActiveTurn() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let workspaces = ConversationWorkspaceClient(managedRoot: fixture.managed)
    let existing = try await workspaces.prepare(directory: fixture.repository, mode: .existingDirectory)
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let captured = try await checkpoints.capture(directory: fixture.repository, conversationID: "shared", label: "Baseline")
    await #expect(throws: (any Error).self) {
        try await checkpoints.restore(captured, isolation: .init(workspace: existing, otherWorkspaceDirectories: [], isBusy: false))
    }
    let managed = try await workspaces.prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
    let managedCheckpoint = try await checkpoints.capture(directory: managed.workingDirectory, conversationID: "managed", label: "Baseline")
    await #expect(throws: (any Error).self) {
        try await checkpoints.restore(managedCheckpoint, isolation: .init(workspace: managed, otherWorkspaceDirectories: [], isBusy: true))
    }
}

@Test func checkpointsPreserveRawBytesWithoutRunningGitFilters() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let workspace = try await ConversationWorkspaceClient(managedRoot: fixture.managed)
        .prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
    let checkout = workspace.workingDirectory
    try fixture.write(".gitattributes", "*.txt text eol=lf filter=reject\n", at: checkout)
    _ = try fixture.git(["config", "filter.reject.clean", "exit 71"], at: checkout)
    _ = try fixture.git(["config", "filter.reject.smudge", "exit 72"], at: checkout)
    _ = try fixture.git(["config", "filter.reject.required", "true"], at: checkout)
    let original = Data("raw\r\nbytes\r\n".utf8)
    try original.write(to: checkout.appendingPathComponent("tracked.txt"))
    try FileManager.default.createSymbolicLink(atPath: checkout.appendingPathComponent("link").path, withDestinationPath: "tracked.txt")
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let checkpoint = try await checkpoints.capture(directory: checkout, conversationID: "raw", label: "Raw")
    try fixture.write("tracked.txt", "later\n", at: checkout)
    try FileManager.default.removeItem(at: checkout.appendingPathComponent("link"))
    _ = try await checkpoints.restore(checkpoint, isolation: .init(workspace: workspace, otherWorkspaceDirectories: [], isBusy: false))
    #expect(try Data(contentsOf: checkout.appendingPathComponent("tracked.txt")) == original)
    #expect(try FileManager.default.destinationOfSymbolicLink(atPath: checkout.appendingPathComponent("link").path) == "tracked.txt")
}

@Test func checkpointRestorePreservesOverlappingIgnoredFile() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let workspace = try await ConversationWorkspaceClient(managedRoot: fixture.managed)
        .prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
    let checkout = workspace.workingDirectory
    try fixture.write("local.txt", "checkpoint", at: checkout)
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let checkpoint = try await checkpoints.capture(directory: checkout, conversationID: "ignored", label: "Before ignore")
    try fixture.write(".gitignore", "local.txt\n", at: checkout)
    try fixture.write("local.txt", "user-owned ignored bytes", at: checkout)
    await #expect(throws: (any Error).self) {
        try await checkpoints.restore(checkpoint, isolation: .init(workspace: workspace, otherWorkspaceDirectories: [], isBusy: false))
    }
    #expect(try String(contentsOf: checkout.appendingPathComponent("local.txt"), encoding: .utf8) == "user-owned ignored bytes")
}

@Test func managedWorktreeArchiveKeepsDirtyFilesAndCleanupRequiresNoReferences() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let client = ConversationWorkspaceClient(managedRoot: fixture.managed)
    let workspace = try await client.prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
    try fixture.write("user.txt", "keep", at: workspace.workingDirectory)
    try await client.setArchived(workspace, archived: true)
    #expect(try await client.managedWorkspaces().first?.archived == true)
    await #expect(throws: (any Error).self) {
        try await client.removeCleanCheckout(workspace, otherWorkspaceDirectories: [], isBusy: false)
    }
    #expect(try String(contentsOf: workspace.workingDirectory.appendingPathComponent("user.txt"), encoding: .utf8) == "keep")
    try FileManager.default.removeItem(at: workspace.workingDirectory.appendingPathComponent("user.txt"))
    let excludes = try fixture.git(["rev-parse", "--path-format=absolute", "--git-path", "info/exclude"], at: workspace.workingDirectory)
    try Data("ignored.txt\n".utf8).write(to: URL(fileURLWithPath: excludes))
    try fixture.write("ignored.txt", "ignored but user-owned", at: workspace.workingDirectory)
    await #expect(throws: (any Error).self) {
        try await client.removeCleanCheckout(workspace, otherWorkspaceDirectories: [], isBusy: false)
    }
    #expect(try String(contentsOf: workspace.workingDirectory.appendingPathComponent("ignored.txt"), encoding: .utf8) == "ignored but user-owned")
    try FileManager.default.removeItem(at: workspace.workingDirectory.appendingPathComponent("ignored.txt"))
    await #expect(throws: (any Error).self) {
        try await client.removeCleanCheckout(workspace, otherWorkspaceDirectories: [workspace.workingDirectory], isBusy: false)
    }
    try await client.removeCleanCheckout(workspace, otherWorkspaceDirectories: [], isBusy: false)
    #expect(!FileManager.default.fileExists(atPath: workspace.workingDirectory.path))
    #expect(try fixture.git(["rev-parse", "--verify", "refs/heads/" + (workspace.branch ?? "missing")]) == workspace.baseCommit)
    let restored = try await client.reattach(workspace)
    #expect(restored == workspace)
    #expect(try String(contentsOf: restored.workingDirectory.appendingPathComponent("tracked.txt"), encoding: .utf8) == "base\n")
    #expect(try await client.attach(directory: restored.workingDirectory) == workspace)
    let restoredRoot = try #require(restored.worktreeRoot).standardizedFileURL.resolvingSymlinksInPath()
    let listed = try await client.checkouts(directory: fixture.repository)
    let reattached = try #require(listed.first { $0.directory.path == restoredRoot.path })
    #expect(reattached.branch == restored.branch)
    #expect(reattached.commit == restored.baseCommit)
}

@Test func branchAndCheckpointDiffScopesIncludeTheirActualContent() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let client = WorkspaceChangesClient()
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let before = try await checkpoints.capture(directory: fixture.repository, conversationID: "chat", label: "Before")
    try fixture.write("tracked.txt", "changed\n")
    try fixture.write("new.txt", "new\n")
    let after = try await checkpoints.capture(directory: fixture.repository, conversationID: "chat", label: "After")
    let branch = try #require(try await client.snapshot(directory: fixture.repository, scope: .branch("trunk")))
    #expect(Set(branch.files.map(\.path)) == ["tracked.txt", "new.txt"])
    let scope = WorkspaceDiffScope.checkpoint(from: before.tree, to: after.tree, label: "Turn")
    let snapshot = try #require(try await client.snapshot(directory: fixture.repository, scope: scope))
    let changed = try #require(snapshot.files.first { $0.path == "tracked.txt" })
    let patch = try await client.diff(file: changed, root: fixture.repository, scope: scope)
    #expect(patch.contains("-base") && patch.contains("+changed"))
}

@Test func splitDiffPreservesUnpairedLinesAndSectionHeaders() {
    let rows = WorkspaceSplitDiffRow.parse("Staged changes\n@@ -1 +1,2 @@\n-old\n+new\n+extra\n context\n")
    #expect(rows.contains { $0.left == "Staged changes" && $0.right == "Staged changes" })
    #expect(rows.contains { $0.left == "-old" && $0.right == "+new" })
    #expect(rows.contains { $0.left.isEmpty && $0.right == "+extra" })
    #expect(rows.contains { $0.left == " context" && $0.right == " context" })
}

@Test func projectCreationNeverReplacesExistingFolderAndRetryRetainsFailure() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let client = ProjectCreationClient(recordsRoot: fixture.root.appendingPathComponent("operations"))
    let created = try await client.create(parent: fixture.root, name: "new-project", kind: .repository)
    #expect(FileManager.default.fileExists(atPath: created.appendingPathComponent(".git").path))
    #expect(throws: (any Error).self) { try fixture.git(["rev-parse", "--verify", "HEAD"], at: created) }
    try fixture.write("user.txt", "preserve", at: created)
    await #expect(throws: (any Error).self) { try await client.create(parent: fixture.root, name: "new-project", kind: .repository) }
    #expect(try String(contentsOf: created.appendingPathComponent("user.txt"), encoding: .utf8) == "preserve")
    let cloned = try await client.create(parent: fixture.root, name: "new-project", kind: .clone,
        remote: fixture.repository.path, retryInNewFolder: true)
    #expect(cloned.lastPathComponent == "new-project-2")
    #expect(try String(contentsOf: cloned.appendingPathComponent("tracked.txt"), encoding: .utf8) == "base\n")
}

@Test func failedCloneRetainsManifestAndRetryUsesAnotherFolder() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let records = fixture.root.appendingPathComponent("operations")
    let client = ProjectCreationClient(recordsRoot: records)
    do {
        _ = try await client.create(parent: fixture.root, name: "clone", kind: .clone,
            remote: fixture.root.appendingPathComponent("missing-repo").path)
        Issue.record("Expected clone failure")
    } catch let failure as ProjectCreationFailure {
        #expect(failure.retainedDirectory.lastPathComponent == "clone")
        let files = try FileManager.default.contentsOfDirectory(at: records, includingPropertiesForKeys: nil)
        let record = try JSONDecoder().decode(ProjectCreationRecord.self, from: Data(contentsOf: #require(files.first)))
        #expect(record.state == .failed)
        try fixture.write("user.txt", "after failure", at: failure.retainedDirectory)
    }
    let retried = try await client.create(parent: fixture.root, name: "clone", kind: .clone,
        remote: fixture.repository.path, retryInNewFolder: true)
    #expect(retried.lastPathComponent == "clone-2")
    #expect(try String(contentsOf: fixture.root.appendingPathComponent("clone/user.txt"), encoding: .utf8) == "after failure")
}

@Test func checkpointCaptureSkipsUntrackedNestedRepositoryAndChangesRefuseToStageIt() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let nested = fixture.repository.appendingPathComponent("n")
    try FileManager.default.createDirectory(at: nested, withIntermediateDirectories: true)
    _ = try fixture.git(["init", "-q"], at: nested)
    try fixture.write("inner.txt", "inner\n", at: nested)
    try fixture.write("outer.txt", "outer\n")
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let checkpoint = try await checkpoints.capture(directory: fixture.repository, conversationID: "nested", label: "Nested")
    #expect(checkpoint.skippedPaths == ["n"])
    let files = try fixture.git(["ls-tree", "-r", "--name-only", checkpoint.tree])
    #expect(files.split(separator: "\n").sorted() == ["outer.txt", "tracked.txt"])
    let snapshot = try #require(try await WorkspaceChangesClient().snapshot(directory: fixture.repository))
    let entry = try #require(snapshot.files.first { $0.path == "n/" })
    #expect(entry.isNestedRepository && entry.status == "Nested repository")
    await #expect(throws: (any Error).self) { try await WorkspaceGitClient().stage(directory: fixture.repository, paths: ["n/"]) }
    #expect(try fixture.git(["diff", "--cached", "--name-only"]).isEmpty)
}

@Test func checkpointCaptureReusesUnchangedBlobsButDetectsSameSizeRewrites() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let file = fixture.repository.appendingPathComponent("tracked.txt")
    try Data("aaaa\n".utf8).write(to: file)
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let first = try await checkpoints.capture(directory: fixture.repository, conversationID: "cache", label: "First")
    let unchanged = try await checkpoints.capture(directory: fixture.repository, conversationID: "cache", label: "Unchanged")
    #expect(unchanged.tree == first.tree)
    // Same size and a restored modification time: only ctime and the bytes differ.
    let modified = try FileManager.default.attributesOfItem(atPath: file.path)[.modificationDate] as? Date
    try Data("bbbb\n".utf8).write(to: file)
    try FileManager.default.setAttributes([.modificationDate: try #require(modified)], ofItemAtPath: file.path)
    let rewritten = try await checkpoints.capture(directory: fixture.repository, conversationID: "cache", label: "Rewritten")
    #expect(try fixture.git(["show", rewritten.tree + ":tracked.txt"]) == "bbbb")
    // A blob collected after its checkpoint was pruned is hashed again.
    try fixture.write("tracked.txt", "cccc\n")
    let cached = try await checkpoints.capture(directory: fixture.repository, conversationID: "cache", label: "Cached")
    let blob = try fixture.git(["rev-parse", cached.tree + ":tracked.txt"])
    let objects = try fixture.git(["rev-parse", "--git-path", "objects"])
    let loose = fixture.repository.appendingPathComponent(objects).appendingPathComponent(String(blob.prefix(2)))
        .appendingPathComponent(String(blob.dropFirst(2)))
    try FileManager.default.removeItem(at: loose)
    let recovered = try await checkpoints.capture(directory: fixture.repository, conversationID: "cache", label: "Recovered")
    #expect(try fixture.git(["cat-file", "-p", recovered.tree + ":tracked.txt"]) == "cccc")
}

@Test func checkpointRetentionPrunesOldestRefsButKeepsRestoreTarget() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let workspace = try await ConversationWorkspaceClient(managedRoot: fixture.managed)
        .prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
    let checkout = workspace.workingDirectory
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed, retainedCheckpoints: 2)
    func refs() throws -> Set<String> {
        Set(try fixture.git(["for-each-ref", "--format=%(refname)", "refs/memex/checkpoints/"], at: checkout)
            .split(separator: "\n").map(String.init))
    }
    let oldest = try await checkpoints.capture(directory: checkout, conversationID: "retain", label: "Oldest")
    try fixture.write("tracked.txt", "second\n", at: checkout)
    _ = try await checkpoints.capture(directory: checkout, conversationID: "retain", label: "Second")
    try fixture.write("tracked.txt", "third\n", at: checkout)
    let third = try await checkpoints.capture(directory: checkout, conversationID: "retain", label: "Third")
    #expect(try await checkpoints.list(directory: checkout, conversationID: "retain").map(\.label) == ["Second", "Third"])
    #expect(try refs().count == 4)
    #expect(try !refs().contains("refs/memex/checkpoints/\(oldest.id)/files"))
    // The recovery capture inside restore would evict the oldest record, but
    // never the checkpoint being restored.
    let second = try #require(try await checkpoints.list(directory: checkout, conversationID: "retain").first)
    let restored = try await checkpoints.restore(second, isolation: .init(workspace: workspace, otherWorkspaceDirectories: [], isBusy: false))
    #expect(try String(contentsOf: checkout.appendingPathComponent("tracked.txt"), encoding: .utf8) == "second\n")
    #expect(Set(try await checkpoints.list(directory: checkout, conversationID: "retain").map(\.id)) == [second.id, restored.recovery.id])
    #expect(try !refs().contains("refs/memex/checkpoints/\(third.id)/files"))
    try await checkpoints.discard(conversationID: "retain")
    #expect(try refs().isEmpty)
    #expect(try await checkpoints.list(directory: checkout, conversationID: "retain").isEmpty)
}

@Test func checkpointRestoreReplacesSymlinksAndModesFromStagedBlobs() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let workspace = try await ConversationWorkspaceClient(managedRoot: fixture.managed)
        .prepare(directory: fixture.repository, mode: .newWorktree, baseRef: "HEAD")
    let checkout = workspace.workingDirectory
    let large = Data((0..<3_000_000).map { UInt8($0 % 251) })
    try large.write(to: checkout.appendingPathComponent("large.bin"))
    try fixture.write("script.sh", "#!/bin/sh\n", at: checkout)
    try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: checkout.appendingPathComponent("script.sh").path)
    try FileManager.default.createDirectory(at: checkout.appendingPathComponent("dir/sub"), withIntermediateDirectories: true)
    try fixture.write("deep.txt", "deep\n", at: checkout.appendingPathComponent("dir/sub"))
    let checkpoints = WorkspaceCheckpointClient(storage: fixture.records, managedRoot: fixture.managed)
    let checkpoint = try await checkpoints.capture(directory: checkout, conversationID: "modes", label: "Modes")
    try FileManager.default.removeItem(at: checkout.appendingPathComponent("large.bin"))
    try FileManager.default.createSymbolicLink(atPath: checkout.appendingPathComponent("large.bin").path, withDestinationPath: "tracked.txt")
    try FileManager.default.removeItem(at: checkout.appendingPathComponent("dir"))
    try FileManager.default.setAttributes([.posixPermissions: 0o644], ofItemAtPath: checkout.appendingPathComponent("script.sh").path)
    _ = try await checkpoints.restore(checkpoint, isolation: .init(workspace: workspace, otherWorkspaceDirectories: [], isBusy: false))
    let attributes = try FileManager.default.attributesOfItem(atPath: checkout.appendingPathComponent("large.bin").path)
    #expect(attributes[.type] as? FileAttributeType == .typeRegular)
    #expect(try Data(contentsOf: checkout.appendingPathComponent("large.bin")) == large)
    #expect(try String(contentsOf: checkout.appendingPathComponent("dir/sub/deep.txt"), encoding: .utf8) == "deep\n")
    #expect((try FileManager.default.attributesOfItem(atPath: checkout.appendingPathComponent("script.sh").path)[.posixPermissions] as? NSNumber)?.intValue == 0o755)
    #expect(try String(contentsOf: checkout.appendingPathComponent("tracked.txt"), encoding: .utf8) == "base\n")
}

@Test func explicitCommitAndPushRunRepositoryHooksAndReportTheirOutput() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let hooks = fixture.repository.appendingPathComponent(".git/hooks").path
    try FileManager.default.createDirectory(atPath: hooks, withIntermediateDirectories: true)
    func hook(_ name: String, _ body: String) throws {
        let url = URL(fileURLWithPath: hooks).appendingPathComponent(name)
        try Data("#!/bin/sh\n\(body)\n".utf8).write(to: url)
        try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: url.path)
    }
    let marker = fixture.root.appendingPathComponent("pre-push-ran")
    try hook("pre-commit", "for i in $(seq 1 400); do echo \"lint progress $i\" >&2; done\necho 'lint: trailing whitespace' >&2\nexit 1")
    try hook("pre-push", "touch '\(marker.path)'")
    try fixture.write("tracked.txt", "staged\n")
    let client = WorkspaceGitClient()
    try await client.stage(directory: fixture.repository, paths: ["tracked.txt"])
    do {
        _ = try await client.commit(directory: fixture.repository, message: "Blocked")
        Issue.record("The failing pre-commit hook did not block the commit.")
    } catch {
        // The tail of hook output carries the actual failure.
        #expect(error.localizedDescription.contains("lint: trailing whitespace"))
    }
    // App-internal Git commands keep hooks disabled.
    _ = try fixture.git(["commit", "-qm", "Internal"])
    try hook("pre-commit", "exit 0")
    try fixture.write("tracked.txt", "allowed\n")
    try await client.stage(directory: fixture.repository, paths: ["tracked.txt"])
    _ = try await client.commit(directory: fixture.repository, message: "Allowed")
    #expect(try fixture.git(["log", "--format=%s"]).split(separator: "\n").prefix(2) == ["Allowed", "Internal"])
    let remote = fixture.root.appendingPathComponent("remote.git")
    _ = try fixture.git(["init", "-q", "--bare", remote.path])
    _ = try fixture.git(["remote", "add", "origin", remote.path])
    #expect(!FileManager.default.fileExists(atPath: marker.path))
    try await client.push(directory: fixture.repository, remote: "origin")
    #expect(FileManager.default.fileExists(atPath: marker.path))
}

@Test func gitCommandPathIncludesHomebrewToolsAfterInheritedEntries() {
    #expect(WorkspaceGitCommand.toolPath("/usr/bin:/custom/bin") == "/usr/bin:/custom/bin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/bin:/usr/sbin:/sbin")
    #expect(WorkspaceGitCommand.toolPath(nil).hasPrefix("/opt/homebrew/bin:"))
}

@Test func draftPullRequestTargetsPushedForkBranchAgainstParentRepository() throws {
    let fork = try WorkspaceGitClient.pullRequestTarget(branch: "feature",
        remoteURLs: ["origin": "git@github.com:me/memex.git", "upstream": "https://github.com/nicosuave/memex.git"],
        pushRemote: nil, upstreamRemote: "origin", mergeRef: "refs/heads/feature-remote")
    #expect(fork == .init(repository: "https://github.com/nicosuave/memex.git", head: "me:feature-remote"))
    let same = try WorkspaceGitClient.pullRequestTarget(branch: "feature",
        remoteURLs: ["origin": "ssh://git@github.com/nicosuave/memex"], pushRemote: nil, upstreamRemote: "origin", mergeRef: "refs/heads/feature")
    #expect(same == .init(repository: "ssh://git@github.com/nicosuave/memex", head: "feature"))
    #expect(throws: (any Error).self) {
        try WorkspaceGitClient.pullRequestTarget(branch: "feature", remoteURLs: ["origin": "git@github.com:me/memex.git"],
            pushRemote: nil, upstreamRemote: nil, mergeRef: nil)
    }
    #expect(throws: (any Error).self) {
        try WorkspaceGitClient.pullRequestTarget(branch: "feature", remoteURLs: ["origin": "git@github.com:-x/memex.git"],
            pushRemote: nil, upstreamRemote: "origin", mergeRef: "refs/heads/feature")
    }
}

@Test func branchDiffStopsAtTheDisplayLimitAndDisclosesTruncation() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    try fixture.write("tracked.txt", String(repeating: "a long changed line for the bounded diff\n", count: 20_000))
    let client = WorkspaceChangesClient()
    let snapshot = try #require(try await client.snapshot(directory: fixture.repository, scope: .branch("trunk")))
    let file = try #require(snapshot.files.first { $0.path == "tracked.txt" })
    let patch = try await client.diff(file: file, root: fixture.repository, scope: .branch("trunk"))
    #expect(patch.hasSuffix("[Diff truncated at 512 KB.]"))
    #expect(patch.utf8.count <= WorkspaceChangesClient.outputLimit + 64)
}

@Test func setupReportsStandardErrorOnSuccessAndTheTailOnFailure() async throws {
    let fixture = try WorkspaceGitFixture()
    defer { fixture.clean() }
    let setup = WorkspaceSetupClient()
    let output = try await setup.run(directory: fixture.repository, script: "echo out; echo warning >&2")
    #expect(output.contains("out") && output.contains("warning"))
    do {
        _ = try await setup.run(directory: fixture.repository, script: "for i in $(seq 1 3000); do echo \"line $i\"; done; echo 'final failure' >&2; exit 3")
        Issue.record("The failing setup command succeeded.")
    } catch {
        #expect(error.localizedDescription.contains("final failure"))
        #expect(!error.localizedDescription.contains("line 1\n"))
    }
}
