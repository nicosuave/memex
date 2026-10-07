import CryptoKit
import Foundation

struct WorkspaceCheckpoint: Identifiable, Codable, Sendable, Equatable {
    enum Moment: String, Codable, Sendable { case beforeTurn, afterTurn, manual }
    let id: String
    let conversationID: String
    let turnID: String?
    var moment: Moment? = nil
    let directory: URL
    let label: String
    let createdAt: Date
    let head: String?
    let tree: String
    let indexTree: String
}

/// Root supplies every other retained conversation's cwd (including archived
/// chats) and the current runtime state at action time. Existing/unmanaged
/// checkouts never become restore-eligible merely because this list is empty.
struct WorkspaceIsolation: Sendable {
    let workspace: ConversationWorkspace
    let otherWorkspaceDirectories: [URL]
    let isBusy: Bool
}

struct WorkspaceCheckpointRestore: Sendable {
    let restored: WorkspaceCheckpoint
    let recovery: WorkspaceCheckpoint
}

actor WorkspaceCheckpointClient {
    static let shared = WorkspaceCheckpointClient()
    let storage: URL
    let managedRoot: URL
    private var busyConversations: Set<String> = []

    init(storage: URL = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent("Library/Application Support/dev.memex.app/Checkpoints", isDirectory: true),
         managedRoot: URL = ConversationWorkspaceClient().managedRoot) {
        self.storage = storage
        self.managedRoot = managedRoot
    }

    func list(directory: URL, conversationID: String) throws -> [WorkspaceCheckpoint] {
        let folder = folder(conversationID: conversationID)
        guard FileManager.default.fileExists(atPath: folder.path) else { return [] }
        return try FileManager.default.contentsOfDirectory(at: folder, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "json" }
            .map { try JSONDecoder().decode(WorkspaceCheckpoint.self, from: Data(contentsOf: $0)) }
            .sorted { $0.createdAt < $1.createdAt }
    }

    /// Writes Git tree objects and private refs, never a commit on the user's
    /// branch and never the user's index. Ignored files remain outside snapshots.
    func capture(directory: URL, conversationID: String, turnID: String? = nil,
                 moment: WorkspaceCheckpoint.Moment = .manual, label: String) async throws -> WorkspaceCheckpoint {
        guard busyConversations.insert(conversationID).inserted else {
            throw WorkspaceGitError(message: "Another checkpoint operation is running for this chat.")
        }
        defer { busyConversations.remove(conversationID) }
        let folder = folder(conversationID: conversationID)
        return try await WorkspaceGitCommand.run { command in
            try Self.capture(directory: directory, conversationID: conversationID, turnID: turnID,
                moment: moment, label: label, folder: folder, command: command)
        }
    }

    func restore(_ checkpoint: WorkspaceCheckpoint, isolation: WorkspaceIsolation) async throws -> WorkspaceCheckpointRestore {
        guard busyConversations.insert(checkpoint.conversationID).inserted else {
            throw WorkspaceGitError(message: "Another checkpoint operation is running for this chat.")
        }
        defer { busyConversations.remove(checkpoint.conversationID) }
        let managedRoot = self.managedRoot
        let folder = folder(conversationID: checkpoint.conversationID)
        return try await WorkspaceGitCommand.run { command in
            try Self.validateIsolation(isolation, managedRoot: managedRoot, command: command)
            let root = try WorkspaceGitCommand.root(isolation.workspace.workingDirectory, command: command)
            guard root == checkpoint.directory.standardizedFileURL.resolvingSymlinksInPath() else {
                throw WorkspaceGitError(message: "This checkpoint belongs to another checkout.")
            }
            let saved = try JSONDecoder().decode(WorkspaceCheckpoint.self,
                from: Data(contentsOf: folder.appendingPathComponent(checkpoint.id + ".json")))
            guard saved == checkpoint else { throw WorkspaceGitError(message: "The checkpoint record changed. Reload checkpoints before restoring.") }
            // Preserve current work before any restore. A failed later command
            // still leaves this snapshot visible in the checkpoint list.
            let recovery = try Self.capture(directory: root, conversationID: checkpoint.conversationID,
                turnID: nil, moment: .manual, label: "Before restoring \(checkpoint.label)", folder: folder, command: command)
            let target = try WorkspaceRawTree.entries(root: root, tree: checkpoint.tree, command: command)
            let current = try WorkspaceRawTree.entries(root: root, tree: recovery.tree, command: command)
            let ignored = try WorkspaceGitCommand.data(root, ["ls-files", "--others", "--ignored", "--exclude-standard", "-z"], command: command)
                .split(separator: 0).map { String(decoding: $0, as: UTF8.self) }
            guard !ignored.contains(where: { ignoredPath in
                target.keys.contains { path in ignoredPath == path || ignoredPath.hasPrefix(path + "/") || path.hasPrefix(ignoredPath + "/") }
            }) else {
                throw WorkspaceGitError(message: "Ignored files overlap the checkpoint. Move or preserve them before restoring; they are not included in Git snapshots.")
            }
            try Self.validateIsolation(isolation, managedRoot: managedRoot, command: command)
            try WorkspaceRawTree.restore(root: root, target: target, current: current, command: command)
            _ = try WorkspaceGitCommand.text(root, ["read-tree", checkpoint.indexTree], command: command)
            return WorkspaceCheckpointRestore(restored: checkpoint, recovery: recovery)
        }
    }

    static func validateIsolation(_ isolation: WorkspaceIsolation, managedRoot: URL, command: CommandRun) throws {
        let workspace = isolation.workspace
        guard !isolation.isBusy, UUID(uuidString: workspace.id) != nil, workspace.state == .ready,
              let checkout = workspace.worktreeRoot, let manifest = workspace.metadataURL else {
            throw WorkspaceGitError(message: "File restore requires an idle, Memex-owned isolated worktree. Rewind conversation only instead.")
        }
        let expected = managedRoot.standardizedFileURL.resolvingSymlinksInPath()
            .appendingPathComponent(workspace.id).appendingPathComponent("checkout")
        guard checkout.standardizedFileURL.resolvingSymlinksInPath() == expected,
              manifest.standardizedFileURL == expected.deletingLastPathComponent().appendingPathComponent("workspace.json"),
              try JSONDecoder().decode(ConversationWorkspace.self, from: Data(contentsOf: manifest)) == workspace,
              try WorkspaceGitCommand.root(checkout, command: command) == expected else {
            throw WorkspaceGitError(message: "This checkout's ownership could not be verified. Files were not restored.")
        }
        for other in isolation.otherWorkspaceDirectories {
            let path = other.standardizedFileURL.resolvingSymlinksInPath().path
            guard path != expected.path, !path.hasPrefix(expected.path + "/"), !expected.path.hasPrefix(path + "/") else {
                throw WorkspaceGitError(message: "Another chat uses this checkout. Rewind conversation only instead.")
            }
        }
    }

    private static func capture(directory: URL, conversationID: String, turnID: String?, moment: WorkspaceCheckpoint.Moment, label: String,
                                folder: URL, command: CommandRun) throws -> WorkspaceCheckpoint {
        let root = try WorkspaceGitCommand.root(directory, command: command)
        let manager = FileManager.default
        let temporary = manager.temporaryDirectory.appendingPathComponent("memex-checkpoint-" + UUID().uuidString)
        try manager.createDirectory(at: temporary, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
        defer { try? manager.removeItem(at: temporary) }
        let head = try? WorkspaceGitCommand.text(root, ["rev-parse", "--verify", "HEAD"], command: command)
        let indexPath = try WorkspaceGitCommand.text(root, ["rev-parse", "--path-format=absolute", "--git-path", "index"], command: command)
        let copy = temporary.appendingPathComponent("index")
        if manager.fileExists(atPath: indexPath) { try manager.copyItem(at: URL(fileURLWithPath: indexPath), to: copy) }
        let environment = ["GIT_INDEX_FILE": copy.path]
        if !manager.fileExists(atPath: copy.path) {
            _ = try WorkspaceGitCommand.text(root, ["read-tree", "--empty"], command: command, environment: environment)
        }
        let indexTree = try WorkspaceGitCommand.text(root, ["write-tree"], command: command, environment: environment)
        try WorkspaceRawTree.capture(root: root, temporary: temporary, environment: environment, command: command)
        let tree = try WorkspaceGitCommand.text(root, ["write-tree"], command: command, environment: environment)
        let checkpoint = WorkspaceCheckpoint(id: UUID().uuidString.lowercased(), conversationID: conversationID,
            turnID: turnID, moment: moment, directory: root, label: label, createdAt: Date(), head: head, tree: tree, indexTree: indexTree)
        // Keep both trees alive through Git garbage collection. The refs are
        // namespaced by random checkpoint ID and do not move HEAD or branches.
        _ = try WorkspaceGitCommand.text(root, ["update-ref", "refs/memex/checkpoints/" + checkpoint.id + "/files", tree], command: command)
        _ = try WorkspaceGitCommand.text(root, ["update-ref", "refs/memex/checkpoints/" + checkpoint.id + "/index", indexTree], command: command)
        try manager.createDirectory(at: folder, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        let record = folder.appendingPathComponent(checkpoint.id + ".json")
        try JSONEncoder().encode(checkpoint).write(to: record, options: .atomic)
        try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: record.path)
        return checkpoint
    }

    private func folder(conversationID: String) -> URL {
        // A conversation may move between nested cwd and worktree root. Each
        // record owns its canonical checkout; directory matching happens before
        // restore, rather than losing snapshots when the caller's cwd changes.
        let hash = SHA256.hash(data: Data(conversationID.utf8)).map { String(format: "%02x", $0) }.joined()
        return storage.appendingPathComponent(hash, isDirectory: true)
    }
}
