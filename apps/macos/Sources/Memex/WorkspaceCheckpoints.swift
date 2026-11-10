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
    /// Untracked nested repositories this checkpoint did not capture.
    var skippedPaths: [String]? = nil
    /// The shared Git directory that holds this checkpoint's refs, so they can
    /// be deleted after a linked worktree is removed.
    var gitDirectory: URL? = nil
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

/// Retention: each checkpoint pins its file and index trees with two refs
/// under refs/memex/checkpoints/<id>/. A conversation keeps its most recent
/// `retainedCheckpoints` records; capturing another deletes the oldest records
/// and their refs so Git can collect the objects. A checkpoint being restored
/// is never pruned (the restore's own recovery capture protects it), and
/// `discard(conversationID:)` deletes every record and ref of a chat.
actor WorkspaceCheckpointClient {
    static let shared = WorkspaceCheckpointClient()
    let storage: URL
    let managedRoot: URL
    let retainedCheckpoints: Int
    private var busyConversations: Set<String> = []

    init(storage: URL = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent("Library/Application Support/dev.memex.app/Checkpoints", isDirectory: true),
         managedRoot: URL = ConversationWorkspaceClient().managedRoot, retainedCheckpoints: Int = 100) {
        self.storage = storage
        self.managedRoot = managedRoot
        self.retainedCheckpoints = max(2, retainedCheckpoints)
    }

    func list(directory: URL, conversationID: String) throws -> [WorkspaceCheckpoint] {
        try Self.records(folder: folder(conversationID: conversationID))
    }

    private static func records(folder: URL) throws -> [WorkspaceCheckpoint] {
        guard FileManager.default.fileExists(atPath: folder.path) else { return [] }
        return try FileManager.default.contentsOfDirectory(at: folder, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "json" }
            .map { try JSONDecoder().decode(WorkspaceCheckpoint.self, from: Data(contentsOf: $0)) }
            .sorted { $0.createdAt < $1.createdAt }
    }

    /// Deletes every checkpoint record and ref of a conversation that no
    /// longer needs file restore.
    func discard(conversationID: String) async throws {
        guard busyConversations.insert(conversationID).inserted else {
            throw WorkspaceGitError(message: "Another checkpoint operation is running for this chat.")
        }
        defer { busyConversations.remove(conversationID) }
        let folder = folder(conversationID: conversationID)
        try await WorkspaceGitCommand.run { command in
            for checkpoint in try Self.records(folder: folder) { try Self.delete(checkpoint, folder: folder, command: command) }
            try? FileManager.default.removeItem(at: folder)
        }
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
        let cache = cacheFolder
        let retained = retainedCheckpoints
        return try await WorkspaceGitCommand.run { command in
            try Self.capture(directory: directory, conversationID: conversationID, turnID: turnID,
                moment: moment, label: label, folder: folder, cache: cache, retained: retained, protected: [], command: command)
        }
    }

    func restore(_ checkpoint: WorkspaceCheckpoint, isolation: WorkspaceIsolation) async throws -> WorkspaceCheckpointRestore {
        guard busyConversations.insert(checkpoint.conversationID).inserted else {
            throw WorkspaceGitError(message: "Another checkpoint operation is running for this chat.")
        }
        defer { busyConversations.remove(checkpoint.conversationID) }
        let managedRoot = self.managedRoot
        let folder = folder(conversationID: checkpoint.conversationID)
        let cache = cacheFolder
        let retained = retainedCheckpoints
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
                turnID: nil, moment: .manual, label: "Before restoring \(checkpoint.label)", folder: folder, cache: cache,
                retained: retained, protected: [checkpoint.id], command: command)
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
            do {
                try WorkspaceRawTree.restore(root: root, target: target, current: current, command: command)
                _ = try WorkspaceGitCommand.text(root, ["read-tree", checkpoint.indexTree], command: command)
            } catch {
                // Name the recovery checkpoint: some files may already have
                // been replaced when a later step failed or timed out.
                throw WorkspaceGitError(message: "Restore stopped: \(error.localizedDescription) Files may be partly restored; "
                    + "your previous files are saved in the checkpoint \"\(recovery.label)\".")
            }
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
                                folder: URL, cache: URL, retained: Int, protected: Set<String>, command: CommandRun) throws -> WorkspaceCheckpoint {
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
        let skipped = try WorkspaceRawTree.capture(root: root, temporary: temporary, environment: environment,
            cache: cache.appendingPathComponent(cacheName(root)), command: command)
        let tree = try WorkspaceGitCommand.text(root, ["write-tree"], command: command, environment: environment)
        let gitDirectory = try? WorkspaceGitCommand.text(root, ["rev-parse", "--path-format=absolute", "--git-common-dir"], command: command)
        let checkpoint = WorkspaceCheckpoint(id: UUID().uuidString.lowercased(), conversationID: conversationID,
            turnID: turnID, moment: moment, directory: root, label: label, createdAt: Date(), head: head, tree: tree, indexTree: indexTree,
            skippedPaths: skipped.isEmpty ? nil : skipped, gitDirectory: gitDirectory.map { URL(fileURLWithPath: $0, isDirectory: true) })
        // Keep both trees alive through Git garbage collection. The refs are
        // namespaced by random checkpoint ID and do not move HEAD or branches.
        _ = try WorkspaceGitCommand.text(root, ["update-ref", "refs/memex/checkpoints/" + checkpoint.id + "/files", tree], command: command)
        _ = try WorkspaceGitCommand.text(root, ["update-ref", "refs/memex/checkpoints/" + checkpoint.id + "/index", indexTree], command: command)
        try manager.createDirectory(at: folder, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        let record = folder.appendingPathComponent(checkpoint.id + ".json")
        try JSONEncoder().encode(checkpoint).write(to: record, options: .atomic)
        try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: record.path)
        // Retention is best effort; a failed prune never fails the capture.
        if let existing = try? records(folder: folder), existing.count > retained {
            var excess = existing.count - retained
            for old in existing where excess > 0 && !protected.contains(old.id) && old.id != checkpoint.id {
                try? delete(old, folder: folder, command: command)
                excess -= 1
            }
        }
        return checkpoint
    }

    /// Removes the record first, so a failed ref deletion leaves at worst an
    /// unlisted ref, never a listed checkpoint whose trees may be collected.
    private static func delete(_ checkpoint: WorkspaceCheckpoint, folder: URL, command: CommandRun) throws {
        guard UUID(uuidString: checkpoint.id) != nil else { return }
        let record = folder.appendingPathComponent(checkpoint.id + ".json")
        if FileManager.default.fileExists(atPath: record.path) { try FileManager.default.removeItem(at: record) }
        let repository = [checkpoint.gitDirectory, checkpoint.directory].compactMap { $0 }
            .first { FileManager.default.fileExists(atPath: $0.path) }
        guard let repository else { return }
        for kind in ["files", "index"] {
            _ = try? WorkspaceGitCommand.text(repository, ["update-ref", "-d", "refs/memex/checkpoints/" + checkpoint.id + "/" + kind], command: command)
        }
    }

    private var cacheFolder: URL { storage.appendingPathComponent("capture-cache", isDirectory: true) }

    private static func cacheName(_ root: URL) -> String {
        SHA256.hash(data: Data(root.path.utf8)).map { String(format: "%02x", $0) }.joined()
    }

    private func folder(conversationID: String) -> URL {
        // A conversation may move between nested cwd and worktree root. Each
        // record owns its canonical checkout; directory matching happens before
        // restore, rather than losing snapshots when the caller's cwd changes.
        let hash = SHA256.hash(data: Data(conversationID.utf8)).map { String(format: "%02x", $0) }.joined()
        return storage.appendingPathComponent(hash, isDirectory: true)
    }
}
