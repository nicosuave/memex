import CryptoKit
import Foundation

/// A durable archive of filesystem bytes, plus independent HEAD/index trees.
/// Recovery always creates another owned worktree; it never overwrites a user
/// checkout. Ignored files are included, unlike turn checkpoints.
public struct ManagedWorkspaceSnapshot: Codable, Sendable, Equatable, Identifiable {
    public let id: String
    public let workspaceID: String
    public let createdAt: Date
    public let head: String
    public let indexTree: String
    public let archiveSHA256: String
}

extension ManagedWorkspaceStore {
    public func snapshots(_ workspace: ConversationWorkspace, command: CommandRun) throws -> [ManagedWorkspaceSnapshot] {
        try Self.validateManaged(workspace, managedRoot: managedRoot, requireCheckout: false, command: command)
        let folder = try snapshotFolder(workspace)
        guard FileManager.default.fileExists(atPath: folder.path) else { return [] }
        return try FileManager.default.contentsOfDirectory(at: folder, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "json" }
            .map { try JSONDecoder().decode(ManagedWorkspaceSnapshot.self, from: Data(contentsOf: $0)) }
            .sorted { $0.createdAt > $1.createdAt }
    }

    public func captureSnapshot(_ workspace: ConversationWorkspace, isBusy: Bool, command: CommandRun) throws -> ManagedWorkspaceSnapshot {
        guard !isBusy else { throw WorkspaceGitError(message: "Wait for the active turn before capturing a workspace snapshot.") }
        try Self.validateManaged(workspace, managedRoot: managedRoot, requireCheckout: true, command: command)
        guard let checkout = workspace.worktreeRoot else { throw WorkspaceGitError(message: "The workspace has no checkout.") }
        let manager = FileManager.default
        let folder = try snapshotFolder(workspace)
        try manager.createDirectory(at: folder, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        let id = UUID().uuidString.lowercased()
        let archive = folder.appendingPathComponent(id + ".tar.gz")
        let index = folder.appendingPathComponent(id + ".index")
        defer { try? manager.removeItem(at: index) }
        let head = try WorkspaceGitCommand.text(checkout, ["rev-parse", "HEAD"], command: command)
        let indexPath = try WorkspaceGitCommand.text(checkout, ["rev-parse", "--path-format=absolute", "--git-path", "index"], command: command)
        try manager.copyItem(at: URL(fileURLWithPath: indexPath), to: index)
        let indexTree = try WorkspaceGitCommand.text(checkout, ["write-tree"], command: command, environment: ["GIT_INDEX_FILE": index.path])
        // Refuse submodules; their external Git metadata is not recoverable
        // from this archive. The filesystem pass also rejects embedded repos.
        let tracked = try WorkspaceGitCommand.text(checkout, ["ls-files", "--stage"], command: command)
        guard !tracked.split(separator: "\n").contains(where: { $0.hasPrefix("160000 ") }) else {
            throw WorkspaceGitError(message: "Snapshot recovery does not support submodules. The checkout was retained.")
        }
        let before = try Self.filesystemFingerprint(checkout)
        // tar preserves binary contents, names, file modes, empty directories,
        // and symlinks without following them. Only the checkout's .git pointer
        // is excluded; ignored files and nested .gitignore files are retained.
        _ = try command.execute(executable: URL(fileURLWithPath: "/usr/bin/tar"),
            arguments: ["-czf", archive.path, "--exclude=./.git", "-C", checkout.path, "."], timeout: 300)
        try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: archive.path)
        // Do not publish a mixed read if another process changed the checkout
        // during capture. No original files or index entries have been touched.
        try manager.removeItem(at: index)
        try manager.copyItem(at: URL(fileURLWithPath: indexPath), to: index)
        let finalIndexTree = try WorkspaceGitCommand.text(checkout, ["write-tree"], command: command, environment: ["GIT_INDEX_FILE": index.path])
        guard before == (try Self.filesystemFingerprint(checkout)), finalIndexTree == indexTree,
              try WorkspaceGitCommand.text(checkout, ["rev-parse", "HEAD"], command: command) == head else {
            throw WorkspaceGitError(message: "The checkout changed during snapshot capture. No recovery snapshot was published; retry when it is idle.")
        }
        let snapshot = ManagedWorkspaceSnapshot(id: id, workspaceID: workspace.id, createdAt: Date(), head: head,
            indexTree: indexTree, archiveSHA256: try Self.snapshotDigest(archive))
        // Anchor both commits and staged contents independently of the branch.
        _ = try WorkspaceGitCommand.text(checkout, ["update-ref", "refs/memex/snapshots/" + id + "/head", head], command: command)
        _ = try WorkspaceGitCommand.text(checkout, ["update-ref", "refs/memex/snapshots/" + id + "/index", indexTree], command: command)
        try JSONEncoder().encode(snapshot).write(to: folder.appendingPathComponent(id + ".json"), options: .atomic)
        return snapshot
    }

    public func recoverSnapshot(_ snapshot: ManagedWorkspaceSnapshot, workspace: ConversationWorkspace,
                                command: CommandRun) throws -> ConversationWorkspace {
        try Self.validateManaged(workspace, managedRoot: managedRoot, requireCheckout: false, command: command)
        guard UUID(uuidString: snapshot.id) != nil, snapshot.workspaceID == workspace.id,
              let repository = workspace.repositoryRoot else { throw WorkspaceGitError(message: "Snapshot ownership does not match this workspace.") }
        let folder = try snapshotFolder(workspace)
        let saved = try JSONDecoder().decode(ManagedWorkspaceSnapshot.self,
            from: Data(contentsOf: folder.appendingPathComponent(snapshot.id + ".json")))
        let archive = folder.appendingPathComponent(snapshot.id + ".tar.gz")
        guard saved == snapshot, try Self.snapshotDigest(archive) == snapshot.archiveSHA256 else {
            throw WorkspaceGitError(message: "The snapshot archive changed or is incomplete. No checkout was modified.")
        }
        // A fresh managed branch keeps the original checkout and its branch
        // intact, even when the branch moved after a snapshot or cleanup.
        let recovered = try prepare(directory: repository, newWorktree: true, baseRef: snapshot.head, command: command)
        guard let checkout = recovered.worktreeRoot else { throw WorkspaceGitError(message: "Recovery checkout could not be created.") }
        do {
            for child in try FileManager.default.contentsOfDirectory(at: checkout, includingPropertiesForKeys: nil) where child.lastPathComponent != ".git" {
                try FileManager.default.removeItem(at: child)
            }
            _ = try command.execute(executable: URL(fileURLWithPath: "/usr/bin/tar"),
                arguments: ["-xzf", archive.path, "-C", checkout.path], timeout: 300)
            _ = try WorkspaceGitCommand.text(checkout, ["read-tree", snapshot.indexTree], command: command)
            return recovered
        } catch {
            throw WorkspaceGitError(message: "Recovery stopped; the snapshot and partial recovery checkout at \(checkout.path) were retained. \(error.localizedDescription)")
        }
    }

    private func snapshotFolder(_ workspace: ConversationWorkspace) throws -> URL {
        guard let metadata = workspace.metadataURL else { throw WorkspaceGitError(message: "Workspace ownership is unavailable.") }
        return metadata.deletingLastPathComponent().appendingPathComponent("snapshots", isDirectory: true)
    }

    private static func filesystemFingerprint(_ checkout: URL) throws -> [String: String] {
        let manager = FileManager.default
        var result: [String: String] = [:]
        var enumerationError: Error?
        guard let enumerator = manager.enumerator(at: checkout, includingPropertiesForKeys: nil,
            errorHandler: { _, error in enumerationError = error; return false }) else {
            throw WorkspaceGitError(message: "The checkout could not be read for snapshot capture.")
        }
        while let path = enumerator.nextObject() as? URL {
            if path.lastPathComponent == ".git" {
                guard path.deletingLastPathComponent().standardizedFileURL.path == checkout.standardizedFileURL.path else {
                    throw WorkspaceGitError(message: "Snapshot recovery does not support embedded repositories. The checkout was retained.")
                }
                enumerator.skipDescendants()
                continue
            }
            let attributes = try manager.attributesOfItem(atPath: path.path)
            let mode = String(describing: attributes[.posixPermissions])
            switch attributes[.type] as? FileAttributeType {
            case .typeRegular:
                result[path.path] = mode + ":file:" + (try snapshotDigest(path))
            case .typeSymbolicLink:
                result[path.path] = mode + ":link:" + (try manager.destinationOfSymbolicLink(atPath: path.path))
                enumerator.skipDescendants()
            case .typeDirectory:
                result[path.path] = mode + ":directory"
            default:
                throw WorkspaceGitError(message: "The checkout contains a special filesystem entry that cannot be safely snapshotted: \(path.path)")
            }
        }
        if let enumerationError { throw enumerationError }
        return result
    }

    private static func snapshotDigest(_ url: URL) throws -> String {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var hash = SHA256()
        while let chunk = try handle.read(upToCount: 1024 * 1024), !chunk.isEmpty { hash.update(data: chunk) }
        return hash.finalize().map { String(format: "%02x", $0) }.joined()
    }
}
