import Foundation

public struct WorkspaceCheckout: Identifiable, Sendable, Equatable {
    public let directory: URL
    public let branch: String?
    public let commit: String?
    public let locked: Bool
    public var id: String { directory.path }
}

public struct ManagedWorkspaceEntry: Identifiable, Sendable {
    public let workspace: ConversationWorkspace
    public let archived: Bool
    public let removed: Bool
    public var id: String { workspace.id }
}

extension ManagedWorkspaceStore {
    private struct Lifecycle: Codable { var archived = false; var removed = false }

    public func checkouts(directory: URL, command: CommandRun) throws -> [WorkspaceCheckout] {
        let root = try WorkspaceGitCommand.root(directory, command: command)
        let bytes = try WorkspaceGitCommand.data(root, ["worktree", "list", "--porcelain", "-z"], command: command)
        var result: [WorkspaceCheckout] = []
        var path: String?
        var branch: String?
        var commit: String?
        var locked = false
        func append() {
            if let path {
                result.append(.init(directory: URL(fileURLWithPath: path, isDirectory: true)
                    .standardizedFileURL.resolvingSymlinksInPath(), branch: branch, commit: commit, locked: locked))
            }
            path = nil; branch = nil; commit = nil; locked = false
        }
        for field in bytes.split(separator: 0, omittingEmptySubsequences: false).map({ String(decoding: $0, as: UTF8.self) }) {
            if field.isEmpty { append() }
            else if field.hasPrefix("worktree ") { path = String(field.dropFirst(9)) }
            else if field.hasPrefix("branch refs/heads/") { branch = String(field.dropFirst(18)) }
            else if field.hasPrefix("HEAD ") { commit = String(field.dropFirst(5)) }
            else if field == "locked" || field.hasPrefix("locked ") { locked = true }
        }
        append()
        return result
    }

    /// Reuse an existing checkout as-is, without claiming ownership of an
    /// arbitrary directory or copying another chat's dirty files.
    public func attach(directory: URL, command: CommandRun) throws -> ConversationWorkspace {
        let root = try WorkspaceGitCommand.root(directory, command: command)
        let manifest = root.deletingLastPathComponent().appendingPathComponent("workspace.json")
        let canonicalManaged = managedRoot.standardizedFileURL.resolvingSymlinksInPath()
        if root.path.hasPrefix(canonicalManaged.path + "/"),
           let workspace = try? JSONDecoder().decode(ConversationWorkspace.self, from: Data(contentsOf: manifest)),
           workspace.worktreeRoot?.standardizedFileURL.resolvingSymlinksInPath() == root,
           workspace.metadataURL == manifest, workspace.state == .ready {
            return workspace
        }
        return try prepare(directory: directory, newWorktree: false, command: command)
    }

    public func managedWorkspaces() throws -> [ManagedWorkspaceEntry] {
        let manager = FileManager.default
        guard manager.fileExists(atPath: managedRoot.path) else { return [] }
        return try manager.contentsOfDirectory(at: managedRoot, includingPropertiesForKeys: [.isDirectoryKey])
            .compactMap { reservation in
                let manifest = reservation.appendingPathComponent("workspace.json")
                guard manager.fileExists(atPath: manifest.path) else { return nil }
                let workspace = try JSONDecoder().decode(ConversationWorkspace.self, from: Data(contentsOf: manifest))
                guard reservation.lastPathComponent == workspace.id else { throw WorkspaceGitError(message: "A workspace manifest has an unexpected identity: \(manifest.path)") }
                let lifecycle = try Self.readLifecycle(workspace)
                return ManagedWorkspaceEntry(workspace: workspace, archived: lifecycle.archived, removed: lifecycle.removed)
            }
    }

    /// Archiving hides the entry but deliberately retains every file, including
    /// ignored output and uncommitted work. Disk cleanup is a separate action.
    public func setArchived(_ workspace: ConversationWorkspace, archived: Bool, command: CommandRun) throws {
        try Self.validateManaged(workspace, managedRoot: managedRoot, requireCheckout: false, command: command)
        var lifecycle = try Self.readLifecycle(workspace)
        lifecycle.archived = archived
        try Self.writeLifecycle(lifecycle, workspace: workspace)
    }

    /// Removes only an idle, owned, entirely clean checkout. Ignored files count
    /// as work. The branch and all commits remain for explicit reattachment.
    public func removeCleanCheckout(_ workspace: ConversationWorkspace, otherWorkspaceDirectories: [URL], isBusy: Bool, command: CommandRun) throws {
        guard !isBusy else { throw WorkspaceGitError(message: "An active conversation uses this checkout. Cleanup was refused.") }
        try Self.validateManaged(workspace, managedRoot: managedRoot, requireCheckout: true, command: command)
        let expected = workspace.worktreeRoot!.standardizedFileURL.resolvingSymlinksInPath().path
        for other in otherWorkspaceDirectories {
            let path = other.standardizedFileURL.resolvingSymlinksInPath().path
            guard path != expected, !path.hasPrefix(expected + "/"), !expected.hasPrefix(path + "/") else {
                throw WorkspaceGitError(message: "Another retained chat uses this checkout. Cleanup was refused.")
            }
        }
        guard let checkout = workspace.worktreeRoot, let repository = workspace.repositoryRoot,
              let branch = workspace.branch else { throw WorkspaceGitError(message: "Workspace ownership is unavailable.") }
        let current = try WorkspaceGitCommand.text(checkout, ["branch", "--show-current"], command: command)
        guard current == branch else { throw WorkspaceGitError(message: "The checkout changed branches. Keep it or reattach it explicitly before cleanup.") }
        let status = try WorkspaceGitCommand.text(checkout, ["status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignored"], command: command)
        guard status.isEmpty else { throw WorkspaceGitError(message: "This checkout contains staged, unstaged, untracked or ignored files. Cleanup was refused; archive it to retain those files.") }
        _ = try captureSnapshot(workspace, isBusy: isBusy, command: command)
        // Snapshot creation can take time. Recheck eligibility before the
        // non-forced Git removal; preserve the snapshot if cleanup is refused.
        let finalStatus = try WorkspaceGitCommand.text(checkout, ["status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignored"], command: command)
        guard finalStatus.isEmpty,
              try WorkspaceGitCommand.text(checkout, ["branch", "--show-current"], command: command) == branch else {
            throw WorkspaceGitError(message: "The checkout changed while capturing its snapshot. Snapshot saved; cleanup was refused.")
        }
        _ = try WorkspaceGitCommand.text(repository, ["worktree", "remove", "--", checkout.path], command: command, timeout: 120)
        var lifecycle = try Self.readLifecycle(workspace)
        lifecycle.removed = true
        lifecycle.archived = true
        try Self.writeLifecycle(lifecycle, workspace: workspace)
    }

    public func reattach(_ workspace: ConversationWorkspace, command: CommandRun) throws -> ConversationWorkspace {
        try Self.validateManaged(workspace, managedRoot: managedRoot, requireCheckout: false, command: command)
        guard let checkout = workspace.worktreeRoot, let repository = workspace.repositoryRoot,
              let branch = workspace.branch else { throw WorkspaceGitError(message: "Workspace ownership is unavailable.") }
        if !FileManager.default.fileExists(atPath: checkout.path) {
            _ = try WorkspaceGitCommand.text(repository, ["worktree", "add", "--", checkout.path, branch], command: command, timeout: 120)
        }
        try Self.validateManaged(workspace, managedRoot: managedRoot, requireCheckout: true, command: command)
        guard try WorkspaceGitCommand.text(checkout, ["branch", "--show-current"], command: command) == branch else {
            throw WorkspaceGitError(message: "The checkout changed branches. Reattachment left its files and branch unchanged.")
        }
        try Self.writeLifecycle(Lifecycle(), workspace: workspace)
        return workspace
    }

    public static func validateManaged(_ workspace: ConversationWorkspace, managedRoot: URL,
                                        requireCheckout: Bool, command: CommandRun) throws {
        let reservation = managedRoot.standardizedFileURL.resolvingSymlinksInPath().appendingPathComponent(workspace.id)
        // URL equality includes the directory hint/trailing slash. Ownership is
        // a filesystem identity check, including after cleanup removes checkout.
        guard UUID(uuidString: workspace.id) != nil, reservation.resolvingSymlinksInPath().path == reservation.path,
              workspace.state == .ready, let manifest = workspace.metadataURL,
              manifest.standardizedFileURL.path == reservation.appendingPathComponent("workspace.json").path,
              manifest.resolvingSymlinksInPath().path == manifest.standardizedFileURL.path,
              workspace.worktreeRoot?.standardizedFileURL.path == reservation.appendingPathComponent("checkout").path,
              try JSONDecoder().decode(ConversationWorkspace.self, from: Data(contentsOf: manifest)) == workspace else {
            throw WorkspaceGitError(message: "Only verified Memex-owned worktrees can be managed here.")
        }
        guard let repository = workspace.repositoryRoot else { throw WorkspaceGitError(message: "Workspace repository ownership is unavailable.") }
        let source = workspace.sourceDirectory.standardizedFileURL.path
        let repositoryPath = repository.standardizedFileURL.path
        guard source == repositoryPath || source.hasPrefix(repositoryPath + "/") else {
            throw WorkspaceGitError(message: "The workspace source is outside its recorded repository.")
        }
        let relative = source == repositoryPath ? "" : String(source.dropFirst(repositoryPath.count + 1))
        let expectedDirectory = reservation.appendingPathComponent("checkout").appendingPathComponent(relative).standardizedFileURL.path
        guard workspace.workingDirectory.standardizedFileURL.path == expectedDirectory else {
            throw WorkspaceGitError(message: "The workspace working directory does not match its owned checkout.")
        }
        if requireCheckout {
            let checkout = reservation.appendingPathComponent("checkout")
            guard checkout.resolvingSymlinksInPath().path == checkout.path,
                  workspace.workingDirectory.resolvingSymlinksInPath().path == expectedDirectory,
                  try WorkspaceGitCommand.root(checkout, command: command).path == checkout.path,
                  try WorkspaceGitCommand.text(checkout, ["rev-parse", "--path-format=absolute", "--git-common-dir"], command: command)
                    == WorkspaceGitCommand.text(repository, ["rev-parse", "--path-format=absolute", "--git-common-dir"], command: command) else {
                throw WorkspaceGitError(message: "The managed checkout resolves to a different repository.")
            }
        }
    }

    private static func readLifecycle(_ workspace: ConversationWorkspace) throws -> Lifecycle {
        guard let manifest = workspace.metadataURL else { throw WorkspaceGitError(message: "The workspace has no ownership manifest.") }
        let url = manifest.deletingLastPathComponent().appendingPathComponent("lifecycle.json")
        guard FileManager.default.fileExists(atPath: url.path) else { return Lifecycle() }
        return try JSONDecoder().decode(Lifecycle.self, from: Data(contentsOf: url))
    }

    private static func writeLifecycle(_ value: Lifecycle, workspace: ConversationWorkspace) throws {
        guard let manifest = workspace.metadataURL else { throw WorkspaceGitError(message: "The workspace has no ownership manifest.") }
        let url = manifest.deletingLastPathComponent().appendingPathComponent("lifecycle.json")
        try JSONEncoder().encode(value).write(to: url, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
    }
}
