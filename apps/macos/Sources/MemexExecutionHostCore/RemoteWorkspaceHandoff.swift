import Foundation
import CryptoKit
import Darwin

public struct WorkspaceTransferEntry: Codable, Equatable, Sendable {
    public var path: String
    public var kind: String
    public var mode: Int
    public var data: Data
}

/// All checked-out files (including ignored), the exact index and its staged
/// blobs travel together. Payload limits reject whole transfers, never omit files.
public struct WorkspaceTransfer: Codable, Sendable {
    public var sourcePath: String
    public var head: String
    public var branch: String
    public var entries: [WorkspaceTransferEntry]
    public var index: Data
    public var indexObjects: [String: Data]
    public var bundle: Data
}

public final class RemoteWorkspaceHandoff {
    public static let byteLimit = 16 * 1024 * 1024
    private let directory: URL
    public init(directory: URL) { self.directory = directory }
    public func requireCleanDestination(_ root: URL) throws {
        let command = CommandRun()
        try validateRepository(root, command: command)
        guard try git(root, ["status", "--porcelain=v1", "--untracked-files=all", "--ignored"], command).isEmpty else {
            throw HostFailure("handoff_conflict", "Destination contains changed, untracked, or ignored files; preserve them before moving here")
        }
    }

    public func requireSnapshot(_ snapshot: WorkspaceTransfer, at root: URL) throws {
        let command = CommandRun()
        try validateRepository(root, command: command)
        guard try tree(root) == snapshot.entries, try boundedData(indexPath(root, command)) == snapshot.index,
              try git(root, ["rev-parse", "HEAD"], command) == snapshot.head else {
            throw HostFailure("handoff_conflict", "Retained destination changed since its previous handoff; preserve those changes before returning here")
        }
    }

    public func export(root: URL) throws -> WorkspaceTransfer {
        let command = CommandRun()
        try validateRepository(root, command: command)
        let head = try git(root, ["rev-parse", "HEAD"], command)
        let branch = (try? git(root, ["symbolic-ref", "--quiet", "HEAD"], command)) ?? ""
        let indexURL = try indexPath(root, command)
        guard (try git(root, ["rev-parse", "--shared-index-path"], command)).isEmpty else {
            throw HostFailure("handoff_unsupported", "Disable split-index explicitly before moving this workspace")
        }
        let index = try boundedData(indexURL)
        let entries = try tree(root)
        let listing = try WorkspaceGitCommand.data(root, ["ls-files", "--stage", "-z"], command: command, maximumOutputBytes: Self.byteLimit)
        var objects: [String: Data] = [:]
        for record in listing.split(separator: 0) {
            let header = String(decoding: record.prefix(while: { $0 != 9 }), as: UTF8.self).split(separator: " ")
            guard header.count == 3, header[0] != "160000", header[2] == "0" else {
                throw HostFailure("handoff_unsupported", "Resolve merge conflicts and remove submodule checkouts before handoff")
            }
            let oid = String(header[1])
            if objects[oid] == nil {
                let remaining = Self.byteLimit - objects.values.reduce(0) { $0 + $1.count }
                objects[oid] = try WorkspaceGitCommand.data(root, ["cat-file", "blob", oid], command: command, maximumOutputBytes: remaining)
            }
        }
        let scratch = try makeDirectory()
        let bundleURL = scratch.appendingPathComponent("repository.bundle")
        defer { try? FileManager.default.removeItem(at: scratch) }
        _ = try git(root, ["bundle", "create", bundleURL.path, "HEAD"], command)
        let transfer = WorkspaceTransfer(sourcePath: root.path, head: head, branch: branch, entries: entries,
            index: index, indexObjects: objects, bundle: try boundedData(bundleURL))
        try validate(transfer)
        // Detect writers while capturing; never bind a partial/racing snapshot.
        guard try tree(root) == entries, try boundedData(indexURL) == index,
              try git(root, ["rev-parse", "HEAD"], command) == head else {
            throw HostFailure("handoff_conflict", "Source workspace changed during capture; retry once its writers are idle")
        }
        return transfer
    }

    /// Destination must be a separately granted, clean repository root. Both
    /// snapshots persist before changing it. Source checkout is never modified.
    /// The returned recovery ID identifies exact snapshots for explicit recovery.
    public func install(_ transfer: WorkspaceTransfer, destination: URL, operationID: String, retainedSnapshot: WorkspaceTransfer? = nil) throws -> String {
        try validate(transfer)
        guard UUID(uuidString: operationID) != nil else { throw HostFailure("invalid_params", "A UUID handoff operation is required") }
        let command = CommandRun()
        try validateRepository(destination, command: command)
        if let retainedSnapshot { try requireSnapshot(retainedSnapshot, at: destination) }
        else { try requireCleanDestination(destination) }
        let recovery = directory.appendingPathComponent(operationID)
        guard !FileManager.default.fileExists(atPath: recovery.path) else {
            throw HostFailure("handoff_recovery", "This operation already has recovery snapshots. Inspect its recorded phase rather than repeating it.")
        }
        let previous = try export(root: destination)
        try FileManager.default.createDirectory(at: recovery, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        try persist(previous, at: recovery.appendingPathComponent("destination.json"))
        try persist(transfer, at: recovery.appendingPathComponent("source.json"))
        try persist(["phase": "prepared", "destination": destination.path], at: recovery.appendingPathComponent("state.json"))
        guard try tree(destination) == previous.entries else { throw HostFailure("handoff_conflict", "Destination changed while preparing recovery") }
        do {
            try apply(transfer, to: destination, expected: previous, recovery: recovery, command: command)
            try persist(["phase": "installed", "destination": destination.path], at: recovery.appendingPathComponent("state.json"))
            return operationID
        } catch {
            try? persist(["phase": "needs_inspection", "destination": destination.path, "error": error.localizedDescription], at: recovery.appendingPathComponent("state.json"))
            throw HostFailure("handoff_recovery", "Handoff stopped; source is retained. Inspect recovery \(operationID) before continuing. \(error.localizedDescription)")
        }
    }

    public func recover(operationID: String, destination: URL) throws {
        guard UUID(uuidString: operationID) != nil else { throw HostFailure("invalid_params", "Invalid recovery identity") }
        let recovery = directory.appendingPathComponent(operationID)
        guard FileManager.default.fileExists(atPath: recovery.path) else { return } // Failed preflight did not modify files.
        let state = try JSONDecoder().decode([String: String].self, from: boundedData(recovery.appendingPathComponent("state.json")))
        guard state["destination"] == destination.path else {
            throw HostFailure("handoff_recovery", "Recovery snapshot belongs to another destination")
        }
        let source = try JSONDecoder().decode(WorkspaceTransfer.self, from: Data(contentsOf: recovery.appendingPathComponent("source.json")))
        let previous = try JSONDecoder().decode(WorkspaceTransfer.self, from: Data(contentsOf: recovery.appendingPathComponent("destination.json")))
        let command = CommandRun()
        if try tree(destination) != previous.entries || boundedData(indexPath(destination, command)) != previous.index || git(destination, ["rev-parse", "HEAD"], command) != previous.head {
            try apply(previous, to: destination, expected: source, recovery: recovery, command: command)
        }
        // Restore the original branch only if its tip still matches the snapshot.
        if !previous.branch.isEmpty {
            let command = CommandRun()
            guard try git(destination, ["rev-parse", previous.branch], command) == previous.head else {
                throw HostFailure("handoff_recovery", "Original branch advanced; restored files remain on detached HEAD")
            }
            _ = try git(destination, ["symbolic-ref", "HEAD", previous.branch], command)
        }
        try persist(["phase": "restored", "destination": destination.path], at: recovery.appendingPathComponent("state.json"))
    }

    private func apply(_ transfer: WorkspaceTransfer, to destination: URL, expected: WorkspaceTransfer,
                       recovery: URL, command: CommandRun) throws {
        try validate(transfer)
        let index = try indexPath(destination, command)
        guard try tree(destination) == expected.entries, try boundedData(index) == expected.index,
              try git(destination, ["rev-parse", "HEAD"], command) == expected.head else {
            throw HostFailure("handoff_conflict", "Destination changed since its snapshot; recovery will not overwrite it")
        }
        let bundleURL = recovery.appendingPathComponent("transfer.bundle")
        try transfer.bundle.write(to: bundleURL, options: .atomic)
        _ = try git(destination, ["bundle", "verify", bundleURL.path], command)
        _ = try git(destination, ["fetch", "--no-tags", bundleURL.path, "HEAD"], command)
        guard try git(destination, ["rev-parse", "FETCH_HEAD"], command) == transfer.head else {
            throw HostFailure("handoff_identity", "Repository bundle does not match the captured commit")
        }
        for (oid, data) in transfer.indexObjects {
            let input = recovery.appendingPathComponent("object-input")
            try data.write(to: input, options: .atomic)
            let stored = String(decoding: try WorkspaceGitCommand.data(destination, ["hash-object", "-w", "--stdin"], command: command, inputFile: input), as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
            guard stored == oid else { throw HostFailure("handoff_identity", "Staged Git object failed its identity check") }
        }
        // Reserve the index against other Git writers for the file replacement.
        let indexLock = index.appendingPathExtension("lock")
        let fd = Darwin.open(indexLock.path, O_WRONLY | O_CREAT | O_EXCL, 0o600)
        guard fd >= 0 else { throw HostFailure("handoff_conflict", "Destination Git index is in use") }
        defer { Darwin.close(fd); try? FileManager.default.removeItem(at: indexLock) }
        guard try tree(destination) == expected.entries, try boundedData(index) == expected.index else {
            throw HostFailure("handoff_conflict", "Destination changed before applying the handoff")
        }
        let destinationFD = open(destination.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard destinationFD >= 0 else { throw HostFailure("workspace_denied", "Destination root changed before replacement") }
        defer { close(destinationFD) }
        for entry in expected.entries.sorted(by: { $0.path.count > $1.path.count }) {
            let parent = try openParent(entry.path, root: destinationFD)
            defer { close(parent.fd) }
            var current = stat()
            guard fstatat(parent.fd, parent.name, &current, AT_SYMLINK_NOFOLLOW) == 0,
                  (entry.kind == "directory" ? current.st_mode & S_IFMT == S_IFDIR :
                    entry.kind == "symlink" ? current.st_mode & S_IFMT == S_IFLNK : current.st_mode & S_IFMT == S_IFREG) else {
                throw HostFailure("handoff_conflict", "Destination entry changed before replacement; recovery snapshots are retained")
            }
            guard unlinkat(parent.fd, parent.name, entry.kind == "directory" ? AT_REMOVEDIR : 0) == 0 else {
                throw HostFailure("handoff_conflict", "Destination changed while applying; recovery snapshots are retained")
            }
        }
        for entry in transfer.entries.sorted(by: { $0.path.count < $1.path.count }) {
            let parent = try openParent(entry.path, root: destinationFD)
            defer { close(parent.fd) }
            if entry.kind == "directory" {
                guard mkdirat(parent.fd, parent.name, mode_t(entry.mode | 0o700)) == 0 else { throw HostFailure("handoff_io", "Cannot create destination directory") }
            } else if entry.kind == "symlink" {
                guard symlinkat(String(decoding: entry.data, as: UTF8.self), parent.fd, parent.name) == 0 else { throw HostFailure("handoff_io", "Cannot create destination symbolic link") }
            } else {
                let fd = openat(parent.fd, parent.name, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
                guard fd >= 0 else { throw HostFailure("handoff_conflict", "Destination file appeared while applying") }
                let handle = FileHandle(fileDescriptor: fd, closeOnDealloc: true)
                defer { try? handle.close() }
                try handle.write(contentsOf: entry.data)
                guard fchmod(fd, mode_t(entry.mode)) == 0 else { throw HostFailure("handoff_io", "Cannot preserve destination file mode") }
                try handle.synchronize()
            }
        }
        // Restore restrictive directory modes after writing their descendants.
        for entry in transfer.entries.filter({ $0.kind == "directory" }).sorted(by: { $0.path.count > $1.path.count }) {
            let parent = try openParent(entry.path, root: destinationFD)
            defer { close(parent.fd) }
            let fd = openat(parent.fd, parent.name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
            guard fd >= 0 else { throw HostFailure("handoff_conflict", "Destination directory changed before restoring its mode") }
            defer { close(fd) }
            guard fchmod(fd, mode_t(entry.mode)) == 0 else { throw HostFailure("handoff_io", "Cannot preserve destination directory mode") }
        }
        try transfer.index.write(to: index, options: .atomic)
        _ = try git(destination, ["update-ref", "--no-deref", "HEAD", transfer.head, expected.head], command)
        guard try tree(destination) == transfer.entries, try boundedData(index) == transfer.index else {
            throw HostFailure("handoff_conflict", "Destination does not match the transfer; recovery snapshots are retained")
        }
    }

    private func openParent(_ path: String, root: Int32) throws -> (fd: Int32, name: String) {
        let parts = try RemoteWorkspaceAccess.components(path)
        var descriptor = dup(root)
        guard descriptor >= 0 else { throw HostFailure("handoff_io", "Cannot retain destination root descriptor") }
        for part in parts.dropLast() {
            let next = openat(descriptor, part, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
            close(descriptor)
            guard next >= 0 else { throw HostFailure("handoff_conflict", "Destination parent changed or became a symbolic link") }
            descriptor = next
        }
        return (descriptor, parts.last!)
    }

    private func validateRepository(_ root: URL, command: CommandRun) throws {
        guard root.standardizedFileURL.resolvingSymlinksInPath().path == root.path,
              try WorkspaceGitCommand.root(root, command: command).path == root.path else {
            throw HostFailure("workspace_denied", "Handoff requires an exact canonical registered Git root")
        }
        if (try? git(root, ["config", "--bool", "core.sparseCheckout"], command)) == "true" {
            throw HostFailure("handoff_unsupported", "Sparse checkouts cannot be transferred without expanding their missing working files")
        }
    }
    private func validate(_ transfer: WorkspaceTransfer) throws {
        let count = transfer.bundle.count + transfer.index.count + transfer.indexObjects.values.reduce(0) { $0 + $1.count } + transfer.entries.reduce(0) { $0 + $1.data.count }
        guard count <= Self.byteLimit, transfer.entries.count <= 10000,
              [40, 64].contains(transfer.head.count), transfer.head.allSatisfy(\.isHexDigit) else {
            throw HostFailure("handoff_limit", "Workspace transfer exceeds the 16 MiB or 10,000 entry bound, or has invalid Git identity")
        }
        var paths: [String: String] = [:]
        for entry in transfer.entries {
            _ = try RemoteWorkspaceAccess.components(entry.path)
            guard paths[entry.path] == nil, ["directory", "file", "symlink"].contains(entry.kind),
                  (0...0o777).contains(entry.mode), entry.kind != "symlink" || (!entry.data.contains(0) && String(data: entry.data, encoding: .utf8) != nil) else {
                throw HostFailure("handoff_identity", "Invalid or duplicate transferred file")
            }
            paths[entry.path] = entry.kind
        }
        for entry in transfer.entries {
            var parts = entry.path.split(separator: "/"); parts.removeLast()
            while !parts.isEmpty {
                guard paths[parts.joined(separator: "/")] == "directory" else { throw HostFailure("handoff_identity", "Transferred parent is missing or is not a directory") }
                parts.removeLast()
            }
        }
    }
    private func tree(_ root: URL) throws -> [WorkspaceTransferEntry] {
        // atPath emits relative names. URL enumeration may expand /var to
        // /private/var while URL.path keeps /var, corrupting prefix slicing.
        guard let walker = FileManager.default.enumerator(atPath: root.path) else {
            throw HostFailure("handoff_io", "Cannot enumerate workspace")
        }
        var entries: [WorkspaceTransferEntry] = [], bytes = 0
        for case let path as String in walker {
            let url = root.appendingPathComponent(path)
            if path == ".git" { walker.skipDescendants(); continue }
            _ = try RemoteWorkspaceAccess.components(path)
            let values = try url.resourceValues(forKeys: [.isSymbolicLinkKey, .isDirectoryKey])
            let attributes = try FileManager.default.attributesOfItem(atPath: url.path)
            let kind: String, data: Data
            if values.isSymbolicLink == true {
                walker.skipDescendants(); kind = "symlink"
                data = Data(try FileManager.default.destinationOfSymbolicLink(atPath: url.path).utf8)
            } else if values.isDirectory == true { kind = "directory"; data = Data() }
            else {
                guard attributes[.type] as? FileAttributeType == .typeRegular,
                      (attributes[.referenceCount] as? NSNumber)?.intValue == 1 else {
                    throw HostFailure("handoff_unsupported", "Special files and hardlinks require manual workspace transfer")
                }
                kind = "file"; data = try RemoteWorkspaceAccess.captureFile(root: root, path: path, limit: Self.byteLimit)
            }
            bytes += data.count
            guard bytes <= Self.byteLimit, entries.count < 10000 else { throw HostFailure("handoff_limit", "Working files exceed the transfer bound; no files were omitted") }
            entries.append(.init(path: path, kind: kind, mode: ((attributes[.posixPermissions] as? NSNumber)?.intValue ?? 0o644) & 0o777, data: data))
        }
        return entries.sorted { $0.path < $1.path }
    }
    private func indexPath(_ root: URL, _ command: CommandRun) throws -> URL {
        URL(fileURLWithPath: try git(root, ["rev-parse", "--path-format=absolute", "--git-path", "index"], command))
    }
    private func boundedData(_ url: URL) throws -> Data {
        let handle = try FileHandle(forReadingFrom: url); defer { try? handle.close() }
        let data = try handle.read(upToCount: Self.byteLimit + 1) ?? Data()
        guard data.count <= Self.byteLimit else { throw HostFailure("handoff_limit", "A file exceeds the transfer bound") }
        return data
    }
    private func makeDirectory() throws -> URL {
        let result = directory.appendingPathComponent("capture-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: result, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        return result
    }
    private func persist<T: Encodable>(_ value: T, at url: URL) throws {
        try JSONEncoder().encode(value).write(to: url, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
        let handle = try FileHandle(forWritingTo: url); try handle.synchronize(); try handle.close()
    }
    private func git(_ root: URL, _ args: [String], _ command: CommandRun) throws -> String {
        try WorkspaceGitCommand.text(root, args, command: command, timeout: 30, maximumOutputBytes: Self.byteLimit)
    }
}
