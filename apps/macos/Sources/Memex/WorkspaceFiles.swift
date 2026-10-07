import CryptoKit
import Darwin
import Foundation

struct WorkspaceFileEntry: Identifiable, Sendable, Equatable {
    let path: String
    let isDirectory: Bool
    let isSymbolicLink: Bool
    var id: String { path }
    var name: String { URL(fileURLWithPath: path).lastPathComponent }
}

struct WorkspaceFileRevision: Sendable, Equatable {
    let contents: String
    let fingerprint: String
}

enum WorkspaceFileError: LocalizedError {
    case outsideWorkspace, unsupported, tooLarge, conflict
    var errorDescription: String? {
        switch self {
        case .outsideWorkspace: "Choose a regular file inside this workspace. Symbolic links cannot be edited."
        case .unsupported: "This file is not UTF-8 text. Open it in its default application."
        case .tooLarge: "This file exceeds the 5 MB editor limit. Open it in its default application."
        case .conflict: "The file changed on disk. Your edits have been kept. Compare with the disk version before saving again."
        }
    }
}

/// Serializes this app's edits; NSFileCoordinator additionally cooperates with
/// document-based editors. Non-cooperating tools are checked against the exact
/// bytes read, immediately before an atomic replacement. No automatic overwrite.
actor WorkspaceFilesClient {
    static let shared = WorkspaceFilesClient()
    static let maximumBytes = 5_000_000

    func entries(root: URL, path: String = "") throws -> [WorkspaceFileEntry] {
        let directory = try resolve(root: root, path: path, allowDirectory: true)
        return try FileManager.default.contentsOfDirectory(at: directory,
            includingPropertiesForKeys: [.isDirectoryKey, .isSymbolicLinkKey])
            .filter { $0.lastPathComponent != ".git" }
            .map { url in
                let values = try url.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey])
                return WorkspaceFileEntry(path: path.isEmpty ? url.lastPathComponent : path + "/" + url.lastPathComponent,
                    isDirectory: values.isDirectory == true, isSymbolicLink: values.isSymbolicLink == true)
            }
            .sorted { lhs, rhs in
                if lhs.isDirectory != rhs.isDirectory { return lhs.isDirectory }
                return lhs.name.localizedStandardCompare(rhs.name) == .orderedAscending
            }
    }

    func read(root: URL, path: String) throws -> WorkspaceFileRevision {
        let url = try resolve(root: root, path: path)
        return try revision(at: url)
    }

    func save(root: URL, path: String, contents: String, expectedFingerprint: String) throws -> WorkspaceFileRevision {
        let url = try resolve(root: root, path: path)
        let bytes = Data(contents.utf8)
        guard bytes.count <= Self.maximumBytes else { throw WorkspaceFileError.tooLarge }
        var coordinationError: NSError?
        var result: Result<WorkspaceFileRevision, any Error>?
        NSFileCoordinator().coordinate(writingItemAt: url, options: .forReplacing, error: &coordinationError) { coordinatedURL in
            result = Result {
                // Resolve again inside the coordination block: a tree change
                // while waiting cannot redirect this save through a symlink.
                guard try resolve(root: root, path: path) == coordinatedURL else { throw WorkspaceFileError.outsideWorkspace }
                let current = try revision(at: coordinatedURL)
                guard current.fingerprint == expectedFingerprint else { throw WorkspaceFileError.conflict }
                let permissions = try FileManager.default.attributesOfItem(atPath: coordinatedURL.path)[.posixPermissions]
                try bytes.write(to: coordinatedURL, options: .atomic)
                if let permissions { try FileManager.default.setAttributes([.posixPermissions: permissions], ofItemAtPath: coordinatedURL.path) }
                return WorkspaceFileRevision(contents: contents, fingerprint: Self.fingerprint(bytes))
            }
        }
        if let coordinationError { throw coordinationError }
        guard let result else { throw WorkspaceFileError.conflict }
        return try result.get()
    }

    func fileURL(root: URL, path: String) throws -> URL { try resolve(root: root, path: path) }

    /// Publish a fully written initial document without replacing an existing
    /// file. The hard link is atomic and exclusive across actors and processes;
    /// readers never observe a placeholder or partially written document.
    func openOrCreate(root: URL, name: String, contents: String) throws -> WorkspaceFileRevision {
        guard !name.isEmpty, name != ".", name != "..", name != ".git", !name.contains("/"), !name.contains("\0") else {
            throw WorkspaceFileError.outsideWorkspace
        }
        let parent = try resolve(root: root, path: "", allowDirectory: true)
        do { return try read(root: parent, path: name) }
        catch let error as CocoaError where error.code == .fileReadNoSuchFile || error.code == .fileNoSuchFile {}
        let bytes = Data(contents.utf8)
        guard bytes.count <= Self.maximumBytes else { throw WorkspaceFileError.tooLarge }
        guard !bytes.contains(0) else { throw WorkspaceFileError.unsupported }
        let directory = Darwin.open(parent.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW)
        guard directory >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        defer { Darwin.close(directory) }
        let temporary = ".plan-initial-" + UUID().uuidString
        let descriptor = openat(directory, temporary, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600)
        guard descriptor >= 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        defer {
            Darwin.close(descriptor)
            _ = unlinkat(directory, temporary, 0)
        }
        try bytes.withUnsafeBytes { buffer in
            var offset = 0
            while offset < buffer.count {
                let count = Darwin.write(descriptor, buffer.baseAddress!.advanced(by: offset), buffer.count - offset)
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
                offset += count
            }
        }
        guard fsync(descriptor) == 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        if linkat(directory, temporary, directory, name, 0) != 0 {
            // Only a competing successful initialization or an existing user
            // file may win. Disk/permission failures must not become success.
            guard errno == EEXIST else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        }
        return try read(root: parent, path: name)
    }

    func create(root: URL, folder: String, name: String, isDirectory: Bool) throws -> String {
        guard !name.isEmpty, name != ".", name != "..", name != ".git", !name.contains("/"), !name.contains("\0") else {
            throw WorkspaceFileError.outsideWorkspace
        }
        let parent = try resolve(root: root, path: folder, allowDirectory: true)
        guard try parent.resourceValues(forKeys: [.isDirectoryKey]).isDirectory == true else { throw WorkspaceFileError.outsideWorkspace }
        let url = parent.appendingPathComponent(name)
        if isDirectory {
            guard url.path.withCString({ mkdir($0, 0o700) }) == 0 else {
                throw WorkspaceGitError(message: "Could not create folder: \(String(cString: strerror(errno)))")
            }
        } else {
            let descriptor = url.path.withCString { Darwin.open($0, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, 0o600) }
            guard descriptor >= 0 else { throw WorkspaceGitError(message: "Could not create file: \(String(cString: strerror(errno)))") }
            Darwin.close(descriptor)
        }
        return folder.isEmpty ? name : folder + "/" + name
    }

    private func revision(at url: URL) throws -> WorkspaceFileRevision {
        let values = try url.resourceValues(forKeys: [.fileSizeKey])
        guard (values.fileSize ?? Int.max) <= Self.maximumBytes else { throw WorkspaceFileError.tooLarge }
        let data = try Data(contentsOf: url)
        guard data.count <= Self.maximumBytes else { throw WorkspaceFileError.tooLarge }
        guard !data.contains(0), let text = String(data: data, encoding: .utf8) else { throw WorkspaceFileError.unsupported }
        return WorkspaceFileRevision(contents: text, fingerprint: Self.fingerprint(data))
    }

    private static func fingerprint(_ bytes: Data) -> String {
        SHA256.hash(data: bytes).map { String(format: "%02x", $0) }.joined()
    }

    private func resolve(root: URL, path: String, allowDirectory: Bool = false) throws -> URL {
        let root = try LocalProjects.validatedDirectory(root)
        guard !path.hasPrefix("/"), !path.contains("\0") else { throw WorkspaceFileError.outsideWorkspace }
        let parts = path.split(separator: "/", omittingEmptySubsequences: false)
        guard !parts.contains(".."), !parts.contains(".git") else { throw WorkspaceFileError.outsideWorkspace }
        var url = root
        for part in parts where !part.isEmpty {
            url.appendPathComponent(String(part))
            let values = try url.resourceValues(forKeys: [.isSymbolicLinkKey])
            guard values.isSymbolicLink != true else { throw WorkspaceFileError.outsideWorkspace }
        }
        let canonical = url.standardizedFileURL.resolvingSymlinksInPath()
        guard canonical.path == root.path || canonical.path.hasPrefix(root.path + "/") else { throw WorkspaceFileError.outsideWorkspace }
        let values = try canonical.resourceValues(forKeys: [.isRegularFileKey, .isDirectoryKey])
        guard values.isRegularFile == true || (allowDirectory && values.isDirectory == true) else { throw WorkspaceFileError.outsideWorkspace }
        return canonical
    }
}
