import Foundation
import CryptoKit
import Darwin

/// Called only after ExecutionHost has verified host identity and resolved the
/// currently granted workspace. Relative paths never become viewer-local URLs.
public final class RemoteWorkspaceAccess {
    public static let readMethods: Set<String> = ["workspace.files", "workspace.file.read", "workspace.diff", "workspace.terminal.read"]
    public static let mutationMethods: Set<String> = ["workspace.file.write", "workspace.terminal.open", "workspace.terminal.input", "workspace.terminal.resize", "workspace.terminal.close"]
    public static let capabilities = ["workspace.files", "workspace.diff", "workspace.terminal"]
    private let terminals = RemoteWorkspaceTerminals()
    private let beforeFileReplace: (() throws -> Void)?
    public init(beforeFileReplace: (() throws -> Void)? = nil) { self.beforeFileReplace = beforeFileReplace }
    public func hasRunningTerminal(root: URL) -> Bool { terminals.hasRunningTerminal(root: root) }

    public func handle(_ request: HostRequest, root: URL) throws -> HostValue {
        let p = request.params
        if request.method.hasPrefix("workspace.terminal.") {
            return try terminals.handle(request, root: root)
        }
        if request.method == "workspace.diff" {
            let command = CommandRun()
            guard try WorkspaceGitCommand.root(root, command: command).path == root.path else {
                throw HostFailure("workspace_denied", "Review requires a granted repository root")
            }
            let staged = p["staged"]?.bool ?? false
            let arguments = ["diff", "--no-ext-diff", "--no-textconv", "--no-color"] + (staged ? ["--cached"] : []) + ["--"]
            let data = try WorkspaceGitCommand.data(root, arguments, command: command, timeout: 15, maximumOutputBytes: 2 * 1024 * 1024)
            let limit = 2 * 1024 * 1024
            return .object(["text": .string(String(decoding: data.prefix(limit), as: UTF8.self)),
                "truncated": .bool(data.count > limit), "staged": .bool(staged)])
        }
        let path = p["path"]?.string ?? ""
        if request.method == "workspace.files" { return try list(root: root, path: path) }
        let descriptor = try openFile(root: root, path: path, writing: request.method == "workspace.file.write")
        defer { close(descriptor) }
        guard flock(descriptor, LOCK_EX) == 0 else { throw failure("Cannot lock workspace file") }
        defer { flock(descriptor, LOCK_UN) }
        let original = try read(descriptor)
        if request.method == "workspace.file.read" { return try contents(original) }
        guard request.method == "workspace.file.write", let expected = p["revision"]?.string,
              let text = p["text"]?.string, text.utf8.count <= Self.fileLimit else {
            throw HostFailure("invalid_params", "A bounded text value and original revision are required")
        }
        guard Self.revision(original) == expected else {
            throw HostFailure("file_conflict", "This file changed on the host. Reload it before saving; your draft is retained.")
        }
        let bytes = Data(text.utf8)
        try replace(root: root, path: path, originalDescriptor: descriptor, original: original, bytes: bytes)
        return try contents(bytes)
    }

    private func replace(root: URL, path: String, originalDescriptor: Int32, original: Data, bytes: Data) throws {
        let parts = try Self.components(path)
        let parent = try directory(root: root, parts: Array(parts.dropLast()))
        defer { close(parent) }
        let temporary = ".memex-save-" + UUID().uuidString
        let fd = openat(parent, temporary, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard fd >= 0 else { throw failure("Cannot prepare an atomic workspace save") }
        defer { close(fd); unlinkat(parent, temporary, 0) }
        var position = 0
        try bytes.withUnsafeBytes { buffer in
            while position < bytes.count {
                let count = pwrite(fd, buffer.baseAddress!.advanced(by: position), bytes.count - position, off_t(position))
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else { throw failure("Cannot write replacement workspace file") }
                position += count
            }
        }
        var originalInfo = stat(), currentInfo = stat()
        guard fstat(originalDescriptor, &originalInfo) == 0, fchmod(fd, originalInfo.st_mode & 0o777) == 0,
              fsync(fd) == 0 else { throw failure("Cannot synchronize replacement workspace file") }
        try beforeFileReplace?()
        // Optimistic external-writer check, matching the local editor contract.
        // Rename is atomic; an uncooperative writer can still race the final check.
        guard fstatat(parent, parts.last!, &currentInfo, AT_SYMLINK_NOFOLLOW) == 0,
              currentInfo.st_dev == originalInfo.st_dev, currentInfo.st_ino == originalInfo.st_ino,
              try read(originalDescriptor) == original else {
            throw HostFailure("file_conflict", "This file changed on the host while saving. Your draft is retained.")
        }
        guard renameat(parent, temporary, parent, parts.last!) == 0 else { throw failure("Cannot atomically replace workspace file") }
        guard fsync(parent) == 0 else { throw HostFailure("file_durability", "The complete replacement is visible, but directory synchronization failed. Reload its revision before retrying.") }
    }

    private static let fileLimit = 2 * 1024 * 1024
    static func components(_ path: String, allowRoot: Bool = false) throws -> [String] {
        if path.isEmpty && allowRoot { return [] }
        let parts = path.split(separator: "/", omittingEmptySubsequences: false).map(String.init)
        guard !path.isEmpty, !path.hasPrefix("/"), !path.contains("\0"),
              parts.allSatisfy({ !$0.isEmpty && $0 != "." && $0 != ".." && $0.lowercased() != ".git" }) else {
            throw HostFailure("workspace_denied", "Choose a relative workspace path outside Git metadata: \(path.debugDescription)")
        }
        return parts
    }
    private func directory(root: URL, parts: [String]) throws -> Int32 {
        var fd = open(root.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard fd >= 0 else { throw failure("Workspace root is unavailable") }
        for part in parts {
            let next = openat(fd, part, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
            close(fd)
            guard next >= 0 else { throw HostFailure("workspace_denied", "Directory is unavailable or is a symbolic link") }
            fd = next
        }
        return fd
    }
    private func openFile(root: URL, path: String, writing: Bool) throws -> Int32 {
        let parts = try Self.components(path)
        let parent = try directory(root: root, parts: Array(parts.dropLast()))
        defer { close(parent) }
        let fd = openat(parent, parts.last!, (writing ? O_RDWR : O_RDONLY) | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)
        guard fd >= 0 else { throw HostFailure("workspace_denied", "File is unavailable or is a symbolic link") }
        var info = stat()
        guard fstat(fd, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG,
              info.st_nlink == 1, info.st_size <= Self.fileLimit else {
            close(fd)
            throw HostFailure("file_limit", "Only regular, singly linked files up to 2 MiB can be edited")
        }
        return fd
    }
    private func list(root: URL, path: String) throws -> HostValue {
        let fd = try directory(root: root, parts: Self.components(path, allowRoot: true))
        guard let stream = fdopendir(fd) else { close(fd); throw failure("Cannot list directory") }
        defer { closedir(stream) }
        var entries: [HostValue] = []
        while let entry = readdir(stream) {
            let name = withUnsafePointer(to: &entry.pointee.d_name) {
                $0.withMemoryRebound(to: CChar.self, capacity: Int(NAME_MAX) + 1) { String(cString: $0) }
            }
            guard name != ".", name != "..", name.lowercased() != ".git" else { continue }
            var info = stat()
            guard fstatat(fd, name, &info, AT_SYMLINK_NOFOLLOW) == 0 else { continue }
            let type = info.st_mode & S_IFMT
            guard type == S_IFDIR || (type == S_IFREG && info.st_nlink == 1) else { continue }
            guard entries.count < 2000 else { throw HostFailure("file_limit", "This directory exceeds the 2,000 entry listing limit") }
            entries.append(.object(["name": .string(name), "path": .string(path.isEmpty ? name : path + "/" + name),
                "directory": .bool(type == S_IFDIR), "size": .number(Double(info.st_size))]))
        }
        return .array(entries.sorted { ($0["name"].string ?? "") < ($1["name"].string ?? "") })
    }
    private func read(_ fd: Int32) throws -> Data {
        try Self.readDescriptor(fd, limit: Self.fileLimit)
    }
    static func captureFile(root: URL, path: String, limit: Int) throws -> Data {
        let parts = try components(path)
        let parent = try RemoteWorkspaceAccess().directory(root: root, parts: Array(parts.dropLast()))
        defer { close(parent) }
        let fd = openat(parent, parts.last!, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC)
        guard fd >= 0 else { throw HostFailure("workspace_denied", "Capture target changed or became a symbolic link") }
        defer { close(fd) }
        var info = stat()
        guard fstat(fd, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG, info.st_nlink == 1, info.st_size <= limit else {
            throw HostFailure("handoff_limit", "Capture requires a regular, singly linked file within the transfer bound")
        }
        return try readDescriptor(fd, limit: limit)
    }
    private static func readDescriptor(_ fd: Int32, limit: Int) throws -> Data {
        var result = Data(), buffer = [UInt8](repeating: 0, count: 65536)
        while true {
            let count = pread(fd, &buffer, min(buffer.count, limit + 1 - result.count), off_t(result.count))
            if count < 0 && errno == EINTR { continue }
            guard count >= 0 else { throw HostFailure("workspace_io", "Cannot read workspace file") }
            if count == 0 { return result }
            result.append(contentsOf: buffer.prefix(count))
            guard result.count <= limit else { throw HostFailure("file_limit", "File exceeds the read limit") }
        }
    }
    private func contents(_ data: Data) throws -> HostValue {
        guard !data.contains(0), let text = String(data: data, encoding: .utf8) else {
            throw HostFailure("file_encoding", "This panel edits UTF-8 text files only")
        }
        return .object(["text": .string(text), "revision": .string(Self.revision(data))])
    }
    private static func revision(_ data: Data) -> String { SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined() }
    private func failure(_ message: String) -> HostFailure { HostFailure("workspace_io", message + ": " + String(cString: strerror(errno))) }
}
