import CryptoKit
import Darwin
import Foundation

/// Checkpoints preserve disk bytes, bypassing Git clean/smudge filters and EOL
/// conversion. The index tree is recorded separately by the checkpoint owner.
enum WorkspaceRawTree {
    struct Entry: Equatable {
        let mode: String
        let object: String
    }

    /// lstat identity of a captured path. Any completed write moves ctime, so
    /// a path whose identity matches the previous capture still holds the bytes
    /// hashed then, unless its times are not strictly before that capture began.
    struct FileIdentity: Equatable {
        let device: Int64
        let inode: UInt64
        let size: Int64
        let modified: timespec
        let changed: timespec

        init(_ info: stat) {
            device = Int64(info.st_dev)
            inode = UInt64(info.st_ino)
            size = Int64(info.st_size)
            modified = info.st_mtimespec
            changed = info.st_ctimespec
        }

        init(device: Int64, inode: UInt64, size: Int64, modified: timespec, changed: timespec) {
            self.device = device
            self.inode = inode
            self.size = size
            self.modified = modified
            self.changed = changed
        }

        static func == (lhs: Self, rhs: Self) -> Bool {
            lhs.device == rhs.device && lhs.inode == rhs.inode && lhs.size == rhs.size
                && lhs.modified.tv_sec == rhs.modified.tv_sec && lhs.modified.tv_nsec == rhs.modified.tv_nsec
                && lhs.changed.tv_sec == rhs.changed.tv_sec && lhs.changed.tv_nsec == rhs.changed.tv_nsec
        }

        func isBefore(_ instant: timespec) -> Bool {
            func before(_ time: timespec) -> Bool {
                time.tv_sec < instant.tv_sec || (time.tv_sec == instant.tv_sec && time.tv_nsec < instant.tv_nsec)
            }
            return before(modified) && before(changed)
        }
    }

    struct CachedFile {
        let mode: String
        let object: String
        let identity: FileIdentity
    }

    static func entries(root: URL, tree: String, command: CommandRun) throws -> [String: Entry] {
        let data = try WorkspaceGitCommand.data(root, ["ls-tree", "-r", "-z", tree], command: command, timeout: 120)
        var result: [String: Entry] = [:]
        for record in data.split(separator: 0) {
            guard let tab = record.firstIndex(of: 9) else { throw invalidPath() }
            let header = String(decoding: record[..<tab], as: UTF8.self).split(separator: " ")
            guard header.count == 3,
                  let path = String(data: Data(record[record.index(after: tab)...]), encoding: .utf8) else { throw invalidPath() }
            _ = try location(root: root, path: path)
            result[path] = Entry(mode: String(header[0]), object: String(header[2]))
        }
        return result
    }

    /// Builds the raw tree in the temporary index named by `environment`.
    /// `cache` remembers each path's lstat identity and blob from the previous
    /// capture of this checkout, so only new or changed files are read and
    /// hashed again. Returns untracked nested repositories, which are skipped.
    static func capture(root: URL, temporary: URL, environment: [String: String], cache: URL?,
                        command: CommandRun) throws -> [String] {
        let indexedTree = try WorkspaceGitCommand.text(root, ["write-tree"], command: command, environment: environment, timeout: 120)
        // Only gitlinks are needed from the index tree; skip parsing the rest.
        var gitlinks: [String: Entry] = [:]
        let listing = try WorkspaceGitCommand.data(root, ["ls-tree", "-r", "-z", indexedTree], command: command, timeout: 120)
        for record in listing.split(separator: 0) where record.starts(with: Data("160000 ".utf8)) {
            guard let tab = record.firstIndex(of: 9),
                  let path = String(data: Data(record[record.index(after: tab)...]), encoding: .utf8) else { throw invalidPath() }
            let header = String(decoding: record[..<tab], as: UTF8.self).split(separator: " ")
            guard header.count == 3 else { throw invalidPath() }
            gitlinks[path] = Entry(mode: String(header[0]), object: String(header[2]))
        }
        let data = try WorkspaceGitCommand.data(root, ["ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            command: command, timeout: 120)
        let paths = try Set(data.split(separator: 0).map { bytes -> String in
            guard let path = String(data: Data(bytes), encoding: .utf8) else { throw invalidPath() }
            return path
        })
        let format = try objectFormat(root: root, command: command)
        let previous = cache.flatMap { load(cache: $0, format: format) }
        // Taken before any lstat below. A path changed at or after this instant
        // may share a timestamp with a later write, so the next capture rehashes it.
        let started = now()
        var skipped: [String] = []
        var records = Data()
        var next: [String: CachedFile] = [:]
        var reused: [(path: String, file: CachedFile)] = []
        var batch: [(path: String, source: URL, mode: String, identity: FileIdentity)] = []
        var batchSize = 0
        func appendRecord(mode: String, object: String, path: String) {
            records.append(Data("\(mode) \(object)\t\(path)\0".utf8))
        }
        func flush() throws {
            guard !batch.isEmpty else { return }
            let result = try WorkspaceGitCommand.text(root,
                ["hash-object", "-w", "--no-filters", "--"] + batch.map { $0.source.path }, command: command,
                timeout: 120 + Double(batch.reduce(Int64(0)) { $0 + $1.identity.size }) / 8_000_000)
            let objects = result.split(separator: "\n")
            guard objects.count == batch.count else { throw invalidPath() }
            for (item, object) in zip(batch, objects) {
                appendRecord(mode: item.mode, object: String(object), path: item.path)
                next[item.path] = CachedFile(mode: item.mode, object: String(object), identity: item.identity)
            }
            batch.removeAll(keepingCapacity: true)
            batchSize = 0
        }
        func enqueue(path: String, url: URL, mode: String, identity: FileIdentity) throws {
            let source: URL
            if mode == "120000" {
                source = temporary.appendingPathComponent("symlink-" + UUID().uuidString)
                try Data(FileManager.default.destinationOfSymbolicLink(atPath: url.path).utf8).write(to: source, options: .atomic)
            } else { source = url }
            if batchSize + source.path.utf8.count > 64 * 1024 { try flush() }
            batch.append((path, source, mode, identity))
            batchSize += source.path.utf8.count + 1
        }
        for path in paths.sorted() {
            // `ls-files --others` names an untracked nested repository as
            // `dir/`. Its files belong to that repository; never flatten it.
            if path.hasSuffix("/") {
                skipped.append(String(path.dropLast()))
                continue
            }
            let url = try location(root: root, path: path)
            // A gitlink belongs to the nested repository and is never flattened.
            if let entry = gitlinks[path] {
                appendRecord(mode: entry.mode, object: entry.object, path: path)
                continue
            }
            var info = stat()
            guard lstat(url.path, &info) == 0 else {
                if errno == ENOENT || errno == ENOTDIR { continue }
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
            let mode: String
            switch info.st_mode & S_IFMT {
            case S_IFREG: mode = info.st_mode & 0o111 == 0 ? "100644" : "100755"
            case S_IFLNK: mode = "120000"
            default:
                throw WorkspaceGitError(message: "Checkpoint cannot snapshot the directory or special file at \(path). Preserve nested repositories separately.")
            }
            let identity = FileIdentity(info)
            if let previous, let cached = previous.files[path], cached.mode == mode, cached.identity == identity,
               identity.isBefore(previous.started) {
                reused.append((path, cached))
            } else {
                try enqueue(path: path, url: url, mode: mode, identity: identity)
            }
        }
        try flush()
        // Reused blobs are pinned only by retained checkpoint refs; a pruned
        // checkpoint's objects may be gone. Rehash any that are missing.
        if !reused.isEmpty {
            let request = temporary.appendingPathComponent("reused-objects")
            try Data(reused.map { $0.file.object + "\n" }.joined().utf8).write(to: request)
            let reply = try WorkspaceGitCommand.data(root, ["cat-file", "--batch-check=%(objectname) %(objecttype)"], command: command,
                timeout: 30 + Double(reused.count) / 2_000, inputFile: request)
            let lines = String(decoding: reply, as: UTF8.self).split(separator: "\n")
            guard lines.count == reused.count else { throw invalidPath() }
            for (item, line) in zip(reused, lines) {
                if line == item.file.object + " blob" {
                    appendRecord(mode: item.file.mode, object: item.file.object, path: item.path)
                    next[item.path] = item.file
                } else {
                    try enqueue(path: item.path, url: try location(root: root, path: item.path), mode: item.file.mode, identity: item.file.identity)
                }
            }
            try flush()
        }
        let input = temporary.appendingPathComponent("index-info")
        try records.write(to: input)
        _ = try WorkspaceGitCommand.text(root, ["read-tree", "--empty"], command: command, environment: environment)
        _ = try WorkspaceGitCommand.data(root, ["update-index", "-z", "--index-info"], command: command,
            environment: environment, timeout: 120, inputFile: input)
        if let cache { save(cache: cache, format: format, started: started, files: next) }
        return skipped
    }

    /// Restores `target` over `current` in three phases: verify every changed
    /// path against `current` (one streamed read each), stage every target blob
    /// outside the tree, then replace paths with atomic renames. A failure
    /// before the last phase leaves the tree untouched.
    static func restore(root: URL, target: [String: Entry], current: [String: Entry], command: CommandRun) throws {
        let changed = Set(target.keys).union(current.keys).filter { target[$0] != current[$0] }.sorted()
        guard !changed.isEmpty else { return }
        let format = try objectFormat(root: root, command: command)
        // Reject structural conflicts before touching files. In particular never
        // traverse a symlink, remove a directory, or mutate a nested repository.
        var verified: [String: FileIdentity?] = [:]
        for path in changed {
            let url = try location(root: root, path: path)
            if target[path]?.mode == "160000" || current[path]?.mode == "160000" {
                throw WorkspaceGitError(message: "A nested repository changed at \(path). Restore it separately.")
            }
            let type = try attributes(url)?[.type] as? FileAttributeType
            guard type == nil || type == .typeRegular || type == .typeSymbolicLink else {
                throw WorkspaceGitError(message: "A directory or special file blocks restore at \(path). Files were preserved.")
            }
            verified.updateValue(try verify(url: url, path: path, expected: current[path], format: format), forKey: path)
        }
        let manager = FileManager.default
        // Same volume as the checkout, so the final rename is atomic.
        let staging = try manager.url(for: .itemReplacementDirectory, in: .userDomainMask, appropriateFor: root, create: true)
        defer { try? manager.removeItem(at: staging) }
        let wanted = changed.compactMap { path in target[path].map { (path, $0) } }
        var sizes: [String: Int64] = [:]
        if !wanted.isEmpty {
            let request = staging.appendingPathComponent("objects")
            try Data(wanted.map { $0.1.object + "\n" }.joined().utf8).write(to: request)
            let reply = try WorkspaceGitCommand.data(root, ["cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"],
                command: command, timeout: 30 + Double(wanted.count) / 2_000, inputFile: request)
            for line in String(decoding: reply, as: UTF8.self).split(separator: "\n") {
                let fields = line.split(separator: " ")
                guard fields.count == 3, fields[1] == "blob", let size = Int64(fields[2]) else {
                    throw WorkspaceGitError(message: "Checkpoint object \(fields.first ?? "") is missing. No files were restored.")
                }
                sizes[String(fields[0])] = size
            }
        }
        var staged: [String: URL] = [:]
        for (number, (path, entry)) in wanted.enumerated() {
            let blob = staging.appendingPathComponent("blob-\(number)")
            let size = sizes[entry.object] ?? 0
            // Blobs stream to disk; the timeout grows with the object size.
            _ = try WorkspaceGitCommand.data(root, ["cat-file", "blob", entry.object], command: command,
                timeout: 30 + Double(size) / 8_000_000, outputFile: blob)
            switch entry.mode {
            case "100644", "100755":
                try manager.setAttributes([.posixPermissions: entry.mode == "100755" ? 0o755 : 0o644], ofItemAtPath: blob.path)
                staged[path] = blob
            case "120000":
                guard size <= 65_536, let destination = String(data: try Data(contentsOf: blob), encoding: .utf8) else { throw invalidPath() }
                let link = staging.appendingPathComponent("link-\(number)")
                try manager.createSymbolicLink(atPath: link.path, withDestinationPath: destination)
                staged[path] = link
            default: throw WorkspaceGitError(message: "Unsupported checkpoint file mode \(entry.mode).")
            }
        }
        for path in changed {
            let url = try location(root: root, path: path)
            // A path untouched since verification needs no second read.
            if identity(url) != verified[path] ?? nil {
                _ = try verify(url: url, path: path, expected: current[path], format: format)
            }
            guard let source = staged[path] else {
                if try attributes(url) != nil { try manager.removeItem(at: url) }
                continue
            }
            try manager.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            _ = try location(root: root, path: path)
            // rename(2) replaces a final file or symlink itself, never its target.
            guard Darwin.rename(source.path, url.path) == 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        }
    }

    /// Confirms the path still holds `expected` by hashing its bytes as a Git
    /// blob, and returns the identity observed for the later cheap recheck.
    private static func verify(url: URL, path: String, expected: Entry?, format: String) throws -> FileIdentity? {
        let observed = identity(url)
        guard let expected else {
            guard try attributes(url) == nil else { throw changed(path) }
            return nil
        }
        guard let observed, let attributes = try attributes(url) else { throw changed(path) }
        let object: String?
        if expected.mode == "120000" {
            guard attributes[.type] as? FileAttributeType == .typeSymbolicLink else { throw changed(path) }
            object = blobID(Data(try FileManager.default.destinationOfSymbolicLink(atPath: url.path).utf8), format: format)
        } else {
            guard attributes[.type] as? FileAttributeType == .typeRegular else { throw changed(path) }
            object = try blobID(file: url, format: format)
        }
        guard object == expected.object, identity(url) == observed else { throw changed(path) }
        return observed
    }

    static func blobID(_ bytes: Data, format: String) -> String? {
        var hasher = BlobHasher(format: format)
        hasher?.update(Data("blob \(bytes.count)\0".utf8))
        hasher?.update(bytes)
        return hasher?.finalize()
    }

    static func blobID(file url: URL, format: String) throws -> String? {
        let handle = try FileHandle(forReadingFrom: url)
        defer { try? handle.close() }
        var info = stat()
        guard fstat(handle.fileDescriptor, &info) == 0 else { throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        guard var hasher = BlobHasher(format: format) else { return nil }
        hasher.update(Data("blob \(info.st_size)\0".utf8))
        var total: Int64 = 0
        while let chunk = try handle.read(upToCount: 1 << 20), !chunk.isEmpty {
            hasher.update(chunk)
            total += Int64(chunk.count)
        }
        // A concurrent writer changed the length while it was read.
        return total == Int64(info.st_size) ? hasher.finalize() : nil
    }

    private struct BlobHasher {
        private var sha1: Insecure.SHA1?
        private var sha256: SHA256?
        init?(format: String) {
            switch format {
            case "sha1": sha1 = Insecure.SHA1()
            case "sha256": sha256 = SHA256()
            default: return nil
            }
        }
        mutating func update(_ data: Data) { sha1?.update(data: data); sha256?.update(data: data) }
        func finalize() -> String {
            let digest: [UInt8] = sha1.map { Array($0.finalize()) } ?? sha256.map { Array($0.finalize()) } ?? []
            return digest.map { String(format: "%02x", $0) }.joined()
        }
    }

    private static func objectFormat(root: URL, command: CommandRun) throws -> String {
        let format = try WorkspaceGitCommand.text(root, ["rev-parse", "--show-object-format"], command: command)
        guard format == "sha1" || format == "sha256" else {
            throw WorkspaceGitError(message: "Checkpoints do not support the \(format) object format.")
        }
        return format
    }

    private static func identity(_ url: URL) -> FileIdentity? {
        var info = stat()
        return lstat(url.path, &info) == 0 ? FileIdentity(info) : nil
    }

    private static func now() -> timespec {
        var time = timespec()
        clock_gettime(CLOCK_REALTIME, &time)
        return time
    }

    // Cache format: a header line, then one NUL-terminated record per path:
    // `mode object device inode size mtime.s mtime.ns ctime.s ctime.ns\tpath`.
    private static let cacheHeader = "memex-raw-tree-cache 1"

    private static func load(cache: URL, format: String) -> (started: timespec, files: [String: CachedFile])? {
        guard let data = try? Data(contentsOf: cache), let newline = data.firstIndex(of: 10) else { return nil }
        let header = String(decoding: data[..<newline], as: UTF8.self).split(separator: " ")
        guard header.count == 5, header[0...1].joined(separator: " ") == cacheHeader, header[2] == format,
              let seconds = Int(header[3]), let nanoseconds = Int(header[4]) else { return nil }
        var files: [String: CachedFile] = [:]
        for record in data[data.index(after: newline)...].split(separator: 0) {
            guard let tab = record.firstIndex(of: 9),
                  let path = String(data: Data(record[record.index(after: tab)...]), encoding: .utf8) else { return nil }
            let fields = String(decoding: record[..<tab], as: UTF8.self).split(separator: " ").map(String.init)
            guard fields.count == 9, let device = Int64(fields[2]), let inode = UInt64(fields[3]), let size = Int64(fields[4]),
                  let ms = Int(fields[5]), let mn = Int(fields[6]), let cs = Int(fields[7]), let cn = Int(fields[8]) else { return nil }
            files[path] = CachedFile(mode: fields[0], object: fields[1], identity: FileIdentity(device: device, inode: inode, size: size,
                modified: timespec(tv_sec: ms, tv_nsec: mn), changed: timespec(tv_sec: cs, tv_nsec: cn)))
        }
        return (timespec(tv_sec: seconds, tv_nsec: nanoseconds), files)
    }

    private static func save(cache: URL, format: String, started: timespec, files: [String: CachedFile]) {
        var data = Data("\(cacheHeader) \(format) \(started.tv_sec) \(started.tv_nsec)\n".utf8)
        for (path, file) in files {
            let identity = file.identity
            data.append(Data(("\(file.mode) \(file.object) \(identity.device) \(identity.inode) \(identity.size) "
                + "\(identity.modified.tv_sec) \(identity.modified.tv_nsec) \(identity.changed.tv_sec) \(identity.changed.tv_nsec)\t\(path)\0").utf8))
        }
        // The cache only accelerates capture; failing to write it is harmless.
        try? FileManager.default.createDirectory(at: cache.deletingLastPathComponent(), withIntermediateDirectories: true,
                                                 attributes: [.posixPermissions: 0o700])
        try? data.write(to: cache, options: .atomic)
        try? FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: cache.path)
    }

    private static func attributes(_ url: URL) throws -> [FileAttributeKey: Any]? {
        do { return try FileManager.default.attributesOfItem(atPath: url.path) }
        catch let error as NSError where error.domain == NSCocoaErrorDomain && (error.code == NSFileReadNoSuchFileError || error.code == NSFileNoSuchFileError) { return nil }
    }

    private static func location(root: URL, path: String) throws -> URL {
        let parts = path.split(separator: "/", omittingEmptySubsequences: false)
        guard !parts.isEmpty, parts.allSatisfy({ !$0.isEmpty && $0 != "." && $0 != ".." && $0.lowercased() != ".git" }), !path.contains("\0") else { throw invalidPath() }
        // lstat, not FileManager: this runs for every path of every capture.
        var parent = root.path
        for part in parts.dropLast() {
            parent += "/" + part
            var info = stat()
            if lstat(parent, &info) == 0 {
                guard info.st_mode & S_IFMT == S_IFDIR else {
                    throw WorkspaceGitError(message: "A symbolic link or file blocks checkpoint access at \(path).")
                }
            } else if errno != ENOENT {
                throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
            }
        }
        return root.appendingPathComponent(path)
    }

    private static func invalidPath() -> WorkspaceGitError { WorkspaceGitError(message: "Git returned an unsupported checkpoint path.") }
    private static func changed(_ path: String) -> WorkspaceGitError { WorkspaceGitError(message: "\(path) changed during restore. Restore stopped; the recovery checkpoint is retained.") }
}
