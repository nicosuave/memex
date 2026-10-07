import Foundation

/// Checkpoints preserve disk bytes, bypassing Git clean/smudge filters and EOL
/// conversion. The index tree is recorded separately by the checkpoint owner.
enum WorkspaceRawTree {
    struct Entry: Equatable {
        let mode: String
        let object: String
    }

    static func entries(root: URL, tree: String, command: CommandRun) throws -> [String: Entry] {
        let data = try WorkspaceGitCommand.data(root, ["ls-tree", "-r", "-z", tree], command: command)
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

    static func capture(root: URL, temporary: URL, environment: [String: String], command: CommandRun) throws {
        let indexedTree = try WorkspaceGitCommand.text(root, ["write-tree"], command: command, environment: environment)
        let indexed = try entries(root: root, tree: indexedTree, command: command)
        let data = try WorkspaceGitCommand.data(root, ["ls-files", "--cached", "--others", "--exclude-standard", "-z"], command: command)
        let paths = try Set(data.split(separator: 0).map { bytes -> String in
            guard let path = String(data: Data(bytes), encoding: .utf8) else { throw invalidPath() }
            return path
        })
        var records = Data()
        var batch: [(path: String, source: URL, mode: String)] = []
        var batchSize = 0
        func appendRecord(mode: String, object: String, path: String) {
            records.append(Data("\(mode) \(object)\t\(path)\0".utf8))
        }
        func flush() throws {
            guard !batch.isEmpty else { return }
            let result = try WorkspaceGitCommand.text(root,
                ["hash-object", "-w", "--no-filters", "--"] + batch.map { $0.source.path }, command: command, timeout: 120)
            let objects = result.split(separator: "\n")
            guard objects.count == batch.count else { throw invalidPath() }
            for (item, object) in zip(batch, objects) {
                appendRecord(mode: item.mode, object: String(object), path: item.path)
            }
            batch.removeAll(keepingCapacity: true)
            batchSize = 0
        }
        for path in paths.sorted() {
            let url = try location(root: root, path: path)
            // A gitlink belongs to the nested repository and is never flattened.
            if let entry = indexed[path], entry.mode == "160000" {
                appendRecord(mode: entry.mode, object: entry.object, path: path)
                continue
            }
            guard let attributes = try attributes(url) else { continue }
            let mode: String
            let source: URL
            switch attributes[.type] as? FileAttributeType {
            case .typeRegular:
                mode = ((attributes[.posixPermissions] as? NSNumber)?.intValue ?? 0) & 0o111 == 0 ? "100644" : "100755"
                source = url
            case .typeSymbolicLink:
                mode = "120000"
                source = temporary.appendingPathComponent("symlink-" + UUID().uuidString)
                try Data(FileManager.default.destinationOfSymbolicLink(atPath: url.path).utf8).write(to: source, options: .atomic)
            default:
                throw WorkspaceGitError(message: "Checkpoint cannot snapshot the directory or special file at \(path). Preserve nested repositories separately.")
            }
            if batchSize + source.path.utf8.count > 64 * 1024 { try flush() }
            batch.append((path, source, mode))
            batchSize += source.path.utf8.count + 1
        }
        try flush()
        let input = temporary.appendingPathComponent("index-info")
        try records.write(to: input)
        _ = try WorkspaceGitCommand.text(root, ["read-tree", "--empty"], command: command, environment: environment)
        _ = try WorkspaceGitCommand.data(root, ["update-index", "-z", "--index-info"], command: command,
            environment: environment, timeout: 120, inputFile: input)
    }

    static func restore(root: URL, target: [String: Entry], current: [String: Entry], command: CommandRun) throws {
        let changed = Set(target.keys).union(current.keys).filter { target[$0] != current[$0] }.sorted()
        // Reject structural conflicts before touching files. In particular never
        // traverse a symlink, remove a directory, or mutate a nested repository.
        for path in changed {
            let url = try location(root: root, path: path)
            if target[path]?.mode == "160000" || current[path]?.mode == "160000" {
                throw WorkspaceGitError(message: "A nested repository changed at \(path). Restore it separately.")
            }
            let type = try attributes(url)?[.type] as? FileAttributeType
            guard type == nil || type == .typeRegular || type == .typeSymbolicLink else {
                throw WorkspaceGitError(message: "A directory or special file blocks restore at \(path). Files were preserved.")
            }
            try verify(root: root, path: path, expected: current[path], command: command)
        }
        for path in changed {
            let url = try location(root: root, path: path)
            try verify(root: root, path: path, expected: current[path], command: command)
            guard let entry = target[path] else {
                if try attributes(url) != nil { try FileManager.default.removeItem(at: url) }
                continue
            }
            let bytes = try WorkspaceGitCommand.data(root, ["cat-file", "blob", entry.object], command: command)
            try FileManager.default.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
            _ = try location(root: root, path: path)
            switch entry.mode {
            case "100644", "100755":
                // Atomic write replaces a final symlink rather than its target.
                if try attributes(url)?[.type] as? FileAttributeType == .typeSymbolicLink {
                    try FileManager.default.removeItem(at: url)
                }
                try bytes.write(to: url, options: .atomic)
                try FileManager.default.setAttributes([.posixPermissions: entry.mode == "100755" ? 0o755 : 0o644], ofItemAtPath: url.path)
            case "120000":
                guard let destination = String(data: bytes, encoding: .utf8) else { throw invalidPath() }
                if try attributes(url) != nil { try FileManager.default.removeItem(at: url) }
                try FileManager.default.createSymbolicLink(atPath: url.path, withDestinationPath: destination)
            default: throw WorkspaceGitError(message: "Unsupported checkpoint file mode \(entry.mode).")
            }
        }
    }

    private static func verify(root: URL, path: String, expected: Entry?, command: CommandRun) throws {
        let url = try location(root: root, path: path)
        let attributes = try attributes(url)
        guard let expected else {
            guard attributes == nil else { throw changed(path) }
            return
        }
        guard let attributes else { throw changed(path) }
        let actual: Data
        if expected.mode == "120000" {
            guard attributes[.type] as? FileAttributeType == .typeSymbolicLink else { throw changed(path) }
            actual = try Data(FileManager.default.destinationOfSymbolicLink(atPath: url.path).utf8)
        } else {
            guard attributes[.type] as? FileAttributeType == .typeRegular else { throw changed(path) }
            actual = try Data(contentsOf: url)
        }
        let captured = try WorkspaceGitCommand.data(root, ["cat-file", "blob", expected.object], command: command)
        guard actual == captured else { throw changed(path) }
    }

    private static func attributes(_ url: URL) throws -> [FileAttributeKey: Any]? {
        do { return try FileManager.default.attributesOfItem(atPath: url.path) }
        catch let error as NSError where error.domain == NSCocoaErrorDomain && (error.code == NSFileReadNoSuchFileError || error.code == NSFileNoSuchFileError) { return nil }
    }

    private static func location(root: URL, path: String) throws -> URL {
        let parts = path.split(separator: "/", omittingEmptySubsequences: false)
        guard !parts.isEmpty, parts.allSatisfy({ !$0.isEmpty && $0 != "." && $0 != ".." && $0.lowercased() != ".git" }), !path.contains("\0") else { throw invalidPath() }
        var parent = root
        for part in parts.dropLast() {
            parent.appendPathComponent(String(part))
            if let type = try attributes(parent)?[.type] as? FileAttributeType, type != .typeDirectory {
                throw WorkspaceGitError(message: "A symbolic link or file blocks checkpoint access at \(path).")
            }
        }
        return root.appendingPathComponent(path)
    }

    private static func invalidPath() -> WorkspaceGitError { WorkspaceGitError(message: "Git returned an unsupported checkpoint path.") }
    private static func changed(_ path: String) -> WorkspaceGitError { WorkspaceGitError(message: "\(path) changed during restore. Restore stopped; the recovery checkpoint is retained.") }
}
