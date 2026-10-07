import Foundation
import Darwin

struct WorkspaceChange: Identifiable, Hashable, Sendable {
    let path: String
    let originalPath: String?
    let indexStatus: Character
    let worktreeStatus: Character
    var id: String { path }
    var isUntracked: Bool { indexStatus == "?" }
    var label: String { originalPath.map { "\($0) → \(path)" } ?? path }
    var status: String {
        if isUntracked { return "Untracked" }
        if indexStatus == "U" || worktreeStatus == "U" { return "Conflict" }
        return [indexStatus != " " ? "Staged \(indexStatus)" : nil,
                worktreeStatus != " " ? "Unstaged \(worktreeStatus)" : nil].compactMap { $0 }.joined(separator: " · ")
    }
}

struct WorkspaceChangesSnapshot: Sendable {
    let root: URL
    let files: [WorkspaceChange]
}

struct WorkspaceChangesError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}

/// Reads the entire Git worktree containing the selected directory. Index and
/// worktree changes are separate patches so a staged edit undone locally remains visible.
struct WorkspaceChangesClient: Sendable {
    static let outputLimit = 512 * 1024

    /// Resolve the worktree without reading its status or index. Linked Git
    /// worktrees deliberately remain distinct even when they share a gitdir.
    func worktreeRoot(directory: URL) async throws -> URL? {
        try await Task.detached(priority: .userInitiated) {
            try Self.readWorktreeRoot(directory)
        }.value
    }

    private static func readWorktreeRoot(_ directory: URL) throws -> URL? {
        let probe = try git(directory, ["rev-parse", "--show-toplevel"])
        if probe.code != 0 {
            if probe.error.contains("not a git repository") { return nil }
            throw WorkspaceChangesError(message: probe.error)
        }
        let path = probe.text.hasSuffix("\n") ? String(probe.text.dropLast()) : probe.text
        guard !path.isEmpty, !probe.truncated else {
            throw WorkspaceChangesError(message: "Git returned an invalid workspace directory.")
        }
        return URL(fileURLWithPath: path, isDirectory: true).resolvingSymlinksInPath().standardizedFileURL
    }

    func snapshot(directory: URL) async throws -> WorkspaceChangesSnapshot? {
        try await Task.detached(priority: .userInitiated) {
            guard let root = try Self.readWorktreeRoot(directory) else { return nil }
            let status = try Self.git(root, ["status", "--porcelain=v1", "-z", "--untracked-files=all", "--ignore-submodules=none"])
            try status.requireSuccess()
            guard !status.truncated else { throw WorkspaceChangesError(message: "Too many changed paths to display (status exceeds 512 KB). Narrow the workspace using Git outside Memex.") }
            let records = status.text.split(separator: "\0", omittingEmptySubsequences: false)
            var files: [WorkspaceChange] = []
            var position = 0
            while position < records.count {
                let record = records[position]
                position += 1
                if record.isEmpty { continue }
                guard record.count >= 4 else { throw WorkspaceChangesError(message: "Git returned an incomplete status record.") }
                let flags = Array(record.prefix(2))
                let renamed = flags.contains("R") || flags.contains("C")
                var original: String?
                if renamed {
                    guard position < records.count, !records[position].isEmpty else {
                        throw WorkspaceChangesError(message: "Git returned an incomplete rename record.")
                    }
                    original = String(records[position]); position += 1
                }
                files.append(WorkspaceChange(path: String(record.dropFirst(3)), originalPath: original,
                                             indexStatus: flags[0], worktreeStatus: flags[1]))
            }
            return WorkspaceChangesSnapshot(root: root, files: files)
        }.value
    }

    func diff(file: WorkspaceChange, root: URL) async throws -> String {
        try await Task.detached(priority: .userInitiated) {
            let common = ["--literal-pathspecs", "diff", "--no-ext-diff", "--no-textconv", "--no-color", "--find-renames", "--submodule=short"]
            if file.isUntracked {
                let url = root.appendingPathComponent(file.path)
                let values = try url.resourceValues(forKeys: [.isDirectoryKey, .isSymbolicLinkKey])
                if values.isDirectory == true && values.isSymbolicLink != true {
                    return "Untracked directory: \(file.path)\nGit does not provide a file patch for this directory."
                }
                let result = try Self.git(root, common + ["--no-index", "--", "/dev/null", url.path])
                try result.requireSuccess(allowDifference: true)
                return "Untracked file\n\n" + result.displayText
            }
            let paths = [file.originalPath, file.path].compactMap { $0 }
            var sections: [String] = []
            if file.indexStatus != " " {
                let result = try Self.git(root, common + ["--cached", "--"] + paths)
                try result.requireSuccess()
                sections.append("Staged changes (HEAD → index)\n\n" + result.displayText)
            }
            if file.worktreeStatus != " " {
                let result = try Self.git(root, common + ["--"] + paths)
                try result.requireSuccess()
                sections.append("Unstaged changes (index → working tree)\n\n" + result.displayText)
            }
            return sections.joined(separator: "\n\n")
        }.value
    }

    private struct Output {
        let code: Int32
        let text: String
        let error: String
        let truncated: Bool
        var displayText: String {
            (text.isEmpty ? "No textual patch. The file may have changed since refresh, or contain submodule-only changes." : text)
                + (truncated ? "\n\n[Diff truncated at 512 KB.]" : "")
        }
        func requireSuccess(allowDifference: Bool = false) throws {
            guard code == 0 || (allowDifference && code == 1) || truncated else {
                throw WorkspaceChangesError(message: error.isEmpty ? "Git could not read workspace changes." : error)
            }
        }
    }

    /// Temporary output files avoid pipe deadlocks. Size and time watchdogs bound
    /// each read; Git cannot invoke external diff drivers, text conversion or fsmonitor hooks.
    private static func git(_ directory: URL, _ arguments: [String]) throws -> Output {
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent("memex-diff-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: temp, withIntermediateDirectories: false)
        defer { try? FileManager.default.removeItem(at: temp) }
        let outputURL = temp.appendingPathComponent("stdout")
        let errorURL = temp.appendingPathComponent("stderr")
        FileManager.default.createFile(atPath: outputURL.path, contents: nil)
        FileManager.default.createFile(atPath: errorURL.path, contents: nil)
        let output = try FileHandle(forWritingTo: outputURL)
        let errors = try FileHandle(forWritingTo: errorURL)
        defer { try? output.close(); try? errors.close() }
        let child = Process()
        child.executableURL = URL(fileURLWithPath: "/usr/bin/git")
        child.arguments = ["--no-optional-locks", "-c", "core.fsmonitor=false", "-C", directory.path] + arguments
        var environment = ProcessInfo.processInfo.environment
        // Repository selectors inherited from a parent Git process must not redirect this view.
        for key in ["GIT_DIR", "GIT_WORK_TREE", "GIT_INDEX_FILE", "GIT_COMMON_DIR", "GIT_EXTERNAL_DIFF"] { environment.removeValue(forKey: key) }
        environment["GIT_LITERAL_PATHSPECS"] = "1"
        environment["LC_ALL"] = "C"
        child.environment = environment
        child.standardInput = FileHandle.nullDevice
        child.standardOutput = output
        child.standardError = errors
        try child.run()
        let deadline = Date().addingTimeInterval(15)
        var truncated = false
        var timedOut = false
        while child.isRunning {
            let size = (try? outputURL.resourceValues(forKeys: [.fileSizeKey]).fileSize) ?? 0
            let errorSize = (try? errorURL.resourceValues(forKeys: [.fileSizeKey]).fileSize) ?? 0
            if size > outputLimit || errorSize > outputLimit || Date() >= deadline {
                truncated = size > outputLimit
                timedOut = Date() >= deadline
                kill(child.processIdentifier, SIGKILL)
                break
            }
            Thread.sleep(forTimeInterval: 0.01)
        }
        child.waitUntilExit()
        if timedOut { throw WorkspaceChangesError(message: "Git took too long to read workspace changes. Try refreshing.") }
        let reader = try FileHandle(forReadingFrom: outputURL)
        let errorReader = try FileHandle(forReadingFrom: errorURL)
        defer { try? reader.close(); try? errorReader.close() }
        let bytes = try reader.read(upToCount: outputLimit + 1) ?? Data()
        let errorBytes = try errorReader.read(upToCount: 4000) ?? Data()
        return Output(code: child.terminationStatus, text: String(decoding: bytes.prefix(outputLimit), as: UTF8.self),
                      error: String(decoding: errorBytes, as: UTF8.self), truncated: truncated || bytes.count > outputLimit)
    }
}
