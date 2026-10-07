import Foundation

enum WorkspaceDiffScope: Sendable, Equatable {
    case current
    case branch(String)
    case checkpoint(from: String?, to: String, label: String)

    var label: String {
        switch self {
        case .current: "Current working tree"
        case .branch(let ref): "Changes from \(ref)"
        case .checkpoint(_, _, let label): label
        }
    }
}

struct WorkspaceReviewContext: Sendable, Equatable {
    let directory: URL
    let path: String
    let scope: String
    let patch: String
    let comment: String
    var sourceURL: URL? = nil

    var promptText: String {
        "Review \(sourceURL?.absoluteString ?? directory.appendingPathComponent(path).path) (\(scope))\n\(comment)\n\n```diff\n\(patch)\n```"
    }
}

extension WorkspaceChangesClient {
    func snapshot(directory: URL, scope: WorkspaceDiffScope) async throws -> WorkspaceChangesSnapshot? {
        if scope == .current { return try await snapshot(directory: directory) }
        return try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            let refs = try Self.diffRefs(scope, root: root, command: command)
            let data = try WorkspaceGitCommand.data(root, ["diff", "--name-status", "-z", "--find-renames"] + refs + ["--"], command: command)
            guard data.count <= Self.outputLimit else { throw WorkspaceChangesError(message: "Too many changed paths to display.") }
            let fields = data.split(separator: 0).map { String(decoding: $0, as: UTF8.self) }
            var files: [WorkspaceChange] = []
            var index = 0
            while index < fields.count {
                let status = fields[index]
                guard let flag = status.first, index + 1 < fields.count else { throw WorkspaceChangesError(message: "Incomplete Git diff status.") }
                let renamed = flag == "R" || flag == "C"
                if renamed {
                    guard index + 2 < fields.count else { throw WorkspaceChangesError(message: "Incomplete Git rename status.") }
                    files.append(.init(path: fields[index + 2], originalPath: fields[index + 1], indexStatus: " ", worktreeStatus: flag))
                    index += 3
                } else {
                    files.append(.init(path: fields[index + 1], originalPath: nil, indexStatus: " ", worktreeStatus: flag))
                    index += 2
                }
            }
            if case .branch = scope {
                let untracked = try WorkspaceGitCommand.data(root, ["ls-files", "--others", "--exclude-standard", "-z"], command: command)
                    .split(separator: 0).map { String(decoding: $0, as: UTF8.self) }
                files += untracked.map { .init(path: $0, originalPath: nil, indexStatus: "?", worktreeStatus: "?") }
            }
            return WorkspaceChangesSnapshot(root: root, files: files.sorted { $0.path.localizedStandardCompare($1.path) == .orderedAscending })
        }
    }

    func diff(file: WorkspaceChange, root: URL, scope: WorkspaceDiffScope) async throws -> String {
        if scope == .current || file.isUntracked { return try await diff(file: file, root: root) }
        return try await WorkspaceGitCommand.run { command in
            let refs = try Self.diffRefs(scope, root: root, command: command)
            let bytes = try WorkspaceGitCommand.data(root,
                ["--literal-pathspecs", "diff", "--no-ext-diff", "--no-textconv", "--no-color", "--find-renames"]
                + refs + ["--"] + [file.originalPath, file.path].compactMap { $0 }, command: command)
            return String(decoding: bytes.prefix(Self.outputLimit), as: UTF8.self)
                + (bytes.count > Self.outputLimit ? "\n\n[Diff truncated at 512 KB.]" : "")
        }
    }

    private static func diffRefs(_ scope: WorkspaceDiffScope, root: URL, command: CommandRun) throws -> [String] {
        switch scope {
        case .current: return []
        case .branch(let ref):
            try WorkspaceGitCommand.validateRef(ref)
            let resolved = try WorkspaceGitCommand.text(root, ["rev-parse", "--verify", "--end-of-options", ref + "^{commit}"], command: command)
            return [try WorkspaceGitCommand.text(root, ["merge-base", resolved, "HEAD"], command: command)]
        case .checkpoint(let from, let to, _):
            try WorkspaceGitCommand.validateRef(to)
            if let from { try WorkspaceGitCommand.validateRef(from) }
            let baseline = try from ?? WorkspaceGitCommand.text(root, ["hash-object", "-t", "tree", "/dev/null"], command: command)
            let first = try WorkspaceGitCommand.text(root, ["rev-parse", "--verify", "--end-of-options", baseline + "^{tree}"], command: command)
            let last = try WorkspaceGitCommand.text(root, ["rev-parse", "--verify", "--end-of-options", to + "^{tree}"], command: command)
            return [first, last]
        }
    }
}

/// Parses unified hunks into paired rows without dropping additions, deletions,
/// context, hunk headers, or separate staged/unstaged sections.
struct WorkspaceSplitDiffRow: Identifiable, Equatable {
    let id: Int
    let left: String
    let right: String
    let kind: Kind
    enum Kind { case context, change, header }

    static func parse(_ patch: String) -> [Self] {
        var rows: [Self] = []
        var removed: [String] = []
        var added: [String] = []
        var inHunk = false
        func flush() {
            for index in 0..<max(removed.count, added.count) {
                rows.append(.init(id: rows.count, left: index < removed.count ? removed[index] : "",
                    right: index < added.count ? added[index] : "", kind: .change))
            }
            removed.removeAll(); added.removeAll()
        }
        for line in patch.split(separator: "\n", omittingEmptySubsequences: false).map(String.init) {
            if line.hasPrefix("diff --git ") || line.hasPrefix("Staged changes") || line.hasPrefix("Unstaged changes") || line == "Untracked file" { inHunk = false }
            if line.hasPrefix("@@") { inHunk = true; flush(); rows.append(.init(id: rows.count, left: line, right: line, kind: .header)); continue }
            if inHunk && line.hasPrefix("-") { removed.append(line) }
            else if inHunk && line.hasPrefix("+") { added.append(line) }
            else {
                flush()
                rows.append(.init(id: rows.count, left: line, right: line,
                    kind: line.hasPrefix(" ") ? .context : .header))
            }
        }
        flush()
        return rows
    }
}
