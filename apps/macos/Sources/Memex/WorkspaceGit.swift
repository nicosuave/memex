import Foundation
import MemexExecutionHostCore

typealias WorkspaceGitError = MemexExecutionHostCore.WorkspaceGitError
typealias WorkspaceGitCommand = MemexExecutionHostCore.WorkspaceGitCommand

struct WorkspaceGitStatus: Sendable {
    let branch: String
    let head: String?
    let hasStagedChanges: Bool
    let hasUncommittedChanges: Bool
    let remotes: [String]
}

struct WorkspaceGitClient: Sendable {
    func status(directory: URL) async throws -> WorkspaceGitStatus {
        try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            let branch = try WorkspaceGitCommand.text(root, ["branch", "--show-current"], command: command)
            let head = try? WorkspaceGitCommand.text(root, ["rev-parse", "--verify", "HEAD"], command: command)
            let status = try WorkspaceGitCommand.text(root, ["status", "--porcelain=v1", "-z"], command: command)
            let staged = try WorkspaceGitCommand.text(root, ["diff", "--cached", "--name-only", "-z"], command: command)
            let remotes = try WorkspaceGitCommand.text(root, ["remote"], command: command).split(separator: "\n").map(String.init)
            return WorkspaceGitStatus(branch: branch, head: head, hasStagedChanges: !staged.isEmpty,
                hasUncommittedChanges: !status.isEmpty, remotes: remotes)
        }
    }

    /// Stage precisely the paths the user selected. Does not stage unrelated work.
    func stage(directory: URL, paths: [String]) async throws {
        try await changeIndex(directory: directory, paths: paths, stage: true)
    }

    func unstage(directory: URL, paths: [String]) async throws {
        try await changeIndex(directory: directory, paths: paths, stage: false)
    }

    private func changeIndex(directory: URL, paths: [String], stage: Bool) async throws {
        guard !paths.isEmpty, paths.allSatisfy({ !$0.isEmpty && !$0.hasPrefix("/") && !$0.split(separator: "/").contains("..") && !$0.contains("\0") }) else {
            throw WorkspaceGitError(message: "Select files inside this workspace.")
        }
        try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            if stage {
                _ = try WorkspaceGitCommand.text(root, ["--literal-pathspecs", "add", "--"] + paths, command: command)
            } else if (try? WorkspaceGitCommand.text(root, ["rev-parse", "--verify", "HEAD"], command: command)) != nil {
                _ = try WorkspaceGitCommand.text(root, ["--literal-pathspecs", "restore", "--staged", "--"] + paths, command: command)
            } else {
                _ = try WorkspaceGitCommand.text(root, ["--literal-pathspecs", "rm", "--cached", "--"] + paths, command: command)
            }
        }
    }

    /// Explicit product action commits the existing index only. Never `-a`.
    func commit(directory: URL, message: String) async throws -> String {
        guard let message = message.nilIfBlank else { throw WorkspaceGitError(message: "Enter a commit message.") }
        return try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            _ = try WorkspaceGitCommand.text(root, ["commit", "-m", message], command: command, timeout: 120)
            return try WorkspaceGitCommand.text(root, ["rev-parse", "HEAD"], command: command)
        }
    }

    func push(directory: URL, remote: String) async throws {
        try WorkspaceGitCommand.validateRef(remote)
        try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            let remotes = try WorkspaceGitCommand.text(root, ["remote"], command: command).split(separator: "\n")
            guard remotes.contains(Substring(remote)) else { throw WorkspaceGitError(message: "Choose a configured remote.") }
            let branch = try WorkspaceGitCommand.text(root, ["branch", "--show-current"], command: command)
            guard !branch.isEmpty else { throw WorkspaceGitError(message: "Create or attach a branch before pushing a detached checkout.") }
            _ = try WorkspaceGitCommand.text(root, ["push", "--set-upstream", "--", remote, "HEAD:refs/heads/" + branch], command: command, timeout: 300)
        }
    }

    func createPullRequest(directory: URL, title: String, body: String, base: String) async throws -> URL {
        guard let title = title.nilIfBlank else { throw WorkspaceGitError(message: "Enter a pull request title.") }
        try WorkspaceGitCommand.validateRef(base)
        return try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            let temporary = FileManager.default.temporaryDirectory.appendingPathComponent("memex-pr-" + UUID().uuidString)
            try FileManager.default.createDirectory(at: temporary, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
            defer { try? FileManager.default.removeItem(at: temporary) }
            let description = temporary.appendingPathComponent("body.md")
            try Data(body.utf8).write(to: description)
            let search = (ProcessInfo.processInfo.environment["PATH"] ?? "").split(separator: ":").map(String.init)
                + ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"]
            guard let gh = search.map({ URL(fileURLWithPath: $0).appendingPathComponent("gh").path })
                .first(where: { FileManager.default.isExecutableFile(atPath: $0) }) else {
                throw WorkspaceGitError(message: "GitHub CLI is required to create a pull request. Install and authenticate gh, then retry.")
            }
            // gh uses the explicitly supplied repo directory; no shell expansion,
            // implicit push, browser launch or interactive authentication.
            let data = try command.execute(executable: URL(fileURLWithPath: "/usr/bin/env"),
                arguments: ["GH_PROMPT_DISABLED=1", gh, "-R", try WorkspaceGitCommand.text(root,
                    ["remote", "get-url", "origin"], command: command), "pr", "create", "--draft", "--title", title,
                    "--body-file", description.path, "--base", base, "--head",
                    try WorkspaceGitCommand.text(root, ["branch", "--show-current"], command: command)], timeout: 120)
            let value = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
            guard let url = URL(string: value), url.scheme == "https" else { throw WorkspaceGitError(message: "The pull request command did not return a URL: \(value)") }
            return url
        }
    }
}
