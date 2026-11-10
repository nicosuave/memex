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
        // A trailing slash is how Git names an untracked nested repository;
        // `git add` would embed it as a gitlink rather than stage files.
        guard !paths.contains(where: { $0.hasSuffix("/") }) else {
            throw WorkspaceGitError(message: "This is a nested Git repository. Commit inside it, or add it as a submodule with Git.")
        }
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
    /// The repository's pre-commit and commit-msg hooks run, as in a terminal.
    func commit(directory: URL, message: String) async throws -> String {
        guard let message = message.nilIfBlank else { throw WorkspaceGitError(message: "Enter a commit message.") }
        return try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            do {
                _ = try WorkspaceGitCommand.text(root, ["commit", "-m", message], command: command, timeout: 600, runHooks: true)
            } catch { throw Self.hookFailure("Commit", error) }
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
            // pre-push runs so Git LFS uploads objects before their pointers.
            do {
                _ = try WorkspaceGitCommand.text(root, ["push", "--set-upstream", "--", remote, "HEAD:refs/heads/" + branch],
                    command: command, timeout: 600, runHooks: true)
            } catch { throw Self.hookFailure("Push", error) }
        }
    }

    private static func hookFailure(_ action: String, _ error: any Error) -> any Error {
        guard !(error is CancellationError) else { return error }
        return WorkspaceGitError(message: "\(action) failed. Output from Git and repository hooks:\n\(error.localizedDescription)")
    }

    func createPullRequest(directory: URL, title: String, body: String, base: String) async throws -> URL {
        guard let title = title.nilIfBlank else { throw WorkspaceGitError(message: "Enter a pull request title.") }
        try WorkspaceGitCommand.validateRef(base)
        return try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            let target = try Self.pullRequestTarget(root: root, command: command)
            let temporary = FileManager.default.temporaryDirectory.appendingPathComponent("memex-pr-" + UUID().uuidString)
            try FileManager.default.createDirectory(at: temporary, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
            defer { try? FileManager.default.removeItem(at: temporary) }
            let description = temporary.appendingPathComponent("body.md")
            try Data(body.utf8).write(to: description)
            let search = WorkspaceGitCommand.toolPath().split(separator: ":").map(String.init)
            guard let gh = search.map({ URL(fileURLWithPath: $0).appendingPathComponent("gh").path })
                .first(where: { FileManager.default.isExecutableFile(atPath: $0) }) else {
                throw WorkspaceGitError(message: "GitHub CLI is required to create a pull request. Install and authenticate gh, then retry.")
            }
            // gh uses the explicitly supplied repo directory; no shell expansion,
            // implicit push, browser launch or interactive authentication.
            let data = try command.execute(executable: URL(fileURLWithPath: "/usr/bin/env"),
                arguments: ["GH_PROMPT_DISABLED=1", "PATH=" + WorkspaceGitCommand.toolPath(), gh, "-R", target.repository,
                    "pr", "create", "--draft", "--title", title, "--body-file", description.path, "--base", base,
                    "--head", target.head], timeout: 120)
            let value = String(decoding: data, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines)
            guard let url = URL(string: value), url.scheme == "https" else { throw WorkspaceGitError(message: "The pull request command did not return a URL: \(value)") }
            return url
        }
    }

    struct PullRequestTarget: Equatable {
        /// Base repository URL passed to `gh -R`.
        let repository: String
        /// `branch` for a same-repository PR, `owner:branch` from a fork.
        let head: String
    }

    /// Reads the branch's push remote and the base remote without network
    /// access. A fork workflow keeps the parent as `upstream`; otherwise
    /// `origin` is the base repository.
    private static func pullRequestTarget(root: URL, command: CommandRun) throws -> PullRequestTarget {
        let branch = try WorkspaceGitCommand.text(root, ["branch", "--show-current"], command: command)
        guard !branch.isEmpty else { throw WorkspaceGitError(message: "Create or attach a branch before opening a pull request.") }
        func config(_ key: String) -> String? {
            (try? WorkspaceGitCommand.text(root, ["config", "--get", "--end-of-options", key], command: command))?.nilIfBlank
        }
        let remotes = try WorkspaceGitCommand.text(root, ["remote"], command: command).split(separator: "\n").map(String.init)
        var urls: [String: String] = [:]
        for remote in remotes {
            try WorkspaceGitCommand.validateRef(remote)
            urls[remote] = try WorkspaceGitCommand.text(root, ["remote", "get-url", "--", remote], command: command)
        }
        return try pullRequestTarget(branch: branch, remoteURLs: urls,
            pushRemote: config("branch.\(branch).pushRemote") ?? config("remote.pushDefault"),
            upstreamRemote: config("branch.\(branch).remote"), mergeRef: config("branch.\(branch).merge"))
    }

    static func pullRequestTarget(branch: String, remoteURLs: [String: String], pushRemote: String?,
                                  upstreamRemote: String?, mergeRef: String?) throws -> PullRequestTarget {
        guard let baseName = ["upstream", "origin"].first(where: { remoteURLs[$0] != nil }), let base = remoteURLs[baseName],
              let baseRepository = githubRepository(base) else {
            throw WorkspaceGitError(message: "Add a GitHub remote named origin (or upstream for a fork) before opening a pull request.")
        }
        // The branch was pushed with --set-upstream, so its tracking remote
        // names the repository that actually holds the commits.
        guard let headName = pushRemote ?? upstreamRemote, let headURL = remoteURLs[headName] else {
            throw WorkspaceGitError(message: "Push this branch before creating a pull request.")
        }
        guard let headRepository = githubRepository(headURL) else {
            throw WorkspaceGitError(message: "The pushed remote \(headName) is not a GitHub repository.")
        }
        var headBranch = branch
        if pushRemote == nil, let mergeRef, mergeRef.hasPrefix("refs/heads/") { headBranch = String(mergeRef.dropFirst(11)) }
        try WorkspaceGitCommand.validateRef(headBranch)
        guard headRepository.host == baseRepository.host else {
            throw WorkspaceGitError(message: "The pushed remote and base repository are on different GitHub hosts.")
        }
        let sameRepository = headRepository.owner.lowercased() == baseRepository.owner.lowercased()
            && headRepository.name.lowercased() == baseRepository.name.lowercased()
        return PullRequestTarget(repository: base, head: sameRepository ? headBranch : headRepository.owner + ":" + headBranch)
    }

    /// Parses `git@host:owner/repo.git`, `ssh://git@host/owner/repo` and
    /// `https://host/owner/repo(.git)` without resolving URL rewrites.
    static func githubRepository(_ url: String) -> (host: String, owner: String, name: String)? {
        var host: String
        var path: String
        if let parsed = URLComponents(string: url), let scheme = parsed.scheme, ["https", "http", "ssh", "git"].contains(scheme),
           let parsedHost = parsed.host {
            host = parsedHost
            path = parsed.path
        } else if let colon = url.firstIndex(of: ":"), !url.contains("://") {
            host = String(url[..<colon])
            if let at = host.lastIndex(of: "@") { host = String(host[host.index(after: at)...]) }
            path = String(url[url.index(after: colon)...])
        } else { return nil }
        let parts = path.split(separator: "/").map(String.init)
        guard parts.count == 2, !host.isEmpty else { return nil }
        let owner = parts[0]
        let name = parts[1].hasSuffix(".git") ? String(parts[1].dropLast(4)) : parts[1]
        let allowed = CharacterSet.alphanumerics.union(CharacterSet(charactersIn: "-_."))
        guard !owner.isEmpty, !name.isEmpty, !owner.hasPrefix("-"),
              owner.unicodeScalars.allSatisfy(allowed.contains), name.unicodeScalars.allSatisfy(allowed.contains) else { return nil }
        return (host.lowercased(), owner, name)
    }
}
