import Foundation

struct WorkspacePullRequest: Decodable, Sendable {
    struct Author: Decodable, Sendable { let login: String }
    struct Comment: Decodable, Sendable, Identifiable {
        let id: String
        let body: String
        let author: Author?
        let url: URL?
    }
    struct Review: Decodable, Sendable, Identifiable {
        let id: String
        let body: String
        let state: String
        let author: Author?
    }
    let id: String
    let number: Int
    let title: String
    let body: String
    let url: URL
    let state: String
    let isDraft: Bool
    let baseRefName: String
    let headRefName: String
    let headRefOid: String
    let baseRefOid: String
    let comments: [Comment]
    let reviews: [Review]
}

struct WorkspacePullRequestThread: Decodable, Sendable, Identifiable {
    struct Comments: Decodable, Sendable {
        let nodes: [Comment]
        let pageInfo: PageInfo
    }
    struct Comment: Decodable, Sendable, Identifiable {
        let id: String
        let body: String
        let author: WorkspacePullRequest.Author?
        let url: URL
        let diffHunk: String
    }
    struct PageInfo: Decodable, Sendable { let hasNextPage: Bool }
    let id: String
    let path: String
    let line: Int?
    let isResolved: Bool
    let isOutdated: Bool
    let comments: Comments
}

struct WorkspacePullRequestRead: Sendable {
    let pullRequest: WorkspacePullRequest
    let threads: [WorkspacePullRequestThread]
    let hasMoreThreads: Bool
    let patch: String
}

struct WorkspacePullRequestClient: Sendable {
    static let limit = 8 * 1024 * 1024

    func read(directory: URL, selector: String) async throws -> WorkspacePullRequestRead {
        try await WorkspaceGitCommand.run { command in
            let root = try WorkspaceGitCommand.root(directory, command: command)
            let repository = try WorkspaceGitCommand.text(root, ["remote", "get-url", "origin"], command: command)
            let requested = selector.trimmingCharacters(in: .whitespacesAndNewlines)
            let selection = requested.isEmpty
                ? try WorkspaceGitCommand.text(root, ["branch", "--show-current"], command: command) : requested
            guard !selection.isEmpty, !selection.hasPrefix("-"), !selection.contains("\0") else {
                throw WorkspaceGitError(message: "Enter a pull request number or URL, or select a branch.")
            }
            let metadata = try Self.gh(["-R", repository, "pr", "view", selection, "--json",
                "id,number,title,body,url,state,isDraft,baseRefName,headRefName,headRefOid,baseRefOid,comments,reviews"], command: command)
            let pr = try JSONDecoder().decode(WorkspacePullRequest.self, from: metadata)
            // Resolve subsequent reads from the returned identity, never from a
            // mutable branch selection or a guessed GitHub repository name.
            let patch = try Self.gh(["pr", "diff", pr.url.absoluteString, "--color=never"], command: command)
            let query = "query($id:ID!){node(id:$id){... on PullRequest{reviewThreads(first:100){nodes{id path line isResolved isOutdated comments(first:100){nodes{id body author{login} url diffHunk} pageInfo{hasNextPage}}} pageInfo{hasNextPage}}}}}"
            let host = pr.url.host ?? "github.com"
            let threadsData = try Self.gh(["api", "--hostname", host, "graphql", "-f", "query=" + query, "-f", "id=" + pr.id], command: command)
            let threads = try Self.decodeThreads(threadsData)
            // Detect a force-push or new commit during this multi-request read.
            let finalData = try Self.gh(["pr", "view", pr.url.absoluteString, "--json", "headRefOid,baseRefOid"], command: command)
            struct Head: Decodable { let headRefOid: String; let baseRefOid: String }
            let finalHead = try JSONDecoder().decode(Head.self, from: finalData)
            guard finalHead.headRefOid == pr.headRefOid, finalHead.baseRefOid == pr.baseRefOid else {
                throw WorkspaceGitError(message: "The pull request changed while loading. Refresh to read its current diff.")
            }
            return WorkspacePullRequestRead(pullRequest: pr, threads: threads.nodes,
                hasMoreThreads: threads.pageInfo.hasNextPage, patch: String(decoding: patch, as: UTF8.self))
        }
    }

    struct Threads: Decodable { let nodes: [WorkspacePullRequestThread]; let pageInfo: WorkspacePullRequestThread.PageInfo }
    static func decodeThreads(_ data: Data) throws -> Threads {
        struct Response: Decodable {
            struct Payload: Decodable { struct Node: Decodable { let reviewThreads: Threads }; let node: Node }
            let data: Payload
        }
        return try JSONDecoder().decode(Response.self, from: data).data.node.reviewThreads
    }

    private static func gh(_ arguments: [String], command: CommandRun) throws -> Data {
        let search = (ProcessInfo.processInfo.environment["PATH"] ?? "").split(separator: ":").map(String.init)
            + ["/opt/homebrew/bin", "/usr/local/bin", "/usr/bin"]
        guard let executable = search.map({ URL(fileURLWithPath: $0).appendingPathComponent("gh").path })
            .first(where: { FileManager.default.isExecutableFile(atPath: $0) }) else {
            throw WorkspaceGitError(message: "Install and authenticate GitHub CLI to read pull requests.")
        }
        let bytes = try command.execute(executable: URL(fileURLWithPath: "/usr/bin/env"),
            arguments: ["GH_PROMPT_DISABLED=1", "GH_PAGER=cat", executable] + arguments, timeout: 60,
            maximumOutputBytes: limit)
        guard bytes.count <= limit else { throw WorkspaceGitError(message: "This pull request exceeds the 8 MB native review limit. Open it on GitHub.") }
        return bytes
    }
}
