import Foundation

struct WorkspaceSetupClient: Sendable {
    /// Only run after an explicit user action. A repository's scripts are code;
    /// merely selecting a folder or creating a worktree does not authorize them.
    func run(directory: URL, script: String) async throws -> String {
        guard let script = script.nilIfBlank, !script.contains("\0") else {
            throw WorkspaceGitError(message: "Enter a setup command.")
        }
        return try await WorkspaceGitCommand.run { command in
            let directory = try LocalProjects.validatedDirectory(directory)
            // Pass cwd and script as separate positional arguments; neither is
            // interpolated into shell source. Script itself is user-authored code.
            let data = try command.execute(executable: URL(fileURLWithPath: "/bin/zsh"),
                arguments: ["-lc", "cd -- \"$1\" && exec /bin/zsh -c \"$2\"", "memex-setup", directory.path, script], timeout: 1800)
            return String(decoding: data.suffix(512 * 1024), as: UTF8.self)
        }
    }
}
