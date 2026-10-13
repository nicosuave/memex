import Foundation

public struct WorkspaceGitError: LocalizedError, Sendable {
    public init(message: String) { self.message = message }
    public let message: String
    public var errorDescription: String? { message }
}

/// One command boundary for app-owned Git operations. Callers supply arguments,
/// never a shell script. Existing watchdog and cancellation own the subprocess.
public enum WorkspaceGitCommand {
    public static func run<T: Sendable>(_ body: @escaping @Sendable (CommandRun) throws -> T) async throws -> T {
        let command = CommandRun()
        return try await withTaskCancellationHandler {
            try Task.checkCancellation()
            return try await Task.detached(priority: .userInitiated) { try body(command) }.value
        } onCancel: { command.cancel() }
    }

    /// Repository hooks are disabled for app-internal and automatic commands.
    /// Pass `runHooks` only for an explicit user action whose hooks the user
    /// expects to run (commit and push), so pre-commit, commit-msg and Git LFS
    /// pre-push uploads behave as they do in a terminal.
    public static func data(_ directory: URL, _ arguments: [String], command: CommandRun,
                     environment: [String: String] = [:], timeout: TimeInterval = 30, inputFile: URL? = nil,
                     maximumOutputBytes: Int? = nil, outputFile: URL? = nil, runHooks: Bool = false) throws -> Data {
        let unset = ProcessInfo.processInfo.environment.keys.filter { $0.hasPrefix("GIT_") }.sorted().flatMap { ["-u", $0] }
        var variables = ["PATH": toolPath()]
        variables.merge(environment) { _, override in override }
        let overrides = variables.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }
        let hooks = runHooks ? [] : ["-c", "core.hooksPath=/dev/null"]
        return try command.execute(executable: URL(fileURLWithPath: "/usr/bin/env"),
            arguments: unset + ["LC_ALL=C", "GIT_TERMINAL_PROMPT=0"] + overrides
                + ["/usr/bin/git", "--no-optional-locks", "-c", "core.fsmonitor=false"] + hooks
                + ["-c", "submodule.recurse=false", "-C", directory.path] + arguments,
            timeout: timeout, inputFile: inputFile, maximumOutputBytes: maximumOutputBytes, outputFile: outputFile)
    }

    public static func text(_ directory: URL, _ arguments: [String], command: CommandRun,
                     environment: [String: String] = [:], timeout: TimeInterval = 30, maximumOutputBytes: Int? = nil,
                     runHooks: Bool = false) throws -> String {
        let bytes = try data(directory, arguments, command: command, environment: environment, timeout: timeout,
                             maximumOutputBytes: maximumOutputBytes, runHooks: runHooks)
        let value = String(decoding: bytes, as: UTF8.self)
        return value.hasSuffix("\n") ? String(value.dropLast()) : value
    }

    /// An app launched from Finder inherits launchd's minimal PATH. Git
    /// filters, credential helpers and hooks (git-lfs, gh, linters) commonly
    /// live in Homebrew prefixes, so append them after the inherited entries.
    public static func toolPath(_ inherited: String? = ProcessInfo.processInfo.environment["PATH"]) -> String {
        var entries: [String] = []
        for entry in (inherited ?? "").split(separator: ":").map(String.init)
            + ["/opt/homebrew/bin", "/opt/homebrew/sbin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"]
            where !entry.isEmpty && !entries.contains(entry) { entries.append(entry) }
        return entries.joined(separator: ":")
    }

    public static func root(_ directory: URL, command: CommandRun) throws -> URL {
        URL(fileURLWithPath: try text(directory, ["rev-parse", "--show-toplevel"], command: command), isDirectory: true)
            .standardizedFileURL.resolvingSymlinksInPath()
    }

    public static func validateRef(_ ref: String) throws {
        guard !ref.isEmpty, !ref.hasPrefix("-"), !ref.contains("\0"), !ref.contains("\n") else {
            throw WorkspaceGitError(message: "Choose a valid Git ref.")
        }
    }
}
