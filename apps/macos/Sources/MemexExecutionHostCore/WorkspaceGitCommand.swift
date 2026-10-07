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

    public static func data(_ directory: URL, _ arguments: [String], command: CommandRun,
                     environment: [String: String] = [:], timeout: TimeInterval = 30, inputFile: URL? = nil,
                     maximumOutputBytes: Int? = nil) throws -> Data {
        let unset = ProcessInfo.processInfo.environment.keys.filter { $0.hasPrefix("GIT_") }.sorted().flatMap { ["-u", $0] }
        let overrides = environment.sorted { $0.key < $1.key }.map { "\($0.key)=\($0.value)" }
        return try command.execute(executable: URL(fileURLWithPath: "/usr/bin/env"),
            arguments: unset + ["LC_ALL=C", "GIT_TERMINAL_PROMPT=0"] + overrides
                + ["/usr/bin/git", "--no-optional-locks", "-c", "core.fsmonitor=false",
                   "-c", "core.hooksPath=/dev/null", "-c", "submodule.recurse=false", "-C", directory.path] + arguments,
            timeout: timeout, inputFile: inputFile, maximumOutputBytes: maximumOutputBytes)
    }

    public static func text(_ directory: URL, _ arguments: [String], command: CommandRun,
                     environment: [String: String] = [:], timeout: TimeInterval = 30, maximumOutputBytes: Int? = nil) throws -> String {
        let bytes = try data(directory, arguments, command: command, environment: environment, timeout: timeout, maximumOutputBytes: maximumOutputBytes)
        let value = String(decoding: bytes, as: UTF8.self)
        return value.hasSuffix("\n") ? String(value.dropLast()) : value
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
