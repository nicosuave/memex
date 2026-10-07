import Darwin
import Foundation

struct ProjectCreationRecord: Codable, Sendable, Equatable {
    enum Kind: String, Codable, Sendable { case repository, clone }
    enum State: String, Codable, Sendable { case preparing, ready, failed, cancelled }
    let id: String
    let destination: URL
    let kind: Kind
    let remote: String?
    var state: State
    var failure: String?
}

struct ProjectCreationFailure: LocalizedError, Sendable {
    let message: String
    let retainedDirectory: URL
    var errorDescription: String? { message + " Files were retained at " + retainedDirectory.path + "." }
}

struct ProjectCreationClient: Sendable {
    let recordsRoot: URL
    init(recordsRoot: URL = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent("Library/Application Support/dev.memex.app/ProjectOperations", isDirectory: true)) {
        self.recordsRoot = recordsRoot
    }

    /// A retry reserves another empty directory; it never recursively removes a
    /// partial clone or assumes a failed directory contains no user work.
    func create(parent: URL, name: String, kind: ProjectCreationRecord.Kind, remote: String? = nil,
                retryInNewFolder: Bool = false) async throws -> URL {
        let cleanName = name.trimmingCharacters(in: .whitespacesAndNewlines)
        guard !cleanName.isEmpty, cleanName != ".", cleanName != "..", !cleanName.contains("/"), !cleanName.contains("\0") else {
            throw WorkspaceGitError(message: "Enter a project name without path separators.")
        }
        if kind == .clone {
            guard let remote = remote?.nilIfBlank, !remote.hasPrefix("-"), !remote.contains("\0"), !remote.contains("\n"),
                  !remote.hasPrefix("ext::") else { throw WorkspaceGitError(message: "Enter a Git repository URL or local repository path.") }
            if let components = URLComponents(string: remote), components.scheme?.hasPrefix("http") == true,
               components.user != nil || components.password != nil {
                throw WorkspaceGitError(message: "Use a repository URL without embedded credentials and your configured Git authentication.")
            }
        }
        let recordsRoot = self.recordsRoot
        return try await WorkspaceGitCommand.run { command in
            let parent = try LocalProjects.validatedDirectory(parent)
            let manager = FileManager.default
            var destination = parent.appendingPathComponent(cleanName, isDirectory: true)
            var reserved = false
            for attempt in 1...(retryInNewFolder ? 1000 : 1) {
                destination = parent.appendingPathComponent(attempt == 1 ? cleanName : "\(cleanName)-\(attempt)", isDirectory: true)
                if destination.path.withCString({ mkdir($0, 0o700) }) == 0 { reserved = true; break }
                if errno != EEXIST { throw WorkspaceGitError(message: "Could not create the project folder: \(String(cString: strerror(errno)))") }
            }
            guard reserved else { throw WorkspaceGitError(message: "The destination already exists. Choose another name or retry in a new folder.") }
            var record = ProjectCreationRecord(id: UUID().uuidString.lowercased(), destination: destination,
                kind: kind, remote: remote, state: .preparing)
            let recordURL = recordsRoot.appendingPathComponent(record.id + ".json")
            func persist() throws {
                try manager.createDirectory(at: recordsRoot, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
                try JSONEncoder().encode(record).write(to: recordURL, options: .atomic)
                try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: recordURL.path)
            }
            do {
                try persist()
                switch kind {
                case .repository:
                    _ = try WorkspaceGitCommand.text(destination, ["init"], command: command)
                    try Data(("# " + cleanName + "\n").utf8).write(to: destination.appendingPathComponent("README.md"), options: .withoutOverwriting)
                case .clone:
                    guard let remote else { throw WorkspaceGitError(message: "Enter a repository URL.") }
                    _ = try WorkspaceGitCommand.text(parent, ["-c", "protocol.ext.allow=never", "clone", "--", remote, destination.path],
                        command: command, timeout: 1800)
                }
                record.state = .ready
                try persist()
                return destination
            } catch {
                record.state = error is CancellationError ? .cancelled : .failed
                record.failure = error.localizedDescription
                let message = record.state == .cancelled ? "Project creation was cancelled." : error.localizedDescription
                do {
                    // Git can remove its empty destination on early failure.
                    // Retain the reserved location alongside its recovery record.
                    if !manager.fileExists(atPath: destination.path) {
                        try manager.createDirectory(at: destination, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
                    }
                    try persist()
                }
                catch { throw ProjectCreationFailure(message: message + " The recovery record could not be saved: \(error.localizedDescription)", retainedDirectory: destination) }
                throw ProjectCreationFailure(message: message, retainedDirectory: destination)
            }
        }
    }
}
