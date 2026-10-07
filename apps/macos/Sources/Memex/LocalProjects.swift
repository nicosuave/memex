import Foundation
import Observation

enum ConversationWorkspaceMode: String, Codable, CaseIterable, Sendable {
    case existingDirectory, newWorktree
}

struct LocalProject: Identifiable, Codable, Hashable, Sendable {
    let id: String
    var name: String
    var directoryPath: String
    var defaultWorkspace: ConversationWorkspaceMode
    var defaultBaseRef: String?
    var setupCommand: String? = nil

    var directory: URL { URL(fileURLWithPath: directoryPath, isDirectory: true) }
}

struct LocalProjectError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}

/// Saved local folders are independent of indexed conversation project labels.
/// Catalog writes are atomic; unreadable catalogs are never replaced.
@MainActor @Observable
final class LocalProjects {
    private(set) var projects: [LocalProject] = []
    private(set) var error: String?
    @ObservationIgnored private let directory: URL?
    @ObservationIgnored private var canWrite = true

    private struct Saved: Codable {
        let version: Int
        let projects: [LocalProject]
    }

    init(directory: URL? = nil) {
        self.directory = directory
        guard let directory else { return }
        do {
            let data = try Data(contentsOf: directory.appendingPathComponent("projects.json"))
            let saved = try JSONDecoder().decode(Saved.self, from: data)
            guard saved.version == 1,
                  Set(saved.projects.map(\.id)).count == saved.projects.count,
                  Set(saved.projects.map(\.directoryPath)).count == saved.projects.count,
                  saved.projects.allSatisfy({ !$0.id.isEmpty && $0.name.nilIfBlank != nil
                      && $0.directoryPath.hasPrefix("/") && !$0.directoryPath.contains("\0") }) else {
                throw CocoaError(.fileReadCorruptFile)
            }
            // Unmounted or moved directories stay listed so the user can repair them.
            projects = saved.projects
        } catch let failure as CocoaError where failure.code == .fileReadNoSuchFile {
            let file = directory.appendingPathComponent("projects.json")
            if (try? FileManager.default.destinationOfSymbolicLink(atPath: file.path)) != nil {
                canWrite = false
                self.error = "The saved project catalog is an unreadable symbolic link. It has been preserved."
            }
        } catch {
            canWrite = false
            self.error = "Saved projects could not be read. The existing project catalog has been preserved."
        }
    }

    static func persistent() -> LocalProjects {
        LocalProjects(directory: FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/dev.memex.app/Projects", isDirectory: true))
    }

    @discardableResult
    func save(name: String, directory: URL, defaultWorkspace: ConversationWorkspaceMode = .existingDirectory,
              defaultBaseRef: String? = nil, setupCommand: String? = nil) throws -> LocalProject {
        let path = try Self.validatedDirectory(directory).path
        let project = LocalProject(id: projects.first(where: { $0.directoryPath == path })?.id ?? UUID().uuidString,
            name: try Self.validatedName(name), directoryPath: path,
            defaultWorkspace: defaultWorkspace, defaultBaseRef: defaultBaseRef?.nilIfBlank, setupCommand: setupCommand?.nilIfBlank)
        var updated = projects
        if let index = updated.firstIndex(where: { $0.id == project.id }) { updated[index] = project }
        else { updated.append(project) }
        try persist(updated)
        return project
    }

    func update(_ project: LocalProject) throws {
        guard let index = projects.firstIndex(where: { $0.id == project.id }) else {
            throw LocalProjectError(message: "This saved project is no longer in the list.")
        }
        var project = project
        guard project.directoryPath.hasPrefix("/"), !project.directoryPath.contains("\0") else {
            throw LocalProjectError(message: "Choose a local folder with an absolute path.")
        }
        project.directoryPath = try Self.validatedDirectory(project.directory).path
        project.name = try Self.validatedName(project.name)
        project.defaultBaseRef = project.defaultBaseRef?.nilIfBlank
        guard !projects.contains(where: { $0.id != project.id && $0.directoryPath == project.directoryPath }) else {
            throw LocalProjectError(message: "This folder is already saved as another project.")
        }
        var updated = projects
        updated[index] = project
        try persist(updated)
    }

    /// Removes only the saved shortcut; never changes the project or its worktrees.
    func remove(id: String) throws { try persist(projects.filter { $0.id != id }) }

    nonisolated static func validatedDirectory(_ directory: URL) throws -> URL {
        guard directory.isFileURL, directory.path.hasPrefix("/"), !directory.path.contains("\0") else {
            throw LocalProjectError(message: "Choose a local folder with an absolute path.")
        }
        let canonical = directory.standardizedFileURL.resolvingSymlinksInPath()
        let values = try canonical.resourceValues(forKeys: [.isDirectoryKey, .isReadableKey])
        guard values.isDirectory == true, values.isReadable == true else {
            throw LocalProjectError(message: "The project folder is not readable: \(canonical.path)")
        }
        return canonical
    }

    private static func validatedName(_ name: String) throws -> String {
        guard let name = name.nilIfBlank, !name.contains("\0") else {
            throw LocalProjectError(message: "Enter a name for the project.")
        }
        return name.trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private func persist(_ updated: [LocalProject]) throws {
        guard canWrite else { throw LocalProjectError(message: error ?? "The project catalog cannot be updated.") }
        do {
            if let directory {
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                    attributes: [.posixPermissions: 0o700])
                let file = directory.appendingPathComponent("projects.json")
                try JSONEncoder().encode(Saved(version: 1, projects: updated)).write(to: file, options: .atomic)
                try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
            }
            projects = updated
            error = nil
        } catch {
            self.error = "The project could not be saved: \(error.localizedDescription)"
            throw LocalProjectError(message: self.error!)
        }
    }
}
