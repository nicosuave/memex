import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor struct LocalProjectsTests {
    private func directory() throws -> URL {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-projects-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
        return root
    }

    @Test func canonicalFoldersDeduplicateAndRetainIdentityAcrossRelaunch() throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let project = root.appendingPathComponent("project")
        let link = root.appendingPathComponent("linked-project")
        try FileManager.default.createDirectory(at: project, withIntermediateDirectories: false)
        try FileManager.default.createSymbolicLink(at: link, withDestinationURL: project)
        let catalog = LocalProjects(directory: root.appendingPathComponent("catalog"))
        let first = try catalog.save(name: "Project", directory: project)
        let changed = try catalog.save(name: "Renamed", directory: link,
            defaultWorkspace: .newWorktree, defaultBaseRef: "release")
        #expect(first.id == changed.id)
        #expect(catalog.projects == [changed])
        #expect(changed.directory == project.resolvingSymlinksInPath())
        let reopened = LocalProjects(directory: root.appendingPathComponent("catalog"))
        #expect(reopened.projects == [changed])
        #expect(reopened.error == nil)
        try reopened.remove(id: changed.id)
        #expect(LocalProjects(directory: root.appendingPathComponent("catalog")).projects.isEmpty)
        #expect(FileManager.default.fileExists(atPath: project.path))
    }

    @Test func invalidDirectoriesAndNamesDoNotChangeCatalog() throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let catalog = LocalProjects()
        #expect(throws: (any Error).self) { try catalog.save(name: "Missing", directory: root.appendingPathComponent("missing")) }
        #expect(throws: (any Error).self) { try catalog.save(name: "  ", directory: root) }
        let file = root.appendingPathComponent("file")
        try Data().write(to: file)
        #expect(throws: (any Error).self) { try catalog.save(name: "File", directory: file) }
        #expect(catalog.projects.isEmpty)
    }

    @Test func corruptAndUnreadableCatalogsArePreserved() throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let file = root.appendingPathComponent("projects.json")
        let bytes = Data("user-owned unreadable catalog".utf8)
        try bytes.write(to: file)
        let catalog = LocalProjects(directory: root)
        #expect(catalog.error != nil)
        #expect(throws: LocalProjectError.self) { try catalog.save(name: "Project", directory: root) }
        #expect(try Data(contentsOf: file) == bytes)
        // A directory at the file location is an unreadable store, not a new store.
        let blocked = root.appendingPathComponent("blocked")
        try FileManager.default.createDirectory(at: blocked.appendingPathComponent("projects.json"), withIntermediateDirectories: true)
        let unreadable = LocalProjects(directory: blocked)
        #expect(unreadable.error != nil)
        #expect(throws: LocalProjectError.self) { try unreadable.remove(id: "missing") }
        let linked = root.appendingPathComponent("linked")
        try FileManager.default.createDirectory(at: linked, withIntermediateDirectories: false)
        let link = linked.appendingPathComponent("projects.json")
        let target = root.appendingPathComponent("absent-catalog")
        try FileManager.default.createSymbolicLink(at: link, withDestinationURL: target)
        let dangling = LocalProjects(directory: linked)
        #expect(dangling.error != nil)
        #expect(throws: LocalProjectError.self) { try dangling.save(name: "Project", directory: root) }
        #expect(try FileManager.default.destinationOfSymbolicLink(atPath: link.path) == target.path)
    }

    @Test func saveFailureDoesNotInstallAnUnsavedProject() throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let catalogPath = root.appendingPathComponent("catalog")
        let catalog = LocalProjects(directory: catalogPath)
        try Data("blocking file".utf8).write(to: catalogPath)
        #expect(throws: LocalProjectError.self) { try catalog.save(name: "Project", directory: root) }
        #expect(catalog.projects.isEmpty)
        #expect(catalog.error != nil)
        #expect(try String(contentsOf: catalogPath, encoding: .utf8) == "blocking file")
    }

    @Test func movedProjectIsRetainedAndCanBeRepaired() throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let old = root.appendingPathComponent("old")
        let new = root.appendingPathComponent("new")
        try FileManager.default.createDirectory(at: old, withIntermediateDirectories: false)
        let catalogPath = root.appendingPathComponent("catalog")
        let saved = try LocalProjects(directory: catalogPath).save(name: "Project", directory: old)
        try FileManager.default.moveItem(at: old, to: new)
        let reopened = LocalProjects(directory: catalogPath)
        #expect(reopened.projects == [saved])
        var repaired = saved
        repaired.directoryPath = new.path
        try reopened.update(repaired)
        #expect(reopened.projects.first?.id == saved.id)
        #expect(reopened.projects.first?.directory == new.resolvingSymlinksInPath())
    }
}
