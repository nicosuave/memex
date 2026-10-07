import Foundation
import Testing
@testable import Memex

@Test func workspaceEditorRejectsConcurrentChangesAndPreservesBothVersions() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
    defer { try? FileManager.default.removeItem(at: root) }
    let file = root.appendingPathComponent("file.txt")
    try Data("original".utf8).write(to: file)
    let client = WorkspaceFilesClient()
    let original = try await client.read(root: root, path: "file.txt")
    try Data("agent's changes".utf8).write(to: file)
    await #expect(throws: WorkspaceFileError.self) {
        try await client.save(root: root, path: "file.txt", contents: "editor's changes", expectedFingerprint: original.fingerprint)
    }
    #expect(try String(contentsOf: file, encoding: .utf8) == "agent's changes")
    let latest = try await client.read(root: root, path: "file.txt")
    let saved = try await client.save(root: root, path: "file.txt", contents: "merged changes", expectedFingerprint: latest.fingerprint)
    #expect(saved.contents == "merged changes")
    #expect(try String(contentsOf: file, encoding: .utf8) == "merged changes")
}

@Test func workspaceFilesRejectTraversalAndSymlinkWrites() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    let workspace = root.appendingPathComponent("workspace")
    try FileManager.default.createDirectory(at: workspace, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: root) }
    let outside = root.appendingPathComponent("outside.txt")
    try Data("private".utf8).write(to: outside)
    try FileManager.default.createSymbolicLink(at: workspace.appendingPathComponent("link.txt"), withDestinationURL: outside)
    let client = WorkspaceFilesClient()
    await #expect(throws: (any Error).self) { try await client.read(root: workspace, path: "../outside.txt") }
    await #expect(throws: (any Error).self) { try await client.read(root: workspace, path: "link.txt") }
    let entries = try await client.entries(root: workspace)
    #expect(entries.count == 1 && entries[0].isSymbolicLink)
    #expect(try String(contentsOf: outside, encoding: .utf8) == "private")
}

@MainActor @Test func workspaceFileDraftSurvivesRelaunchWithOriginalConflictVersion() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    let storage = root.appendingPathComponent("drafts")
    let original = WorkspaceFileRevision(contents: "old", fingerprint: "original-fingerprint")
    try WorkspaceFileDrafts(storage: storage).save(root: root, path: "file.txt", original: original, edited: "unsaved")
    let relaunched = WorkspaceFileDrafts(storage: storage)
    let draft = try #require(try relaunched.read(root: root, path: "file.txt"))
    #expect(draft.original == "old" && draft.edited == "unsaved")
    #expect(draft.expectedFingerprint == "original-fingerprint")
    try relaunched.discard(root: root, path: "file.txt")
    #expect(try relaunched.read(root: root, path: "file.txt") == nil)
}

@Test func fileCreationIsExclusiveAndDelimitedPreviewRetainsQuotedCells() async throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false)
    defer { try? FileManager.default.removeItem(at: root) }
    let client = WorkspaceFilesClient()
    _ = try await client.create(root: root, folder: "", name: "folder", isDirectory: true)
    _ = try await client.create(root: root, folder: "folder", name: "file.txt", isDirectory: false)
    await #expect(throws: (any Error).self) { try await client.create(root: root, folder: "folder", name: "file.txt", isDirectory: false) }
    let csv = "name,value\r\n\"two, words\",\"a\"\"b\"\r\n\"multiple\nlines\",end\r\n"
    #expect(WorkspaceDelimitedRows.parse(csv, delimiter: ",") == [["name", "value"], ["two, words", "a\"b"], ["multiple\nlines", "end"]])
    #expect(WorkspaceDelimitedRows.parse(csv, delimiter: ",", limit: 1) == [["name", "value"]])
}
