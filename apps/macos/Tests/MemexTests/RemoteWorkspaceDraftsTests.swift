import Foundation
import Testing
@testable import Memex

@MainActor struct RemoteWorkspaceDraftsTests {
    @Test func remoteDraftsPersistAndSeparateOpaqueHostPaths() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("remote-drafts-" + UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let store = RemoteWorkspaceDrafts(directory: root)
        let draft = RemoteWorkspaceDrafts.Draft(text: "unsaved", revision: "host-revision", original: "original")
        try store.save(draft, hostID: "host-a", workspaceID: "/same/path", path: "notes.txt")
        #expect(try RemoteWorkspaceDrafts(directory: root).read(hostID: "host-a", workspaceID: "/same/path", path: "notes.txt") == draft)
        #expect(try store.read(hostID: "host-b", workspaceID: "/same/path", path: "notes.txt") == nil)
        #expect(try store.read(hostID: "host-a", workspaceID: "/other/path", path: "notes.txt") == nil)
        try store.save(.init(text: "original", revision: "host-revision", original: "original"), hostID: "host-a", workspaceID: "/same/path", path: "notes.txt")
        #expect(try store.read(hostID: "host-a", workspaceID: "/same/path", path: "notes.txt") == nil)
    }
}
