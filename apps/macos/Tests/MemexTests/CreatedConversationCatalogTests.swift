import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor struct CreatedConversationCatalogTests {
    private func session(_ id: String = "native-created") -> Session {
        Session(source: "codex", sessionID: id, sourcePath: "/native/sessions/\(id).jsonl", project: "project",
                label: "Build a new feature", lastAt: "2026-10-04T12:00:00Z", cwd: "/tmp/project", conversationKind: "main")
    }

    @Test func relaunchKeepsProviderIdentityWithoutWritingTranscript() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let catalog = CreatedConversationCatalog(directory: directory)
        let native = session()
        catalog.save(native)
        let reopened = CreatedConversationCatalog(directory: directory)
        #expect(reopened.sessions == [native])
        #expect(reopened.sessions.first?.id == native.id)
        let files = try FileManager.default.contentsOfDirectory(atPath: directory.path)
        #expect(files == ["conversations.json"])
        let permissions = try FileManager.default.attributesOfItem(atPath: directory.appendingPathComponent("conversations.json").path)
        #expect(permissions[.posixPermissions] as? Int == 0o600)
        #expect(reopened.error == nil)
    }

    @Test func corruptCatalogIsPreserved() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let file = directory.appendingPathComponent("conversations.json")
        let original = Data("unreadable original catalog".utf8)
        try original.write(to: file)
        let catalog = CreatedConversationCatalog(directory: directory)
        #expect(catalog.error != nil)
        catalog.save(session())
        catalog.retrySave()
        #expect(try Data(contentsOf: file) == original)
    }

    @Test func indexOwnsRefreshedMetadataAndSearchAnchors() {
        let catalog = CreatedConversationCatalog()
        catalog.save(session())
        var indexed = session()
        indexed.label = "Provider generated title"
        indexed.messageCount = 12
        indexed.lastAt = "2026-10-04T13:00:00Z"
        indexed.snippet = "exact match"
        indexed.searchRecordID = "source-record"
        let rows = catalog.merging([indexed], machines: ["local"], project: nil, filters: .defaults,
                                   query: "exact", since: nil, limit: 200)
        #expect(rows == [indexed])
        #expect(rows.first?.id == session().id)
        #expect(rows.first?.searchRecordID == "source-record")
    }

    @Test func catalogRespectsLocalProjectProviderTimeAndSearchFilters() {
        let catalog = CreatedConversationCatalog()
        catalog.save(session())
        func matching(machines: [String] = ["local"], project: String? = nil,
                      filters: ConversationFilters = .defaults, query: String? = nil, since: String? = nil) -> [Session] {
            catalog.merging([], machines: machines, project: project, filters: filters, query: query, since: since, limit: 200)
        }
        #expect(matching().count == 1)
        #expect(matching(machines: ["remote"]).isEmpty)
        #expect(matching(project: "other").isEmpty)
        var filters = ConversationFilters.defaults
        filters.provider = .claude
        #expect(matching(filters: filters).isEmpty)
        filters.provider = .all
        filters.origin = .subagent
        #expect(matching(filters: filters).isEmpty)
        #expect(matching(since: "2026-10-05T00:00:00Z").isEmpty)
        #expect(matching(query: "NEW FEATURE").count == 1)
        #expect(matching(query: "absent").isEmpty)
    }

    @Test func newlyCreatedChatSurvivesRefreshBeforeIndexing() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let executable = directory.appendingPathComponent("fixture-cli")
        try "#!/bin/sh\nprintf '[]'\n".write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        let catalog = CreatedConversationCatalog()
        catalog.save(session())
        let store = Store(client: MemexClient(executable: executable), createdConversations: catalog)
        store.scope = .all
        await store.loadSessions()
        #expect(store.sessions == [session()])
        #expect(store.selectedID == session().id)
        await store.loadSelectedSessionMetadata()
        #expect(store.sessionMetadataError == nil)
        await store.loadSessions()
        #expect(store.sessions == [session()])
        #expect(store.selectedID == session().id)
        store.query = "absent"
        await store.loadSessions()
        #expect(store.sessions.isEmpty)
        #expect(store.selectedID == nil)
    }
}
