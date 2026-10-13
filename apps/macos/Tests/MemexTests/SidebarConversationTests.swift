import Foundation
import Testing
@testable import Memex

@Suite @MainActor struct SidebarConversationTests {
    @Test func layoutModesPreserveSelectionFiltersAndSearchIdentity() throws {
        let name = "Memex-sidebar-test-\(UUID())"
        let preferences = try #require(UserDefaults(suiteName: name))
        defer { preferences.removePersistentDomain(forName: name) }
        let store = Store(filterPreferences: preferences)
        let older = Session(source: "codex", sessionID: "old", sourcePath: "/old", project: "one", lastAt: "2026-10-03T12:00:00Z")
        let newer = Session(source: "claude", sessionID: "new", sourcePath: "/new", project: "two", lastAt: "2026-10-04T12:00:00Z")
        store.filters.timeframe = .day
        store.scope = .all
        store.sessions = [older, newer]
        store.selectedID = older.id
        let request = store.requestID
        #expect(store.sidebarSessions.map(\.id) == [newer.id, older.id])
        #expect(store.sidebarGroups.map(\.name) == ["two", "one"])
        for mode in [Store.SidebarMode.recent, .projects, .recent] {
            store.sidebarMode = mode
            #expect(store.selected == older)
            #expect(store.requestID == request)
            #expect(store.filters.timeframe == .day)
        }
        #expect(Store(filterPreferences: preferences).sidebarMode == .recent)
        store.query = "exact match"
        #expect(store.sidebarSessions == [older, newer])
        #expect(store.sessions == [older, newer])
    }

    @Test func listingPartitionsOneSortIntoPinnedAndProjectGroups() throws {
        let name = "Memex-sidebar-test-\(UUID())"
        let preferences = try #require(UserDefaults(suiteName: name))
        defer { preferences.removePersistentDomain(forName: name) }
        let store = Store(filterPreferences: preferences)
        let sessions = (0..<3).map { Session(source: "codex", sessionID: "s\($0)", sourcePath: "/s\($0)",
                                             project: $0 == 2 ? "two" : "one", lastAt: "2026-10-0\($0 + 1)T12:00:00Z") }
        store.scope = .all
        store.sessions = sessions
        store.sidebarMode = .projects
        _ = store.conversationLibrary.pin([sessions[0]], pinned: true)
        let listing = store.sidebarListing
        #expect(listing.pinned.map(\.id) == [sessions[0].id])
        #expect(listing.unpinned.map(\.id) == [sessions[2].id, sessions[1].id])
        #expect(listing.groups.map(\.name) == ["two", "one"])
        #expect(listing.groups.map(\.count) == [1, 1]) // Loaded rows stand in until project counts arrive.
        store.query = "anything"
        #expect(store.sidebarListing.pinned.isEmpty)
    }

    @Test func savedProjectlessContextGroupsWithoutChangingSourceIdentity() {
        let catalog = CreatedConversationCatalog()
        let store = Store(createdConversations: catalog)
        let session = Session(source: "codex", sessionID: "native-id", sourcePath: "/native/source", project: "files", cwd: "/managed/files")
        let workspace = ConversationWorkspace(id: "workspace", workingDirectory: URL(fileURLWithPath: "/managed/files"),
            sourceDirectory: URL(fileURLWithPath: "/managed/files"), repositoryRoot: nil, baseRef: nil, baseCommit: nil,
            branch: nil, worktreeRoot: nil, metadataURL: nil, state: .ready)
        catalog.save(session, context: .init(projectID: nil, projectName: "No project", workspace: workspace))
        store.sessions = [session]
        #expect(store.sidebarGroups.map(\.name) == ["No project"])
        #expect(store.sidebarGroups.first?.sessions.first == session)
    }
}
