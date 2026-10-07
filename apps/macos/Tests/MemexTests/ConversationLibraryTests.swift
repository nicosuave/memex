import Foundation
import Testing
@testable import Memex

@Suite @MainActor struct ConversationLibraryTests {
    private func session(_ id: String, machine: String? = nil, source: String = "codex") -> Session {
        Session(source: source, sessionID: id, sourcePath: "/provider/\(id).jsonl", project: "project",
                label: "Provider \(id)", lastAt: "2026-10-05T12:00:00Z", machine: machine)
    }

    @Test func organizationSurvivesRelaunchWithoutChangingProviderHistory() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let transcript = directory.appendingPathComponent("provider.jsonl")
        let evidence = Data("provider owned history\n".utf8)
        try evidence.write(to: transcript)
        let original = Session(source: "codex", sessionID: "native", sourcePath: transcript.path,
                               project: "project", label: "Original title")
        let library = ConversationLibrary(directory: directory)
        #expect(library.rename(original, to: "My title"))
        #expect(library.pin([original], pinned: true))
        #expect(library.archive([original], archived: true))
        #expect(library.remove([original]))
        let reopened = ConversationLibrary(directory: directory)
        #expect(reopened.title(for: original) == "My title")
        #expect(reopened.includes(original, in: .removed))
        #expect(!reopened.includes(original, in: .archived))
        #expect(reopened.savedSession(id: original.id)?.title == "Original title")
        #expect(reopened.restore([original]))
        #expect(reopened.includes(original, in: .active))
        #expect(reopened.isPinned(original))
        #expect(reopened.pin([original], pinned: false))
        #expect(reopened.rename(original, to: "  "))
        let restored = ConversationLibrary(directory: directory)
        #expect(restored.title(for: original) == "Original title")
        #expect(!restored.isPinned(original))
        #expect(try Data(contentsOf: transcript) == evidence)
    }

    @Test func archiveAndUnarchiveDoNotRemoveOrChangePinnedState() {
        let library = ConversationLibrary(), row = session("one")
        #expect(library.pin([row], pinned: true))
        #expect(library.archive([row], archived: true))
        #expect(library.includes(row, in: .archived))
        #expect(!library.includes(row, in: .active))
        #expect(library.archive([row], archived: false))
        #expect(library.includes(row, in: .active))
        #expect(library.isPinned(row))
    }

    @Test func nativeIdentitySeparatesProvidersMachinesAndTranscriptFiles() {
        let library = ConversationLibrary()
        let local = session("same"), remote = session("same", machine: "remote"), claude = session("same", source: "claude")
        let otherHome = Session(source: "codex", sessionID: "same", sourcePath: "/other-home/same.jsonl", project: "project")
        #expect(library.rename(local, to: "Local only"))
        #expect(library.remove([local]))
        for row in [remote, claude, otherHome] {
            #expect(library.includes(row, in: .active))
            #expect(library.title(for: row) == row.title)
        }
    }

    @Test func twoAppInstancesMergeIndependentChangesBeforeWriting() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let first = ConversationLibrary(directory: directory), second = ConversationLibrary(directory: directory)
        let row = session("one")
        #expect(first.rename(row, to: "Renamed"))
        #expect(second.pin([row], pinned: true))
        #expect(first.archive([row], archived: true))
        let reopened = ConversationLibrary(directory: directory)
        #expect(reopened.title(for: row) == "Renamed")
        #expect(reopened.isPinned(row))
        #expect(reopened.includes(row, in: .archived))
    }

    @Test func corruptMetadataIsPreservedAndMutationFailsClosed() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let file = directory.appendingPathComponent("organization.json")
        let corrupt = Data("unreadable metadata".utf8)
        try corrupt.write(to: file)
        let library = ConversationLibrary(directory: directory), row = session("one")
        #expect(library.error != nil)
        #expect(!library.remove([row]))
        #expect(library.includes(row, in: .active))
        #expect(try Data(contentsOf: file) == corrupt)
    }

    @Test func bulkReorderPreservesFilteredRowsAndSearchRank() {
        let library = ConversationLibrary()
        let rows = [session("a"), session("b"), session("c"), session("d")]
        #expect(library.reorder(rows, from: IndexSet(integer: 3), to: 0))
        #expect(library.sorted(rows).map(\.sessionID) == ["d", "a", "b", "c"])
        let visible = [rows[3], rows[1]]
        #expect(library.reorder(visible, from: IndexSet(integer: 1), to: 0))
        #expect(library.sorted(rows).map(\.sessionID) == ["b", "a", "d", "c"])
        #expect(library.sorted(rows, preservingSearchRank: true) == rows)
        #expect(library.pin([rows[2], rows[0]], pinned: true))
        #expect(Set(library.sorted(rows).prefix(2).map(\.id)) == Set([rows[2].id, rows[0].id]))
        #expect(library.remove([rows[0], rows[1]]))
        #expect(rows.filter { library.includes($0, in: .removed) }.count == 2)
        #expect(library.restore([rows[0], rows[1]]))
        #expect(rows.allSatisfy { library.includes($0, in: .active) })
    }

    @Test func storeProjectionPreservesRawEvidenceAndRestoresCachedArchivedRows() {
        let library = ConversationLibrary()
        let store = Store(conversationLibrary: library)
        let row = session("one")
        store.sessions = [row]
        #expect(library.rename(row, to: "Memex title"))
        #expect(store.librarySessions.first?.title == "Memex title")
        #expect(store.sessions.first?.title == "Provider one")
        #expect(library.archive([row], archived: true))
        #expect(store.librarySessions.isEmpty)
        store.sessions = [] // No longer in the backend's latest page.
        store.conversationLibraryScope = .archived
        #expect(store.librarySessions.map(\.id) == [row.id])
        #expect(store.nativeLibrarySession(store.librarySessions[0]) == row)
        store.filters.provider = .claude
        #expect(store.librarySessions.isEmpty)
    }

    @Test func sectionsReadStateAndDeletionPreserveExistingMetadata() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let library = ConversationLibrary(directory: directory)
        let row = session("one")
        #expect(library.createSection(named: "Research"))
        let section = try #require(library.sections.first)
        #expect(library.move([row], toSection: section.id))
        #expect(library.markRead([row], read: false))
        #expect(library.pin([row], pinned: true))
        #expect(library.archive([row], archived: true))
        #expect(library.renameSection(section.id, to: "Reading"))
        let reopened = ConversationLibrary(directory: directory)
        #expect(reopened.sections.first?.name == "Reading")
        #expect(reopened.sectionID(for: row) == section.id)
        #expect(reopened.isUnread(row))
        #expect(reopened.deleteSection(section.id))
        #expect(reopened.sectionID(for: row) == nil)
        #expect(reopened.isPinned(row))
        #expect(reopened.includes(row, in: .archived))
        #expect(reopened.markRead([row], read: true))
        #expect(!ConversationLibrary(directory: directory).isUnread(row))
    }

    @Test func versionOneMetadataMigratesAndTwoWritersPreserveSections() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        let row = session("one")
        let first = ConversationLibrary(directory: directory)
        #expect(first.rename(row, to: "Kept title"))
        let file = directory.appendingPathComponent("organization.json")
        var legacy = try #require(JSONSerialization.jsonObject(with: Data(contentsOf: file)) as? [String: Any])
        legacy["version"] = 1
        legacy.removeValue(forKey: "sections")
        try JSONSerialization.data(withJSONObject: legacy).write(to: file)
        let second = ConversationLibrary(directory: directory)
        #expect(second.sections.isEmpty)
        #expect(!second.isUnread(row))
        #expect(second.title(for: row) == "Kept title")
        #expect(first.createSection(named: "One"))
        #expect(second.createSection(named: "Two"))
        #expect(first.markRead([row], read: false))
        let reopened = ConversationLibrary(directory: directory)
        #expect(reopened.sections.map(\.name) == ["One", "Two"])
        #expect(reopened.isUnread(row))
        #expect(reopened.title(for: row) == "Kept title")
        #expect(reopened.moveSection(reopened.sections[1].id, by: -1))
        #expect(reopened.sections.map(\.name) == ["Two", "One"])
    }

    @Test func nativeFilterIncludesAntigravityAndZcode() {
        #expect(ConversationProvider.antigravity.argument == "antigravity")
        #expect(ConversationProvider.zcode.argument == "zcode")
        #expect(ConversationProvider.allCases.contains(.antigravity))
        #expect(ConversationProvider.allCases.contains(.zcode))
    }
}
