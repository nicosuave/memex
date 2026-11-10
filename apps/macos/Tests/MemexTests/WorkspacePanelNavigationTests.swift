import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct WorkspacePanelNavigationTests {
    private func session(_ id: String) -> Session {
        Session(source: "codex", sessionID: id, sourcePath: "/fixture/\(id)",
                project: "fixture", cwd: "/tmp", machine: "local")
    }

    @Test func eachSessionStartsAtTheGridAndRetainsItsOwnOpenTools() {
        let store = Store()
        let first = session("first"), second = session("second")
        store.sessions = [first, second]
        store.scope = .all
        store.selectedID = first.id
        store.toggleWorkspacePanel()
        #expect(store.showingWorkspaceChanges)
        #expect(store.workspacePanel == .tools)
        #expect(store.openWorkspacePanels.isEmpty)
        #expect(store.workspaceTerminals.sessions.isEmpty)

        store.selectWorkspacePanel(.files)
        store.showWorkspaceBrowser()
        store.selectWorkspacePanel(.files)
        #expect(store.openWorkspacePanels == [.files, .browser])
        store.selectWorkspacePanel(.tools)
        #expect(store.openWorkspacePanels == [.files, .browser])

        store.selectedID = second.id
        #expect(store.workspacePanel == .tools)
        #expect(store.openWorkspacePanels.isEmpty)
        store.reviewWorkspaceChange("second.txt")
        #expect(store.workspacePanel == .changes)
        #expect(store.openWorkspacePanels == [.changes])

        store.selectedID = first.id
        #expect(store.workspacePanel == .tools)
        #expect(store.openWorkspacePanels == [.files, .browser])
        store.selectWorkspacePanel(.browser)
        store.toggleWorkspacePanel()
        store.toggleWorkspacePanel()
        #expect(store.workspacePanel == .browser)
        #expect(store.openWorkspacePanels == [.files, .browser])

        store.selectedID = second.id
        #expect(store.workspacePanel == .changes)
        #expect(store.openWorkspacePanels == [.changes])
    }

    @Test func closingTabsPreservesSelectionAndReturnsToTheGridAfterTheLastTool() {
        let store = Store()
        let row = session("close")
        store.sessions = [row]
        store.scope = .all
        store.selectedID = row.id
        store.selectWorkspacePanel(.files)
        store.selectWorkspacePanel(.changes)
        store.selectWorkspacePanel(.browser)
        store.closeWorkspacePanel(.changes)
        #expect(store.workspacePanel == .browser)
        #expect(store.openWorkspacePanels == [.files, .browser])
        store.closeWorkspacePanel(.browser)
        #expect(store.workspacePanel == .files)
        store.selectWorkspacePanel(.tools)
        store.closeWorkspacePanel(.files)
        #expect(store.workspacePanel == .tools)
        #expect(store.openWorkspacePanels.isEmpty)

        store.selectWorkspacePanel(.browser)
        store.closeWorkspacePanel(.browser)
        #expect(store.workspacePanel == .tools)
        #expect(store.openWorkspacePanels.isEmpty)
        store.closeWorkspacePanel(.browser)
        #expect(store.workspacePanel == .tools)
    }
}
