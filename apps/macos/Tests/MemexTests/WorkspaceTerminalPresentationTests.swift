import AppKit
import SwiftUI
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct WorkspaceTerminalPresentationTests {
    @Test func terminalMovesBetweenPaneAndDrawerWithoutDuplicatingItsPresentation() {
        let store = Store()
        store.scope = .all
        let session = Session(source: "codex", sessionID: "terminal-presentation", sourcePath: "/fixture",
                              project: "fixture", cwd: "/tmp/fixture", machine: "local")
        store.sessions = [session]
        store.selectedID = session.id
        store.toggleTerminalDrawer()
        #expect(store.showingTerminalDrawer)
        #expect(!store.showingWorkspaceChanges)
        store.showWorkspaceBrowser()
        #expect(store.showingTerminalDrawer && store.showingWorkspaceChanges)
        #expect(store.workspacePanel == .browser)

        store.selectWorkspacePanel(.terminal)
        #expect(!store.showingTerminalDrawer && store.showingWorkspaceChanges)
        #expect(store.workspacePanel == .terminal)
        let focus = store.terminalFocusRequest
        store.toggleTerminalDrawer()
        #expect(store.showingTerminalDrawer && !store.showingWorkspaceChanges)
        #expect(store.terminalFocusRequest > focus)
        // Reopening the right pane moves its selected terminal back there.
        store.toggleWorkspacePanel()
        #expect(!store.showingTerminalDrawer && store.showingWorkspaceChanges)
        #expect(store.workspacePanel == .terminal)
        store.toggleTerminalDrawer()
        store.toggleTerminalDrawer()
        #expect(!store.showingTerminalDrawer && !store.showingWorkspaceChanges)
        #expect(store.workspaceTerminals.sessions.isEmpty)
    }

    @Test func remoteAndMissingWorkspacesCannotLaunchLocalTerminals() {
        let store = Store()
        store.scope = .all
        let remote = Session(source: "codex", sessionID: "remote-terminal", sourcePath: "/fixture",
                             project: "fixture", cwd: "/tmp/fixture", machine: "nicbook-atm")
        let missing = Session(source: "codex", sessionID: "missing-workspace", sourcePath: "/fixture", project: "fixture")
        store.sessions = [remote, missing]
        for session in store.sessions {
            store.selectedID = session.id
            store.showWorkspaceTerminal()
            store.toggleTerminalDrawer()
            #expect(!store.showingWorkspaceChanges && !store.showingTerminalDrawer)
            #expect(store.workspaceTerminals.sessions.isEmpty)
        }
        // A drawer left open while switching to a remote chat can still hide.
        store.showingTerminalDrawer = true
        store.toggleTerminalDrawer()
        #expect(!store.showingTerminalDrawer)
    }

    @Test func drawerResizesTheExistingReaderWithoutRemountingIt() async throws {
        _ = NSApplication.shared
        let store = Store()
        let host = NSHostingView(rootView: WorkspaceTerminalDrawer(store: store) { ReaderProbe() })
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 700),
                              styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = host
        defer { window.close() }
        func probe(in view: NSView) -> ReaderProbeView? {
            if let view = view as? ReaderProbeView { return view }
            return view.subviews.lazy.compactMap { probe(in: $0) }.first
        }
        host.layoutSubtreeIfNeeded()
        let reader = try #require(probe(in: host))
        let fullHeight = reader.bounds.height
        store.showingTerminalDrawer = true
        try await Task.sleep(for: .milliseconds(30))
        host.layoutSubtreeIfNeeded()
        #expect(probe(in: host) === reader)
        #expect(reader.bounds.height >= 180)
        #expect(reader.bounds.height < fullHeight)
        window.setContentSize(NSSize(width: 700, height: 400))
        host.layoutSubtreeIfNeeded()
        #expect(reader.bounds.height >= 180)
        #expect(reader.bounds.height < 400)
        store.showingTerminalDrawer = false
        try await Task.sleep(for: .milliseconds(30))
        host.layoutSubtreeIfNeeded()
        #expect(probe(in: host) === reader)
        #expect(reader.bounds.height == 400)
        #expect(!window.isVisible)
    }
}

private struct ReaderProbe: NSViewRepresentable {
    func makeNSView(context: Context) -> ReaderProbeView { ReaderProbeView() }
    func updateNSView(_ view: ReaderProbeView, context: Context) {}
}

private final class ReaderProbeView: NSView {}
