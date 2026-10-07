import AppKit
import SwiftUI
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct BrowserToolbarTests {
    @Test func closingSidebarHidesFilterAndReopeningPreservesFilters() async throws {
        let store = Store()
        store.scope = .all
        store.filters.timeframe = .day
        let columns = BrowserColumnsController(store: store, sidebar: Text("Sidebar"), reader: Text("Reader"))
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1200, height: 700),
                              styleMask: [.titled, .closable, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = columns
        window.setContentSize(NSSize(width: 1200, height: 700))
        columns.splitView.autosaveName = nil
        defer { window.close() }
        window.orderBack(nil)
        await pumpNative(window)
        let toolbar = try #require(columns.browserToolbar)
        let sidebar = try #require(columns.splitViewItems.first)
        sidebar.isCollapsed = false
        await pumpNative(window)
        #expect(toolbar.toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.filters })
        sidebar.isCollapsed = true
        await pumpNative(window)
        #expect(!toolbar.toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.filters })
        toolbar.toggleFilters()
        #expect(toolbar.filterPopover == nil)
        #expect(store.filters.timeframe == .day)
        sidebar.isCollapsed = false
        await pumpNative(window)
        #expect(toolbar.toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.filters })
        #expect(store.filters.timeframe == .day)
        store.scope = .home
        await pumpNative(window)
        #expect(!toolbar.toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.filters })
    }

    @Test func newAndWorkspaceActionsUseCurrentLocalSelection() throws {
        let (window, _, controller) = fixture()
        defer { window.close() }
        let new = try item(BrowserToolbarController.newConversation, in: controller)
        #expect(NSApplication.shared.sendAction(try #require(new.action), to: new.target, from: new))
        #expect(controller.store.scope == .home)
        #expect(controller.store.newConversationDraft.focusRequest == 1)
        controller.store.scope = .all
        let changes = try item(BrowserToolbarController.workspaceChanges, in: controller)
        #expect(!changes.isEnabled)
        var session = Session(source: "codex", sessionID: "local-workspace", sourcePath: "/fixture", project: "project", cwd: "/tmp/project")
        controller.store.sessions = [session]
        controller.store.selectedID = session.id
        controller.update()
        #expect(changes.isEnabled)
        #expect(NSApplication.shared.sendAction(try #require(changes.action), to: changes.target, from: changes))
        #expect(controller.store.showingWorkspaceChanges)
        session.machine = "remote"
        controller.store.sessions = [session]
        controller.store.selectedID = session.id
        controller.update()
        #expect(changes.isEnabled) // Remote chats can still use the browser pane.
        controller.store.scope = .home
        controller.update()
        #expect(controller.toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.newConversation })
    }

    @Test func resumeUsesNativeToolbarSegmentsAndTracksSelection() throws {
        let (window, _, controller) = fixture()
        defer { window.close() }
        let resume = try item(BrowserToolbarController.resume, in: controller)
        let control = try #require(resume.view as? NSSegmentedControl)
        #expect(control.isHidden)
        #expect(!control.isEnabled)

        let session = Session(source: "codex", sessionID: "native-resume", sourcePath: "/fixture", project: "memex",
                              resumeCommand: "codex resume native-resume", cwd: "/tmp")
        controller.store.sessions = [session]
        controller.store.selectedID = session.id
        controller.update()
        pump(window)
        #expect(resume.view === control)
        #expect(control.window === window)
        #expect(!control.isHidden)
        #expect(control.isEnabled(forSegment: 0))
        #expect(control.isEnabled(forSegment: 1))
        #expect(control.menu(forSegment: 1)?.items.isEmpty == false)

        controller.store.selectedID = nil
        controller.update()
        #expect(control.isHidden)
        #expect(!control.isEnabled)
    }

    @Test func nativeRootKeepsToolbarAndFullHeightSidebarAcrossHostedUpdates() async throws {
        _ = NSApplication.shared
        let store = Store()
        store.scope = .all
        let controller = BrowserColumnsController(store: store, sidebar: Text("Sidebar"), reader: Text("Reader"))
        let window = NSWindow(contentRect: NSRect(x: -10000, y: -10000, width: 1380, height: 700),
                              styleMask: [.titled, .closable, .resizable, .fullSizeContentView], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.alphaValue = 0
        window.contentViewController = controller
        window.setContentSize(NSSize(width: 1380, height: 700))
        // Do not let a fixture overwrite the user's saved column positions.
        controller.splitView.autosaveName = nil
        defer { window.close() }
        // Exercise the actual native root and real appearance callbacks; do
        // not manually install the toolbar or invoke viewDidAppear in this test.
        window.orderBack(nil)
        await pumpNative(window)
        let toolbar = try #require(window.toolbar)
        let split = controller.splitView
        let content = try #require(window.contentView)
        // AppKit can constrain the requested window size to the runner's display.
        // The full-size content must still reach the top of the actual window.
        #expect(content.bounds.height > 0)
        #expect(abs(content.convert(content.bounds, to: nil).maxY - window.frame.height) < 1)
        #expect(abs(split.convert(split.bounds, to: nil).maxY - content.convert(content.bounds, to: nil).maxY) < 1)
        let sidebar = try #require(split.arrangedSubviews.first)
        #expect(abs(sidebar.convert(sidebar.bounds, to: nil).maxY - content.convert(content.bounds, to: nil).maxY) < 1)
        let originalSeparators = toolbar.items.compactMap { $0 as? NSTrackingSeparatorToolbarItem }
        #expect(originalSeparators.count == 1)
        #expect(originalSeparators.map(\.dividerIndex) == [0])
        #expect(originalSeparators.allSatisfy { $0.splitView === split })
        var session = Session(source: "codex", sessionID: "native-root", sourcePath: "/fixture", project: "memex")
        store.sessions = [session]
        store.selectedID = session.id
        await pumpNative(window)
        let find = try #require(allItems(toolbar).first { $0.itemIdentifier == BrowserToolbarController.find })
        #expect(find.isEnabled)

        for revision in 1...3 {
            store.query = "query \(revision)"
            controller.sidebarHost.rootView = Text("Sidebar \(revision)")
            controller.readerHost.rootView = Text("Reader \(revision)")
            store.loadingSessions = revision.isMultiple(of: 2)
            window.setContentSize(NSSize(width: CGFloat(1200 + revision * 100), height: 700))
            await pumpNative(window)
            #expect(window.toolbar === toolbar)
            let currentSplit = controller.splitView
            #expect(currentSplit === split)
            let separators = toolbar.items.compactMap { $0 as? NSTrackingSeparatorToolbarItem }
            #expect(separators.count == 1)
            #expect(separators.map(\.dividerIndex) == [0])
            #expect(separators.allSatisfy { $0.splitView === currentSplit && $0.splitView.window === window })
            let search = try #require(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.search } as? NSSearchToolbarItem)
            #expect(search.searchField.stringValue == store.query)
            let refresh = try #require(allItems(toolbar).first { $0.itemIdentifier == BrowserToolbarController.refresh })
            #expect(refresh.isEnabled == !store.loadingSessions)
            let filter = try filterView(in: window)
            #expect(filter.frame.width >= 28)
            #expect(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.filters }?.view == nil)
            split.setPosition(CGFloat(290 + revision * 10), ofDividerAt: 0)
            await pumpNative(window)
            let title = try #require(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.title }?.view)
            let firstDivider = split.convert(NSPoint(x: split.arrangedSubviews[0].frame.maxX, y: 0), to: nil).x
            #expect((0...40).contains(title.convert(title.bounds, to: nil).minX - firstDivider))
            #expect((0...40).contains(firstDivider - filter.convert(filter.bounds, to: nil).maxX))
            #expect(abs(sidebar.convert(sidebar.bounds, to: nil).maxY - content.convert(content.bounds, to: nil).maxY) < 1)
        }
        // Replacing selected metadata must also invalidate native action state.
        session.machine = "remote-fixture"
        store.sessions = [session]
        await pumpNative(window)
        #expect(!find.isEnabled)
        store.selectedID = session.id
        await pumpNative(window)
        #expect(find.isEnabled)
        let more = try #require(allItems(toolbar).first { $0.itemIdentifier == BrowserToolbarController.more } as? NSMenuToolbarItem)
        let reveal = try #require(more.menu.items.first { $0.action == #selector(BrowserToolbarController.revealSource) })
        #expect(!reveal.isEnabled)
    }

    private func pumpNative(_ window: NSWindow) async {
        pump(window)
        // Yield the main executor so deferred Observation callbacks can run.
        try? await Task.sleep(for: .milliseconds(30))
        pump(window)
    }

    @Test func toolbarTracksTheSidebarDividerThroughWindowResize() throws {
        let (window, split, controller) = fixture()
        defer { window.close() }
        let separators = controller.toolbar.items.compactMap { $0 as? NSTrackingSeparatorToolbarItem }
        #expect(separators.count == 1)
        #expect(separators.allSatisfy { $0.splitView === split && $0.dividerIndex == 0 })
        let title = try #require(item(BrowserToolbarController.title, in: controller).view)
        let filters = try filterView(in: window)
        var firstOffset: CGFloat?
        for (width, left) in [(1380.0, 300.0), (1380, 360), (1600, 320), (1200, 300)] {
            window.setContentSize(NSSize(width: width, height: 700))
            split.setPosition(left, ofDividerAt: 0)
            pump(window)
            let divider = split.convert(NSPoint(x: split.arrangedSubviews[0].frame.maxX, y: 0), to: nil).x
            #expect(abs(divider - left) < 1)
            #expect(title.window === window && filters.window === window)
            #expect((0...40).contains(title.convert(title.bounds, to: nil).minX - divider))
            let offset = divider - filters.convert(filters.bounds, to: nil).maxX
            #expect((0...40).contains(offset))
            if let firstOffset { #expect(abs(offset - firstOffset) < 1) }
            else { firstOffset = offset }
        }
        #expect(!window.isVisible)
    }

    @Test func searchAndFindUseCurrentStoreAndActionsFollowSelection() throws {
        let (window, _, controller) = fixture()
        defer { window.close() }
        let store = controller.store
        let search = try #require(item(BrowserToolbarController.search, in: controller) as? NSSearchToolbarItem)
        search.searchField.stringValue = "divider regression"
        controller.controlTextDidChange(Notification(name: NSControl.textDidChangeNotification, object: search.searchField))
        #expect(store.query == "divider regression")
        store.query = "replacement query"
        controller.update()
        #expect(search.searchField.stringValue == "replacement query")
        let find = try item(BrowserToolbarController.find, in: controller)
        let more = try #require(item(BrowserToolbarController.more, in: controller) as? NSMenuToolbarItem)
        let reveal = try #require(more.menu.items.first { $0.action == #selector(BrowserToolbarController.revealSource) })
        #expect(!find.isEnabled)
        #expect(!reveal.isEnabled)
        var session = Session(source: "codex", sessionID: "toolbar-fixture", sourcePath: "/fixture", project: "memex")
        store.sessions = [session]
        store.selectedID = session.id
        controller.update()
        #expect(find.isEnabled)
        #expect(reveal.isEnabled)
        let action = try #require(find.action)
        #expect(NSApplication.shared.sendAction(action, to: find.target, from: find))
        #expect(store.findConversationRequest == 1)
        session.machine = "remote-fixture"
        store.sessions = [session]
        store.selectedID = session.id
        store.loadingSessions = true
        controller.update()
        #expect(find.isEnabled)
        #expect(!reveal.isEnabled)
        #expect(try !item(BrowserToolbarController.refresh, in: controller).isEnabled)
    }

    @Test func utilityMenuKeepsActionsAndTracksLocalRemoteAndEmptySelection() throws {
        let (window, _, controller) = fixture()
        defer { window.close() }
        let more = try #require(item(BrowserToolbarController.more, in: controller) as? NSMenuToolbarItem)
        let copy = try #require(more.menu.items.first { $0.action == #selector(BrowserToolbarController.copySessionID) })
        let reveal = try #require(more.menu.items.first { $0.action == #selector(BrowserToolbarController.revealSource) })
        #expect(more.menu.items.map(\.title) == ["Copy session ID", "Reveal source"])
        #expect(copy.target === controller)
        #expect(reveal.target === controller)
        #expect(!more.isEnabled && !copy.isEnabled && !reveal.isEnabled)

        var session = Session(source: "codex", sessionID: "menu-fixture", sourcePath: "/fixture", project: "memex")
        for machine in ["local", "nicbook-atm"] {
            session.machine = machine
            controller.store.sessions = [session]
            controller.store.selectedID = session.id
            controller.update()
            more.menu.update()
            #expect(more.isEnabled && copy.isEnabled)
            #expect(reveal.isEnabled == (machine == "local"))
        }
        controller.store.selectedID = nil
        controller.update()
        more.menu.update()
        #expect(!more.isEnabled && !copy.isEnabled && !reveal.isEnabled)
    }

    @Test func nativeFilterPopoverUsesAccentWhileOpenAndResetsOnClose() async throws {
        let (window, _, controller) = fixture()
        defer { window.close() }
        window.alphaValue = 0
        window.orderBack(nil)
        await pumpNative(window)
        controller.toggleFilters()
        await pumpNative(window)
        #expect(controller.filterPopover?.isShown == true)
        let filter = try item(BrowserToolbarController.filters, in: controller)
        if #available(macOS 26.0, *) { #expect(filter.style == .prominent) }
        controller.filterPopover?.performClose(nil)
        let deadline = Date().addingTimeInterval(2)
        while controller.filterPopover != nil && Date() < deadline { await pumpNative(window) }
        #expect(controller.filterPopover == nil)
        if #available(macOS 26.0, *) { #expect(filter.style == .plain) }
        controller.store.filters.timeframe = .day
        await pumpNative(window)
        if #available(macOS 26.0, *) { #expect(filter.style == .prominent) }
    }

    private func filterView(in window: NSWindow) throws -> NSView {
        func find(in view: NSView) -> NSView? {
            if view.toolTip == "Filter conversations" { return view }
            for child in view.subviews {
                if let match = find(in: child) { return match }
            }
            return nil
        }
        let frame = try #require(window.contentView?.superview)
        return try #require(find(in: frame))
    }

    private func allItems(_ toolbar: NSToolbar) -> [NSToolbarItem] {
        toolbar.items.flatMap { item in
            if let group = item as? NSToolbarItemGroup { return [item] + group.subitems }
            return [item]
        }
    }

    @Test func trailingNativeGroupKeepsEllipsisLastAndHistoryActionsAvailable() async throws {
        let (window, _, controller) = fixture()
        defer { window.close() }
        let group = try #require(controller.toolbar.items.last as? NSToolbarItemGroup)
        #expect(group.subitems.map(\.itemIdentifier) == [BrowserToolbarController.newConversation,
            BrowserToolbarController.refresh, BrowserToolbarController.find,
            BrowserToolbarController.workspaceChanges, BrowserToolbarController.more])
        let session = Session(source: "codex", sessionID: "group-title", sourcePath: "/fixture",
                              project: "memex", label: "Selected chat title")
        controller.store.sessions = [session]
        controller.store.selectedID = session.id
        controller.update()
        window.alphaValue = 0
        window.orderBack(nil)
        await pumpNative(window)
        #expect(try item(BrowserToolbarController.title, in: controller).label == session.title)
        let more = try #require(group.subitems.last as? NSMenuToolbarItem)
        controller.menuNeedsUpdate(more.menu)
        let branch = try #require(more.menu.items.first { $0.title == "Branch with context…" })
        #expect(branch.isEnabled)
        #expect(NSApplication.shared.sendAction(try #require(branch.action), to: branch.target, from: branch))
        #expect(controller.historyPresentation.showingBranch)
        await pumpNative(window)
        #expect(window.attachedSheet != nil)
        controller.historyPresentation.showingBranch = false
        await pumpNative(window)
        #expect(more.menu.items.first { $0.title == "Fork native history…" }?.isEnabled == false)
        #expect(more.menu.items.first { $0.title == "Rewind conversation…" }?.isEnabled == false)
    }

    private func item(_ identifier: NSToolbarItem.Identifier, in controller: BrowserToolbarController) throws -> NSToolbarItem {
        try #require(allItems(controller.toolbar).first { $0.itemIdentifier == identifier })
    }

    private func fixture() -> (NSWindow, NSSplitView, BrowserToolbarController) {
        _ = NSApplication.shared
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1380, height: 700),
                              styleMask: [.titled, .closable, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        let split = NSSplitView(frame: NSRect(x: 0, y: 0, width: 1380, height: 700))
        split.isVertical = true
        split.dividerStyle = .thin
        split.autoresizingMask = [.width, .height]
        for _ in 0..<2 { split.addArrangedSubview(NSView()) }
        window.contentView = split
        split.adjustSubviews()
        let store = Store()
        store.scope = .all
        let controller = BrowserToolbarController(store: store, splitView: split)
        window.toolbar = controller.toolbar
        window.toolbarStyle = .unified
        window.titleVisibility = .hidden
        controller.update()
        pump(window)
        return (window, split, controller)
    }

    private func pump(_ window: NSWindow) {
        window.contentView?.superview?.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        let deadline = Date().addingTimeInterval(0.08)
        while Date() < deadline { RunLoop.main.run(mode: .default, before: deadline) }
        window.contentView?.superview?.layoutSubtreeIfNeeded()
    }
}
