import AppKit
import SwiftUI
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct BrowserToolbarTests {
    @Test func nativeRootKeepsToolbarAndFullHeightSidebarAcrossHostedUpdates() async throws {
        _ = NSApplication.shared
        let store = Store()
        let controller = BrowserColumnsController(store: store, sidebar: Text("Sidebar"),
            conversations: Text("Conversations"), reader: Text("Reader"))
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
        #expect(content.bounds.height >= 700)
        #expect(abs(split.convert(split.bounds, to: nil).maxY - content.convert(content.bounds, to: nil).maxY) < 1)
        let sidebar = try #require(split.arrangedSubviews.first)
        #expect(abs(sidebar.convert(sidebar.bounds, to: nil).maxY - content.convert(content.bounds, to: nil).maxY) < 1)
        let originalSeparators = toolbar.items.compactMap { $0 as? NSTrackingSeparatorToolbarItem }
        #expect(originalSeparators.count == 2)
        #expect(originalSeparators.map(\.dividerIndex) == [0, 1])
        #expect(originalSeparators.allSatisfy { $0.splitView === split })
        var session = Session(source: "codex", sessionID: "native-root", sourcePath: "/fixture", project: "memex")
        store.sessions = [session]
        store.selectedID = session.id
        await pumpNative(window)
        let find = try #require(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.find })
        #expect(find.isEnabled)

        for revision in 1...3 {
            store.query = "query \(revision)"
            controller.sidebarHost.rootView = Text("Sidebar \(revision)")
            controller.conversationsHost.rootView = Text("Conversations \(revision)")
            controller.readerHost.rootView = Text("Reader \(revision)")
            store.loadingSessions = revision.isMultiple(of: 2)
            window.setContentSize(NSSize(width: CGFloat(1200 + revision * 100), height: 700))
            await pumpNative(window)
            #expect(window.toolbar === toolbar)
            let currentSplit = controller.splitView
            #expect(currentSplit === split)
            let separators = toolbar.items.compactMap { $0 as? NSTrackingSeparatorToolbarItem }
            #expect(separators.count == 2)
            #expect(separators.map(\.dividerIndex) == [0, 1])
            #expect(separators.allSatisfy { $0.splitView === currentSplit && $0.splitView.window === window })
            let search = try #require(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.search } as? NSSearchToolbarItem)
            #expect(search.searchField.stringValue == store.query)
            let refresh = try #require(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.refresh })
            #expect(refresh.isEnabled == !store.loadingSessions)
            let filter = try filterView(in: window)
            #expect(filter.frame.width >= 28)
            #expect(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.filters }?.view == nil)
            split.setPosition(CGFloat(200 + revision * 10), ofDividerAt: 0)
            split.setPosition(CGFloat(530 + revision * 20), ofDividerAt: 1)
            await pumpNative(window)
            let title = try #require(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.title }?.view)
            let firstDivider = split.convert(NSPoint(x: split.arrangedSubviews[0].frame.maxX, y: 0), to: nil).x
            let secondDivider = split.convert(NSPoint(x: split.arrangedSubviews[1].frame.maxX, y: 0), to: nil).x
            #expect((0...40).contains(title.convert(title.bounds, to: nil).minX - firstDivider))
            #expect((0...40).contains(secondDivider - filter.convert(filter.bounds, to: nil).maxX))
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
        let reveal = try #require(toolbar.items.first { $0.itemIdentifier == BrowserToolbarController.reveal })
        #expect(!reveal.isEnabled)
    }

    private func pumpNative(_ window: NSWindow) async {
        pump(window)
        // Yield the main executor so deferred Observation callbacks can run.
        try? await Task.sleep(for: .milliseconds(30))
        pump(window)
    }

    @Test func toolbarSectionsFollowBothDividersAndWindowResize() throws {
        let (window, split, controller) = fixture()
        defer { window.close() }
        let separators = controller.toolbar.items.compactMap { $0 as? NSTrackingSeparatorToolbarItem }
        #expect(separators.count == 2)
        #expect(separators.allSatisfy { $0.splitView === split })
        #expect(separators.map(\.dividerIndex) == [0, 1])

        // Native separator views have no public frame API. Measure the actual
        // custom controls immediately inside each tracked boundary instead.
        let title = try #require(item(BrowserToolbarController.title, in: controller).view)
        let filters = try filterView(in: window)
        #expect(filters.frame.width >= 28)
        #expect(try item(BrowserToolbarController.filters, in: controller).view == nil)
        var firstOffsets: (CGFloat, CGFloat)?
        for (width, left, right) in [(1380.0, 210.0, 540.0), (1380, 270, 540),
                                     (1380, 270, 660), (1600, 230, 590), (1200, 200, 530)] {
            window.setContentSize(NSSize(width: width, height: 700))
            split.setPosition(left, ofDividerAt: 0)
            split.setPosition(right, ofDividerAt: 1)
            pump(window)
            let firstDivider = split.convert(NSPoint(x: split.arrangedSubviews[0].frame.maxX, y: 0), to: nil).x
            let secondDivider = split.convert(NSPoint(x: split.arrangedSubviews[1].frame.maxX, y: 0), to: nil).x
            #expect(abs(firstDivider - left) < 1)
            #expect(abs(secondDivider - right) < 1)
            #expect(title.window === window)
            #expect(filters.window === window)
            let titleOffset = title.convert(title.bounds, to: nil).minX - firstDivider
            let filterOffset = secondDivider - filters.convert(filters.bounds, to: nil).maxX
            #expect((0...40).contains(titleOffset))
            #expect((0...40).contains(filterOffset))
            if let firstOffsets {
                #expect(abs(titleOffset - firstOffsets.0) < 1)
                #expect(abs(filterOffset - firstOffsets.1) < 1)
            } else {
                firstOffsets = (titleOffset, filterOffset)
            }
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
        let reveal = try item(BrowserToolbarController.reveal, in: controller)
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

    private func item(_ identifier: NSToolbarItem.Identifier, in controller: BrowserToolbarController) throws -> NSToolbarItem {
        try #require(controller.toolbar.items.first { $0.itemIdentifier == identifier })
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
        for _ in 0..<3 { split.addArrangedSubview(NSView()) }
        window.contentView = split
        split.adjustSubviews()
        let controller = BrowserToolbarController(store: Store(), splitView: split)
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
