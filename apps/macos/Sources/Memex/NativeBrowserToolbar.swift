import AppKit
import Observation
import SwiftUI

/// One native controller owns both the real column dividers and their toolbar.
@MainActor final class BrowserColumnsController<Sidebar: View, Conversations: View, Reader: View>: NSSplitViewController {
    let sidebarHost: NSHostingController<Sidebar>
    let conversationsHost: NSHostingController<Conversations>
    let readerHost: NSHostingController<Reader>
    let store: Store
    var browserToolbar: BrowserToolbarController?

    init(store: Store, sidebar: Sidebar, conversations: Conversations, reader: Reader) {
        self.store = store
        sidebarHost = NSHostingController(rootView: sidebar)
        conversationsHost = NSHostingController(rootView: conversations)
        readerHost = NSHostingController(rootView: reader)
        super.init(nibName: nil, bundle: nil)
        // The split items and window own sizing. A child's temporary empty
        // state must not impose its preferred or maximum size on the window.
        sidebarHost.sizingOptions = []
        conversationsHost.sizingOptions = []
        readerHost.sizingOptions = []
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }
    override func viewDidLoad() {
        super.viewDidLoad()
        splitView.isVertical = true
        splitView.dividerStyle = .thin
        splitView.autosaveName = "MemexBrowserColumns"
        let sidebar = NSSplitViewItem(sidebarWithViewController: sidebarHost)
        sidebar.minimumThickness = 180
        sidebar.maximumThickness = 290
        sidebar.canCollapse = true
        let conversations = NSSplitViewItem(viewController: conversationsHost)
        conversations.minimumThickness = 260
        conversations.maximumThickness = 450
        conversations.holdingPriority = .defaultHigh
        conversations.canCollapse = true
        let reader = NSSplitViewItem(viewController: readerHost)
        reader.minimumThickness = 400
        addSplitViewItem(sidebar)
        addSplitViewItem(conversations)
        addSplitViewItem(reader)
        conversations.isCollapsed = store.scope == .home
    }
    override func viewDidAppear() {
        super.viewDidAppear()
        attachToolbar()
    }
    override func viewDidLayout() {
        super.viewDidLayout()
        attachToolbar()
    }
    private func attachToolbar() {
        guard let window = view.window else { return }
        if browserToolbar == nil { browserToolbar = BrowserToolbarController(store: store, splitView: splitView) }
        if window.toolbar !== browserToolbar?.toolbar {
            window.toolbar = browserToolbar?.toolbar
            window.toolbarStyle = .unified
            window.titleVisibility = .hidden
            browserToolbar?.update()
        }
    }
}

@MainActor final class BrowserToolbarController: NSObject, NSToolbarDelegate, NSSearchFieldDelegate, NSPopoverDelegate {
    // AppKit synchronizes item changes between toolbars with the same identifier.
    // Each window must switch between Home and the reader independently.
    let toolbar = NSToolbar(identifier: "MemexBrowserColumns-\(UUID().uuidString)")
    let store: Store
    let splitView: NSSplitView
    private var actionItems: [NSToolbarItem.Identifier: NSToolbarItem] = [:]
    private var searchItem: NSSearchToolbarItem?
    private(set) var filterPopover: NSPopover?

    static let sidebarBoundary = NSToolbarItem.Identifier("MemexSidebarBoundary")
    static let readerBoundary = NSToolbarItem.Identifier("MemexReaderBoundary")
    static let title = NSToolbarItem.Identifier("MemexConversationTitle")
    static let filters = NSToolbarItem.Identifier("MemexFilters")
    static let refresh = NSToolbarItem.Identifier("MemexRefresh")
    static let find = NSToolbarItem.Identifier("MemexFind")
    static let copyID = NSToolbarItem.Identifier("MemexCopyID")
    static let reveal = NSToolbarItem.Identifier("MemexReveal")
    static let resume = NSToolbarItem.Identifier("MemexResume")
    static let search = NSToolbarItem.Identifier("MemexSearch")

    init(store: Store, splitView: NSSplitView) {
        self.store = store
        self.splitView = splitView
        super.init()
        toolbar.delegate = self
        toolbar.displayMode = .iconOnly
        toolbar.allowsUserCustomization = false
        toolbar.autosavesConfiguration = false
        observeStore()
    }

    private func observeStore() {
        withObservationTracking {
            _ = store.selected
            _ = store.loadingSessions
            _ = store.query
            _ = store.filters
            _ = store.scope
        } onChange: { [weak self] in
            // Observation fires before the mutation; read the completed state
            // on the next main-loop turn, then subscribe to subsequent changes.
            DispatchQueue.main.async { [weak self] in
                self?.update()
                self?.observeStore()
            }
        }
    }

    func toolbarDefaultItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        if store.scope == .home {
            return [.toggleSidebar, Self.sidebarBoundary, .flexibleSpace]
        }
        return [.toggleSidebar, Self.sidebarBoundary, Self.title, .flexibleSpace, Self.filters,
         Self.readerBoundary, Self.refresh, Self.find, Self.copyID, Self.reveal,
         .flexibleSpace, Self.resume, .flexibleSpace, Self.search]
    }
    func toolbarAllowedItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        toolbarDefaultItemIdentifiers(toolbar)
    }
    func toolbar(_ toolbar: NSToolbar, itemForItemIdentifier id: NSToolbarItem.Identifier,
                 willBeInsertedIntoToolbar flag: Bool) -> NSToolbarItem? {
        switch id {
        case Self.sidebarBoundary:
            return NSTrackingSeparatorToolbarItem(identifier: id, splitView: splitView, dividerIndex: 0)
        case Self.readerBoundary:
            return NSTrackingSeparatorToolbarItem(identifier: id, splitView: splitView, dividerIndex: 1)
        case Self.title:
            let item = NSToolbarItem(itemIdentifier: id)
            let title = NSHostingView(rootView: ConversationToolbarTitle(store: store))
            title.setContentHuggingPriority(.defaultLow, for: .horizontal)
            title.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
            item.view = title
            item.isBordered = false
            item.label = "Conversations"
            return item
        case Self.filters:
            return action(id, title: "Filter conversations", symbol: "line.3.horizontal.decrease", selector: #selector(toggleFilters))
        case Self.resume:
            let item = NSToolbarItem(itemIdentifier: id)
            item.view = NSHostingView(rootView: ResumeToolbarButton(store: store).frame(minWidth: 1, minHeight: 32))
            item.label = "Resume"
            item.isBordered = false
            return item
        case Self.search:
            let item = NSSearchToolbarItem(itemIdentifier: id)
            item.searchField.placeholderString = "Search conversations"
            item.searchField.delegate = self
            item.searchField.sendsSearchStringImmediately = true
            item.searchField.stringValue = store.query
            item.searchField.setAccessibilityLabel("Search conversations")
            item.preferredWidthForSearchField = 280
            searchItem = item
            return item
        case Self.refresh: return action(id, title: "Refresh", symbol: "arrow.clockwise", selector: #selector(refresh))
        case Self.find: return action(id, title: "Find in conversation", symbol: "magnifyingglass", selector: #selector(find))
        case Self.copyID: return action(id, title: "Copy session ID", symbol: "link", selector: #selector(copySessionID))
        case Self.reveal: return action(id, title: "Reveal source", symbol: "doc", selector: #selector(revealSource))
        case .toggleSidebar:
            let item = action(id, title: "Toggle sidebar", symbol: "sidebar.left", selector: #selector(toggleSidebar))
            item.isNavigational = true
            return item
        default: return NSToolbarItem(itemIdentifier: id)
        }
    }
    private func action(_ id: NSToolbarItem.Identifier, title: String, symbol: String, selector: Selector) -> NSToolbarItem {
        let item = NSToolbarItem(itemIdentifier: id)
        item.label = title
        item.toolTip = title
        item.image = NSImage(systemSymbolName: symbol, accessibilityDescription: title)
        item.target = self
        item.action = selector
        item.autovalidates = false
        item.isBordered = true
        actionItems[id] = item
        return item
    }
    func update() {
        if let columns = splitView.delegate as? NSSplitViewController, columns.splitViewItems.count == 3 {
            let collapsed = store.scope == .home
            if columns.splitViewItems[1].isCollapsed != collapsed {
                columns.splitViewItems[1].isCollapsed = collapsed
            }
        }
        let identifiers = toolbarDefaultItemIdentifiers(toolbar)
        if toolbar.items.map(\.itemIdentifier) != identifiers {
            filterPopover?.performClose(nil)
            while !toolbar.items.isEmpty { toolbar.removeItem(at: toolbar.items.count - 1) }
            for (index, identifier) in identifiers.enumerated() {
                toolbar.insertItem(withItemIdentifier: identifier, at: index)
            }
        }
        if #available(macOS 26.0, *) {
            actionItems[Self.filters]?.style = filterPopover?.isShown == true || store.filters.isActive ? .prominent : .plain
        }

        actionItems[Self.refresh]?.isEnabled = !store.loadingSessions
        actionItems[Self.find]?.isEnabled = store.selected != nil
        actionItems[Self.copyID]?.isEnabled = store.selected != nil
        actionItems[Self.reveal]?.isEnabled = store.selected?.machineID == "local"
        if let field = searchItem?.searchField, field.stringValue != store.query { field.stringValue = store.query }
    }
    func controlTextDidChange(_ notification: Notification) {
        guard let field = notification.object as? NSSearchField else { return }
        store.query = field.stringValue
    }
    @objc func toggleFilters() {
        if let popover = filterPopover, popover.isShown {
            popover.performClose(nil)
            return
        }
        guard let item = actionItems[Self.filters], splitView.window != nil else { return }
        let popover = NSPopover()
        popover.behavior = .transient
        popover.delegate = self
        popover.contentViewController = NSHostingController(rootView: ConversationFilterControls(store: store) { [weak popover] in
            popover?.performClose(nil)
        })
        filterPopover = popover
        popover.show(relativeTo: item)
        update()
    }
    func popoverDidClose(_ notification: Notification) {
        filterPopover = nil
        update()
    }
    @objc func refresh() { Task { await store.refresh() } }
    @objc func find() { store.findConversationRequest += 1 }
    @objc func copySessionID() {
        guard let session = store.selected else { return }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(session.sessionID, forType: .string)
    }
    @objc func revealSource() {
        guard let session = store.selected, session.machineID == "local" else { return }
        NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: session.sourcePath)])
    }
    @objc func toggleSidebar() {
        (splitView.delegate as? NSSplitViewController)?.toggleSidebar(nil)
    }
}

private struct ConversationToolbarTitle: View {
    @Bindable var store: Store
    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            Text(store.scope.title).font(.headline).lineLimit(1)
            if store.scope != .home {
                Text(store.sessionCountLabel)
                    .font(.subheadline).foregroundStyle(.secondary).monospacedDigit().lineLimit(1)
                    .fixedSize(horizontal: true, vertical: false)
                    .help(store.sessionCountHelp)
            }
        }
        .frame(minWidth: 70, idealWidth: 190, maxWidth: 240, alignment: .leading)
    }
}
