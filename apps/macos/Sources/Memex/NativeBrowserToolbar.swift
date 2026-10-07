import AppKit
import Observation
import SwiftUI

/// One native controller owns both the real column dividers and their toolbar.
@MainActor final class BrowserColumnsController<Sidebar: View, Reader: View>: NSSplitViewController {
    let sidebarHost: NSHostingController<Sidebar>
    let readerHost: NSHostingController<Reader>
    let store: Store
    var browserToolbar: BrowserToolbarController?

    init(store: Store, sidebar: Sidebar, reader: Reader) {
        self.store = store
        sidebarHost = NSHostingController(rootView: sidebar)
        readerHost = NSHostingController(rootView: reader)
        super.init(nibName: nil, bundle: nil)
        // The split items and window own sizing. A child's temporary empty
        // state must not impose its preferred or maximum size on the window.
        sidebarHost.sizingOptions = []
        readerHost.sizingOptions = []
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }
    override func viewDidLoad() {
        super.viewDidLoad()
        splitView.isVertical = true
        splitView.dividerStyle = .thin
        splitView.autosaveName = "MemexSidebarReaderColumns"
        let sidebar = NSSplitViewItem(sidebarWithViewController: sidebarHost)
        sidebar.minimumThickness = 260
        sidebar.maximumThickness = 420
        sidebar.canCollapse = true
        let reader = NSSplitViewItem(viewController: readerHost)
        reader.minimumThickness = 400
        addSplitViewItem(sidebar)
        addSplitViewItem(reader)
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

@MainActor final class BrowserToolbarController: NSObject, NSToolbarDelegate, NSSearchFieldDelegate, NSPopoverDelegate, NSMenuDelegate {
    // AppKit synchronizes item changes between toolbars with the same identifier.
    // Each window must switch between Home and the reader independently.
    let toolbar = NSToolbar(identifier: "MemexBrowserColumns-\(UUID().uuidString)")
    let store: Store
    let splitView: NSSplitView
    private lazy var resumeController = ResumeToolbarController(store: store)
    private var actionItems: [NSToolbarItem.Identifier: NSToolbarItem] = [:]
    private var searchItem: NSSearchToolbarItem?
    private var utilityMenu: NSMenu?
    let historyPresentation = ConversationHistoryPresentation()
    private var sidebarObservation: NSKeyValueObservation?
    private(set) var filterPopover: NSPopover?

    static let conversationActions = NSToolbarItem.Identifier("MemexConversationActions")
    static let sidebarBoundary = NSToolbarItem.Identifier("MemexSidebarBoundary")
    static let title = NSToolbarItem.Identifier("MemexConversationTitle")
    static let filters = NSToolbarItem.Identifier("MemexFilters")
    static let refresh = NSToolbarItem.Identifier("MemexRefresh")
    static let find = NSToolbarItem.Identifier("MemexFind")
    static let more = NSToolbarItem.Identifier("MemexMore")
    static let resume = NSToolbarItem.Identifier("MemexResume")
    static let search = NSToolbarItem.Identifier("MemexSearch")
    static let newConversation = NSToolbarItem.Identifier("MemexNewConversation")
    static let workspaceChanges = NSToolbarItem.Identifier("MemexWorkspaceChanges")

    init(store: Store, splitView: NSSplitView) {
        self.store = store
        self.splitView = splitView
        super.init()
        toolbar.delegate = self
        toolbar.displayMode = .iconOnly
        toolbar.allowsUserCustomization = false
        toolbar.autosavesConfiguration = false
        if let sidebar = (splitView.delegate as? NSSplitViewController)?.splitViewItems.first {
            sidebarObservation = sidebar.observe(\.isCollapsed) { [weak self] _, _ in
                DispatchQueue.main.async { self?.update() }
            }
        }
        observeStore()
    }

    private var sidebarVisible: Bool {
        if let sidebar = (splitView.delegate as? NSSplitViewController)?.splitViewItems.first {
            return !sidebar.isCollapsed
        }
        return splitView.arrangedSubviews.first.map { !splitView.isSubviewCollapsed($0) } ?? false
    }

    private func observeStore() {
        withObservationTracking {
            _ = store.selected
            _ = store.loadingSessions
            _ = store.query
            _ = store.filters
            _ = store.scope
            _ = store.loadingSessionMetadata
            _ = store.sessionMetadataError
            _ = store.showingWorkspaceChanges
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
            return [.toggleSidebar, Self.sidebarBoundary, Self.newConversation, .flexibleSpace]
        }
        let sidebarItems: [NSToolbarItem.Identifier] = sidebarVisible
            ? [.toggleSidebar, .flexibleSpace, Self.filters] : [.toggleSidebar]
        return sidebarItems + [Self.sidebarBoundary, Self.title, .flexibleSpace, Self.search, Self.resume, Self.conversationActions]
    }
    func toolbarAllowedItemIdentifiers(_ toolbar: NSToolbar) -> [NSToolbarItem.Identifier] {
        toolbarDefaultItemIdentifiers(toolbar)
    }
    func toolbar(_ toolbar: NSToolbar, itemForItemIdentifier id: NSToolbarItem.Identifier,
                 willBeInsertedIntoToolbar flag: Bool) -> NSToolbarItem? {
        switch id {
        case Self.conversationActions:
            let group = NSToolbarItemGroup(itemIdentifier: id)
            group.label = "Conversation controls"
            group.isBordered = true
            group.autovalidates = false
            group.controlRepresentation = .expanded
            group.subitems = [Self.newConversation, Self.refresh, Self.find, Self.workspaceChanges, Self.more].compactMap {
                self.toolbar(toolbar, itemForItemIdentifier: $0, willBeInsertedIntoToolbar: flag)
            }
            return group
        case Self.sidebarBoundary:
            return NSTrackingSeparatorToolbarItem(identifier: id, splitView: splitView, dividerIndex: 0)
        case Self.title:
            let item = NSToolbarItem(itemIdentifier: id)
            let title = NSHostingView(rootView: ConversationToolbarTitle(store: store, historyPresentation: historyPresentation))
            title.setContentHuggingPriority(.defaultLow, for: .horizontal)
            title.setContentCompressionResistancePriority(.defaultLow, for: .horizontal)
            item.view = title
            item.isBordered = false
            item.label = "Conversations"
            actionItems[id] = item
            return item
        case Self.filters:
            return action(id, title: "Filter conversations", symbol: "line.3.horizontal.decrease", selector: #selector(toggleFilters))
        case Self.resume:
            let item = NSToolbarItem(itemIdentifier: id)
            item.view = resumeController.control
            item.label = "Resume"
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
        case Self.more:
            let item = NSMenuToolbarItem(itemIdentifier: id)
            item.label = "More conversation actions"
            item.toolTip = item.label
            item.image = NSImage(systemSymbolName: "ellipsis", accessibilityDescription: item.label)
            item.isBordered = true
            item.showsIndicator = false
            item.autovalidates = false
            let menu = NSMenu(title: item.label)
            menu.autoenablesItems = false
            menu.delegate = self
            for (title, symbol, selector) in [
                ("Copy session ID", "link", #selector(copySessionID)),
                ("Reveal source", "doc", #selector(revealSource))
            ] {
                let action = NSMenuItem(title: title, action: selector, keyEquivalent: "")
                action.target = self
                action.image = NSImage(systemSymbolName: symbol, accessibilityDescription: title)
                menu.addItem(action)
            }
            item.menu = menu
            utilityMenu = menu
            actionItems[id] = item
            return item
        case Self.newConversation: return action(id, title: "New conversation (⌘N)", symbol: "square.and.pencil", selector: #selector(newConversation))
        case Self.workspaceChanges: return action(id, title: "Workspace panel", symbol: "sidebar.right", selector: #selector(toggleWorkspaceChanges))
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

        actionItems[Self.title]?.label = store.selected?.title ?? "Chats"
        actionItems[Self.title]?.toolTip = Self.titleHelp(store: store)
        actionItems[Self.refresh]?.isEnabled = !store.loadingSessions
        resumeController.update()
        actionItems[Self.find]?.isEnabled = store.selected != nil
        actionItems[Self.more]?.isEnabled = store.selected != nil
        for item in utilityMenu?.items.prefix(2) ?? [] {
            item.isEnabled = item.action == #selector(revealSource)
                ? store.selected?.machineID == "local" : store.selected != nil
        }
        actionItems[Self.newConversation]?.isEnabled = InAppAgentRuntime.isAvailable
        actionItems[Self.workspaceChanges]?.isEnabled = store.selected != nil
        actionItems[Self.workspaceChanges]?.toolTip = store.showingWorkspaceChanges ? "Hide workspace panel" : "Show workspace panel"
        if let field = searchItem?.searchField, field.stringValue != store.query { field.stringValue = store.query }
    }
    static func titleHelp(store: Store) -> String {
        guard let session = store.selected else { return "Chats" }
        return [session.title, session.source, store.projectName(for: session),
                session.machineID == "local" ? nil : session.machineID,
                store.createdConversations.contexts[session.id]?.workspace.branch]
            .compactMap { $0 }.joined(separator: " · ")
    }

    func menuNeedsUpdate(_ menu: NSMenu) {
        guard menu === utilityMenu else { return }
        while menu.items.count > 2 { menu.removeItem(at: 2) }
        guard let session = store.selected else { return }
        menu.addItem(.separator())
        let enabled = !store.historyActionInProgress && !store.conversationRelationships.pending.contains { $0.source.id == session.id }
        func add(_ title: String, _ selector: Selector, enabled: Bool, session: Session? = nil, to menu: NSMenu) {
            let item = NSMenuItem(title: title, action: selector, keyEquivalent: "")
            item.target = self
            item.isEnabled = enabled
            item.representedObject = session
            menu.addItem(item)
        }
        if let parent = store.conversationRelationships.parent(of: session.id) {
            add("Open parent: \(parent.parent.title)", #selector(openRelated(_:)), enabled: enabled, session: parent.parent, to: menu)
        }
        let children = store.conversationRelationships.children(of: session.id)
        if !children.isEmpty {
            let branches = NSMenu(title: "Branches")
            branches.autoenablesItems = false
            for child in children {
                add(child.child.title, #selector(openRelated(_:)), enabled: enabled, session: child.child, to: branches)
            }
            let item = NSMenuItem(title: "Branches (\(children.count))", action: nil, keyEquivalent: "")
            item.submenu = branches
            item.isEnabled = enabled
            menu.addItem(item)
        }
        add("Branch with context…", #selector(branchWithContext), enabled: enabled, to: menu)
        let canMutate = enabled && store.selectedLiveConversation?.canMutateHistory == true && !store.selectedHistoryBoundaries.isEmpty
        add("Fork native history…", #selector(forkHistory), enabled: canMutate, to: menu)
        add("Rewind conversation…", #selector(rewindHistory), enabled: canMutate, to: menu)
        if store.conversationRelationships.parent(of: session.id) != nil {
            add("Add context to parent draft", #selector(addContextToParent), enabled: enabled, to: menu)
        }
    }
    @objc func openRelated(_ sender: NSMenuItem) {
        guard let session = sender.representedObject as? Session else { return }
        store.openRelatedConversation(session)
    }
    @objc func branchWithContext() { historyPresentation.showingBranch = true }
    @objc func forkHistory() { historyPresentation.begin(.fork, boundaries: store.selectedHistoryBoundaries) }
    @objc func rewindHistory() { historyPresentation.begin(.revert, boundaries: store.selectedHistoryBoundaries) }
    @objc func addContextToParent() {
        guard let session = store.selected else { return }
        store.historyActionError = nil
        Task {
            do { try await store.mergeContextToParent(from: session) }
            catch { store.historyActionError = error.localizedDescription }
        }
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
        guard sidebarVisible, store.scope != .home,
              let item = actionItems[Self.filters], splitView.window != nil else { return }
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
    @objc func newConversation() { store.beginNewConversation() }
    @objc func toggleWorkspaceChanges() { store.toggleWorkspacePanel() }
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
    @Bindable var historyPresentation: ConversationHistoryPresentation

    var body: some View {
        HStack(alignment: .firstTextBaseline, spacing: 8) {
            Text(store.selected?.title ?? (store.scope == .home ? "Home" : "Chats"))
                .font(.headline).lineLimit(1).truncationMode(.tail)
                .help(BrowserToolbarController.titleHelp(store: store))
            if store.selected == nil && store.scope != .home {
                Text(store.sessionCountLabel)
                    .font(.subheadline).foregroundStyle(.secondary).monospacedDigit().lineLimit(1)
                    .fixedSize(horizontal: true, vertical: false)
                    .help(store.sessionCountHelp)
            }
        }
        .frame(minWidth: 70, idealWidth: 260, maxWidth: 400, alignment: .leading)
        .background {
            if let session = store.selected {
                ConversationHistoryActions(store: store, session: session,
                                           presentation: historyPresentation, showsStatus: false)
                    .id(session.id)
            }
        }
    }
}
