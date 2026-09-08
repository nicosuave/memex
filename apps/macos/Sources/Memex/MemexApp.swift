import AppKit
import SwiftUI

@main
struct MemexApp: App {
    @NSApplicationDelegateAdaptor(MemexApplicationDelegate.self) private var delegate

    var body: some Scene {
        Settings { EmptyView() }
            .commands {
                CommandGroup(replacing: .newItem) {
                    Button("Open Memex") { delegate.showBrowser() }
                        .keyboardShortcut("n")
                }
                CommandGroup(after: .newItem) {
                    Button("Refresh Conversations") { Task { await delegate.store.refresh() } }
                        .keyboardShortcut("r")
                    Button("Find in Conversation") { delegate.store.findConversationRequest += 1 }
                        .keyboardShortcut("f")
                        .disabled(delegate.store.selected == nil)
                }
            }
    }
}

@MainActor final class MemexApplicationDelegate: NSObject, NSApplicationDelegate {
    let store = Store(filterPreferences: .standard)
    private var browser: NSWindowController?

    func applicationDidFinishLaunching(_ notification: Notification) { showBrowser() }
    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        showBrowser()
        return false
    }
    func showBrowser() {
        if browser == nil {
            // Own the window and toolbar together. Replacing a SwiftUI WindowGroup's
            // toolbar leaves its private toolbar observations attached to old items.
            let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1380, height: 900),
                                  styleMask: [.titled, .closable, .miniaturizable, .resizable, .fullSizeContentView],
                                  backing: .buffered, defer: false)
            window.title = "Memex"
            window.minSize = NSSize(width: 900, height: 560)
            window.isReleasedWhenClosed = false
            let content = BrowserContent(store: store)
            window.contentViewController = BrowserColumnsController(store: store, sidebar: content.sidebar,
                conversations: content.conversations, reader: content.reader)
            // Installing a native content controller adopts its fitting size.
            // Restore the intended initial browser size before frame autosave.
            window.setContentSize(NSSize(width: 1380, height: 900))
            window.center()
            window.setFrameAutosaveName("MemexBrowser")
            browser = NSWindowController(window: window)
        }
        browser?.showWindow(nil)
        browser?.window?.makeKeyAndOrderFront(nil)
    }
}

@MainActor struct BrowserContent {
    let store: Store
    var sidebar: some View { BrowserSidebar(store: store) }
    var conversations: some View { BrowserConversationList(store: store) }
    var reader: some View { BrowserReader(store: store) }
}

private struct BrowserReader: View {
    @Bindable var store: Store

    var body: some View {
        ReaderView(store: store)
        .task(id: store.requestID) { await store.loadSessions() }
        .task(id: store.sessionCountRequestID) { await store.loadSessionCount() }
        .task(id: store.readerRequestID) { await store.loadRecords() }
        .task(id: store.readerRequestID) { await store.loadSelectedSessionMetadata() }
        .task { await store.loadMachines() }
        .task(id: store.machineRequestID) { await store.loadProjects() }
        .onChange(of: store.scope) { _, _ in store.sessionLimit = 200 }
        .onChange(of: store.machineSelection) { _, _ in store.sessionLimit = 200 }
    }
}

private struct BrowserSidebar: View {
    @Bindable var store: Store

    var body: some View { sidebar }

    private var sidebar: some View {
        VStack(spacing: 0) {
            sidebarList.clipped()
            Divider()
            HStack(spacing: 6) {
                Picker("Machines", selection: $store.machineSelection) {
                    Text("All Machines").tag(MachineSelection.all)
                    ForEach(store.machines) { machine in
                        Text(machine.label).tag(MachineSelection.machine(machine.id))
                    }
                }
                .labelsHidden().pickerStyle(.menu)
                .accessibilityLabel("Machines")
                if store.loadingMachines { ProgressView().controlSize(.mini) }
                if let error = store.machineError {
                    Button { Task { await store.loadMachines() } } label: {
                        Image(systemName: "exclamationmark.triangle")
                    }
                    .buttonStyle(.plain).help(error)
                    .accessibilityLabel("Retry loading machines")
                }
            }
                .frame(maxWidth: .infinity, alignment: .center)
                .padding(.horizontal, 16).padding(.vertical, 12)
                .fixedSize(horizontal: false, vertical: true)
                .background(Color(nsColor: .windowBackgroundColor))
        }
    }

    private var sidebarList: some View {
        List(selection: $store.scope) {
            Section {
                Label("All conversations", systemImage: "bubble.left.and.bubble.right")
                    .tag(Store.Scope.all)
            }
            Section {
                ForEach(store.projects) { project in
                    HStack {
                        Label(project.project, systemImage: "folder").lineLimit(1)
                        Spacer(minLength: 4)
                        Text(project.sessionCount, format: .number)
                            .font(.caption).monospacedDigit().foregroundStyle(.secondary)
                    }
                    .tag(Store.Scope.project(project.project))
                    .help("\(project.project): \(project.sessionCount) conversations across all time, excluding permission reviews")
                }
            } header: {
                HStack {
                    Text("Projects")
                    Spacer()
                    if store.loadingProjects { ProgressView().controlSize(.mini) }
                    if let error = store.projectsError {
                        Button { Task { await store.loadProjects() } } label: {
                            Image(systemName: "exclamationmark.triangle")
                        }
                        .buttonStyle(.plain).help(error)
                        .accessibilityLabel("Retry loading projects")
                    }
                    Menu {
                        Picker("Sort by", selection: Binding(get: { store.projectSort }, set: { store.setProjectSort($0) })) {
                            ForEach(ProjectSort.allCases, id: \.self) { sort in
                                Text(sort.title).tag(sort)
                            }
                        }
                        .pickerStyle(.inline)
                        Divider()
                        Button("Refresh projects") { Task { await store.loadProjects() } }
                            .disabled(store.loadingProjects)
                    } label: {
                        Image(systemName: "ellipsis")
                    }
                    .menuStyle(.borderlessButton).menuIndicator(.hidden).fixedSize()
                    .padding(.trailing, 8)
                    .help("Sort projects").accessibilityLabel("Sort projects")
                }
            }

        }
        .listStyle(.sidebar)
    }

}

private struct BrowserConversationList: View {
    @Bindable var store: Store

    var body: some View {
        VStack(spacing: 0) {
            if let error = store.listError {
                ErrorBanner(message: error) { Task { await store.loadSessions() } }
            }
            List(selection: $store.selectedID) {
                ForEach(store.sessions) { session in
                    SessionRow(session: session)
                        .tag(session.id)
                        .padding(.vertical, 6)
                        .onAppear { store.loadMoreSessionsIfNeeded(visibleID: session.id) }
                }
            }
            .listStyle(.inset)
            .overlay {
                if store.sessions.isEmpty && !store.loadingSessions && store.listError == nil {
                    ContentUnavailableView {
                        Label(store.filters.isActive ? "No matching conversations" : (store.query.isEmpty ? "No conversations yet" : "No matches"),
                              systemImage: "bubble.left.and.bubble.right")
                    } description: {
                        Text(store.filters.isActive ? "Try another timeframe, provider, or conversation type." :
                             (store.query.isEmpty ? "Run memex index to index your local history, then refresh." : "Try different words or another project."))
                    } actions: {
                        if store.filters.isActive { Button("Reset Filters") { store.filters = .defaults } }
                    }
                }
            }

        }
    }
}

struct SessionRow: View {
    let session: Session
    var body: some View {
        VStack(alignment: .leading, spacing: 5) {
            HStack(alignment: .firstTextBaseline) {
                Text(session.projectName).font(.caption).foregroundStyle(.secondary).lineLimit(1)
                Spacer(minLength: 4)
                if let date = session.date {
                    Text(date, format: .dateTime.month(.abbreviated).day())
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
            Text(session.title).font(.system(size: 13, weight: .semibold)).lineLimit(2)
            Text(session.snippet?.nilIfBlank ?? session.source)
                .font(.system(size: 12)).foregroundStyle(.secondary).lineLimit(2)
            if session.machineID != "local" {
                Label(session.machineID, systemImage: "desktopcomputer")
                    .font(.caption).foregroundStyle(.secondary).lineLimit(1)
            }
        }
        .padding(.vertical, 3)
        .accessibilityElement(children: .combine)
    }
}

struct ErrorBanner: View {
    let message: String
    let retry: () -> Void
    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Label("Couldn’t load conversations", systemImage: "exclamationmark.triangle")
                .font(.headline)
            Text(message).font(.caption)
            Button("Try again", action: retry)
        }
        .padding().frame(maxWidth: .infinity, alignment: .leading)
        .background(.quaternary.opacity(0.5))
    }
}
