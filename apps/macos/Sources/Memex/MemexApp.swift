import AppKit
import SwiftUI

@main
struct MemexApp: App {
    @State private var store = Store(filterPreferences: .standard)

    var body: some Scene {
        WindowGroup {
            BrowserView(store: store)
                .frame(minWidth: 900, minHeight: 560)
        }
        .defaultSize(width: 1380, height: 900)
        .commands {
            CommandGroup(after: .newItem) {
                Button("Refresh Conversations") { Task { await store.refresh() } }
                    .keyboardShortcut("r")
            }
        }
    }
}

struct BrowserView: View {
    @Bindable var store: Store

    var body: some View {
        NavigationSplitView {
            sidebar
                .navigationSplitViewColumnWidth(min: 180, ideal: 210, max: 290)
        } content: {
            sessionList
                .navigationSplitViewColumnWidth(min: 260, ideal: 330, max: 450)
        } detail: {
            ReaderView(store: store)
        }
        .navigationSplitViewStyle(.balanced)
        .toolbar(removing: defaultTitleItem)
        .searchable(text: $store.query, placement: .toolbar, prompt: "Search conversations")
        .task(id: store.requestID) { await store.loadSessions() }
        .task(id: store.readerRequestID) { await store.loadRecords() }
        .task(id: store.readerRequestID) { await store.loadSelectedSessionMetadata() }
        .task { await store.loadMachines() }
        .task(id: store.machineRequestID) { await store.loadProjects() }
        .onChange(of: store.scope) { _, _ in store.sessionLimit = 200 }
        .onChange(of: store.machineSelection) { _, _ in store.sessionLimit = 200 }
        .toolbar {
            if #available(macOS 26.0, *) {
                titleToolbarItem.sharedBackgroundVisibility(.hidden)
                toolbarCenter.sharedBackgroundVisibility(.hidden)
            } else {
                titleToolbarItem
                toolbarCenter
            }
            if #available(macOS 26.0, *) {
                resumeToolbarItem.sharedBackgroundVisibility(.hidden)
                ToolbarSpacer(.fixed, placement: .primaryAction)
            } else {
                resumeToolbarItem
            }
            ToolbarItem(placement: .primaryAction) {
                ConversationFilterButton(store: store)
            }
            ToolbarItem(placement: .primaryAction) {
                Button { Task { await store.refresh() } } label: {
                    Label("Refresh", systemImage: "arrow.clockwise")
                }
                .help("Refresh conversations (⌘R)")
                .disabled(store.loadingSessions)
            }
        }
    }

    private var resumeToolbarItem: some ToolbarContent {
        ToolbarItem(placement: .primaryAction) {
            if store.selected != nil { ResumeToolbarButton(store: store) }
        }
    }

    // Retain the native center slot so trailing actions stay beside search.
    private var toolbarCenter: some ToolbarContent {
        ToolbarItem(placement: .principal) {
            Color.clear.frame(width: 1, height: 1).accessibilityHidden(true)
        }
    }

    private var defaultTitleItem: ToolbarDefaultItemKind? {
        if #available(macOS 15.0, *) { return .title }
        return nil
    }

    private var titleToolbarItem: some ToolbarContent {
            ToolbarItem(placement: .navigation) {
                HStack(alignment: .firstTextBaseline, spacing: 10) {
                    Text(store.scope.title)
                        .font(.headline)
                        .lineLimit(1)
                    Text(store.loadingSessions ? "Loading…" : "\(store.sessions.count) conversations")
                        .font(.subheadline.weight(.regular))
                        .foregroundStyle(.secondary)
                        .monospacedDigit()
                        .lineLimit(1)
                        .help("Conversations currently loaded")
                }
            }
    }

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
        .navigationTitle("Memex")
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
                    .help("Sort projects").accessibilityLabel("Sort projects")
                }
            }

        }
        .listStyle(.sidebar)
    }

    private var sessionList: some View {
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
        .navigationTitle(store.scope.title)
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
