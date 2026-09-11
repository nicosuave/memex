import SwiftUI

struct HomeView: View {
    @Bindable var store: Store
    @FocusState private var searchFocused: Bool
    @State private var showingFilters = false

    private var filtersHighlighted: Bool { showingFilters || store.filters.isActive || store.homeProject != nil }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                HomeActivityView(store: store)
                HStack(spacing: 10) {
                    Image(systemName: "magnifyingglass").foregroundStyle(.secondary)
                    TextField("Search conversations", text: $store.query)
                        .textFieldStyle(.plain).font(.title3)
                        .focused($searchFocused)
                        .onSubmit {
                            if let first = store.sessions.first { store.openConversation(first) }
                        }
                    if !store.query.isEmpty {
                        Button { store.query = "" } label: { Image(systemName: "xmark.circle.fill") }
                            .buttonStyle(.plain).foregroundStyle(.secondary)
                            .accessibilityLabel("Clear search")
                    }
                }
                .padding(14)
                .background(.background, in: RoundedRectangle(cornerRadius: 10))
                .overlay(RoundedRectangle(cornerRadius: 10).strokeBorder(.quaternary))

                HStack {
                    Text(store.query.isEmpty ? "Recent conversations" : "Matching conversations").font(.title2.weight(.semibold))
                    if store.loadingSessions { ProgressView().controlSize(.small) }
                    Spacer()
                    Button { showingFilters.toggle() } label: {
                        Image(systemName: "line.3.horizontal.decrease")
                            .frame(width: 20, height: 20)
                    }
                    .modifier(HomeFilterButtonStyle())
                    .foregroundStyle(filtersHighlighted ? Color.accentColor : Color.primary)
                    .accessibilityLabel("Filter conversations")
                    .help("Filter conversations")
                    .popover(isPresented: $showingFilters, arrowEdge: .bottom) {
                        ConversationFilterControls(store: store, includesProject: true) { showingFilters = false }
                    }
                }
                if let error = store.listError {
                    ErrorBanner(message: error) { Task { await store.loadSessions() } }
                }
                if store.sessions.isEmpty && !store.loadingSessions {
                    ContentUnavailableView("No conversations", systemImage: "bubble.left.and.bubble.right",
                        description: Text("Try another search or change your filters."))
                }
                LazyVStack(spacing: 0) {
                    ForEach(store.sessions) { session in
                        Button { store.openConversation(session) } label: {
                            VStack(alignment: .leading, spacing: 7) {
                                HStack(alignment: .firstTextBaseline) {
                                    Text(session.title).font(.headline).lineLimit(1)
                                    Spacer()
                                    if let date = session.date {
                                        Text(date, style: .relative).font(.caption).foregroundStyle(.secondary)
                                    }
                                }
                                HStack(spacing: 6) {
                                    Text([session.projectName, session.source, session.machineID].joined(separator: " · "))
                                    if session.isSubagent {
                                        Text("·")
                                        Text("Subagent")
                                    }
                                }
                                .font(.caption).foregroundStyle(.secondary)
                                if let snippet = session.snippet?.nilIfBlank {
                                    Text(snippet).font(.callout).foregroundStyle(.secondary).lineLimit(2)
                                }
                            }
                            .frame(maxWidth: .infinity, alignment: .leading)
                            .padding(.vertical, 14).padding(.horizontal, 12)
                            .contentShape(Rectangle())
                        }
                        .buttonStyle(.plain)
                        Divider()
                    }
                    if store.hasMoreSessions {
                        Button("Load more conversations") {
                            if let last = store.sessions.last { store.loadMoreSessionsIfNeeded(visibleID: last.id) }
                        }.padding().disabled(store.loadingSessions)
                    }
                }
            }
            .frame(maxWidth: 900, alignment: .leading)
            .padding(32)
            .frame(maxWidth: .infinity)
        }
        .background(Color(nsColor: .windowBackgroundColor))
        .onAppear { searchFocused = true }
        .task {
            while !Task.isCancelled {
                await refreshIfVisible()
                do { try await Task.sleep(for: .seconds(30)) }
                catch { return }
            }
        }
        .onReceive(NotificationCenter.default.publisher(for: NSApplication.didBecomeActiveNotification)) { _ in
            Task { await refreshIfVisible() }
        }
        .onReceive(NotificationCenter.default.publisher(for: NSWindow.didBecomeMainNotification)) { _ in
            Task { await refreshIfVisible() }
        }
    }

    private func refreshIfVisible() async {
        let window = NSApplication.shared.mainWindow
        await store.refreshHomeIfStale(isVisible: NSApplication.shared.isActive
            && window?.isVisible == true && window?.isMiniaturized == false)
    }
}

private struct HomeFilterButtonStyle: ViewModifier {
    @State private var hovering = false

    func body(content: Content) -> some View {
        styledButton(content)
            .overlay {
                Circle()
                    .fill(.primary.opacity(hovering ? 0.08 : 0))
                    .allowsHitTesting(false)
            }
            .contentShape(Circle())
            .onHover { hovering = $0 }
    }

    @ViewBuilder
    private func styledButton(_ content: Content) -> some View {
        if #available(macOS 26.0, *) {
            content
                .buttonStyle(.glass)
                .buttonBorderShape(.circle)
                .controlSize(.large)
        } else {
            content
                .buttonStyle(.bordered)
                .buttonBorderShape(.circle)
        }
    }
}
