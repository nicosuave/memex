import SwiftUI

struct HomeView: View {
    @Bindable var store: Store
    @FocusState private var searchFocused: Bool
    @State private var showingFilters = false
    @State private var visibleSessionIDs: Set<String> = []

    private var filtersHighlighted: Bool { showingFilters || store.filters.isActive || store.homeProject != nil }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 24) {
                if InAppAgentRuntime.isAvailable { HomeConversationComposer(store: store) }
                HomeActivityView(store: store)
                HStack(spacing: 10) {
                    Image(systemName: "magnifyingglass").foregroundStyle(.secondary)
                    TextField("Search conversations", text: $store.query)
                        .textFieldStyle(.plain).font(.title3)
                        .focused($searchFocused)
                        .onSubmit {
                            if let first = store.librarySessions.first { store.openConversation(first) }
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

                VStack(alignment: .leading, spacing: 12) {
                    HStack {
                        Text(store.query.isEmpty ? "Recent conversations" : "Matching conversations").font(.title2.weight(.semibold))
                        Spacer()
                        Button { showingFilters.toggle() } label: {
                            Image(systemName: "line.3.horizontal.decrease")
                                .frame(width: 20, height: 20)
                        }
                        .modifier(HomeActionButtonStyle())
                        .foregroundStyle(filtersHighlighted ? Color.accentColor : Color.primary)
                        .accessibilityLabel("Filter conversations")
                        .help("Filter conversations")
                        .popover(isPresented: $showingFilters, arrowEdge: .bottom) {
                            ConversationFilterControls(store: store, includesProject: true) { showingFilters = false }
                        }
                        if InAppAgentRuntime.isAvailable {
                            Button { store.beginNewConversation() } label: {
                                Image(systemName: "plus")
                                    .frame(width: 20, height: 20)
                            }
                            .modifier(HomeActionButtonStyle())
                            .accessibilityLabel("New conversation")
                            .help("New conversation")
                        }
                    }
                    if let error = store.listError {
                        ErrorBanner(message: error) { Task { await store.loadSessions() } }
                    }
                    if store.librarySessions.isEmpty && !store.loadingSessions {
                        ContentUnavailableView("No conversations", systemImage: "bubble.left.and.bubble.right",
                            description: Text("Try another search or change your filters."))
                    }
                    LazyVStack(spacing: 0) {
                        ForEach(store.librarySessions) { session in
                            Button { store.openConversation(session) } label: {
                                VStack(alignment: .leading, spacing: 4) {
                                    HStack(alignment: .firstTextBaseline) {
                                        Text(session.title).font(.headline).lineLimit(1)
                                        ConversationStateLabel(state: store.liveConversations.listState(for: session))
                                        Spacer()
                                        if let date = session.date {
                                            TimelineView(.periodic(from: .now, by: 60)) { context in
                                                Text(homeRelativeTimestamp(date, now: context.date))
                                                    .font(.caption).foregroundStyle(.secondary)
                                            }
                                        }
                                    }
                                    HStack(spacing: 6) {
                                        Text([store.projectName(for: session), session.source, session.machineID].joined(separator: " · "))
                                        if session.isSubagent {
                                            Text("·")
                                            Text("Subagent")
                                        }
                                    }
                                    .font(.caption).foregroundStyle(.secondary)
                                    if let snippet = session.snippet?.nilIfBlank {
                                        Text(ConversationExcerpt.text(snippet, query: store.query))
                                            .font(.callout).foregroundStyle(.secondary).lineLimit(2).help(snippet)
                                    }
                                }
                                .frame(maxWidth: .infinity, alignment: .leading)
                                .padding(.vertical, 8)
                                .contentShape(Rectangle())
                            }
                            .buttonStyle(.plain)
                            .onAppear {
                                visibleSessionIDs.insert(session.id)
                                loadNextPageIfNeeded()
                            }
                            .onDisappear { visibleSessionIDs.remove(session.id) }
                        }
                        if store.loadingSessions {
                            ForEach(0..<(store.sessions.isEmpty ? 6 : 2), id: \.self) { index in
                                HomeConversationSkeletonRow(index: index)
                            }
                        }
                    }
                }
            }
            .frame(maxWidth: 900, alignment: .leading)
            .padding(32)
            .frame(maxWidth: .infinity)
        }
        .background(Color(nsColor: .windowBackgroundColor))
        .onAppear {
            if InAppAgentRuntime.isAvailable { store.newConversationDraft.focusRequest += 1 }
            else { searchFocused = true }
        }
        .onChange(of: store.loadingSessions) { _, loading in
            if !loading { loadNextPageIfNeeded() }
        }
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

    private func loadNextPageIfNeeded() {
        guard store.listError == nil,
              let visible = store.sessions.suffix(5).first(where: { visibleSessionIDs.contains($0.id) }) else { return }
        store.loadMoreSessionsIfNeeded(visibleID: visible.id)
    }

    private func refreshIfVisible() async {
        let window = NSApplication.shared.mainWindow
        await store.refreshHomeIfStale(isVisible: NSApplication.shared.isActive
            && window?.isVisible == true && window?.isMiniaturized == false)
    }
}

private struct HomeConversationSkeletonRow: View {
    let index: Int
    @Environment(\.accessibilityReduceMotion) private var systemReduceMotion
    @Environment(\.memexReduceMotion) private var appReduceMotion
    private var reduceMotion: Bool { systemReduceMotion || appReduceMotion }

    var body: some View {
        TimelineView(.periodic(from: .now, by: 0.8)) { context in
            let time = reduceMotion ? 0 : context.date.timeIntervalSinceReferenceDate
            GeometryReader { geometry in
                VStack(alignment: .leading, spacing: 6) {
                    HStack {
                        RoundedRectangle(cornerRadius: 3)
                            .frame(width: geometry.size.width * [0.46, 0.34, 0.55][index % 3], height: 14)
                        Spacer()
                        RoundedRectangle(cornerRadius: 3).frame(width: 56, height: 10)
                    }
                    RoundedRectangle(cornerRadius: 3)
                        .frame(width: geometry.size.width * 0.25, height: 10)
                }
                .foregroundStyle(.primary.opacity(0.06 + 0.04 * (sin(time * 1.7 + Double(index)) + 1) / 2))
                .animation(reduceMotion ? nil : .easeInOut(duration: 0.8), value: time)
            }
            .frame(height: 33)
            .padding(.vertical, 8)
        }
        .accessibilityElement(children: .ignore)
        .accessibilityLabel("Loading conversations")
    }
}

func homeRelativeTimestamp(_ date: Date, now: Date) -> String {
    let elapsed = max(0, now.timeIntervalSince(date))
    switch elapsed {
    case ..<60: return "Just now"
    case ..<3600: return "\(Int(elapsed / 60)) min ago"
    case ..<86400: return "\(Int(elapsed / 3600)) hr ago"
    case ..<604800: return "\(Int(elapsed / 86400))d ago"
    default: return date.formatted(.dateTime.month(.abbreviated).day().year())
    }
}

private struct HomeActionButtonStyle: ViewModifier {
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
