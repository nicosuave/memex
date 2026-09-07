import SwiftUI

struct ConversationFilterButton: View {
    @Bindable var store: Store
    @State private var showingFilters = false

    var body: some View {
        Button { showingFilters.toggle() } label: {
            Label("Filters", systemImage: store.filters.isActive
                  ? "line.3.horizontal.decrease.circle.fill" : "line.3.horizontal.decrease.circle")
        }
        .accessibilityLabel("Conversation filters")
        .help(store.filters.isActive ? "Filters: \(store.filters.summary)" : "Filter conversations")
        .popover(isPresented: $showingFilters, arrowEdge: .bottom) { controls }
    }

    private var controls: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Filters").font(.headline)
            Grid(alignment: .leading, horizontalSpacing: 16, verticalSpacing: 12) {
                GridRow {
                    Text("Timeframe").foregroundStyle(.secondary)
                    Picker("Timeframe", selection: $store.filters.timeframe) {
                        ForEach(ConversationTimeframe.allCases, id: \.self) { Text($0.title).tag($0) }
                    }.labelsHidden()
                }
                GridRow {
                    Text("Provider").foregroundStyle(.secondary)
                    Picker("Provider", selection: $store.filters.provider) {
                        ForEach(ConversationProvider.allCases, id: \.self) { Text($0.title).tag($0) }
                    }.labelsHidden()
                }
                GridRow {
                    Text("Type").foregroundStyle(.secondary)
                    Picker("Conversation type", selection: $store.filters.conversationType) {
                        ForEach(ConversationOrigin.conversationTypes, id: \.self) { Text($0.title).tag($0) }
                    }
                    .labelsHidden()
                    .help("Your direct chats, work spawned as subagents, or both")
                }
            }
            .pickerStyle(.menu)
            Toggle("Show permission reviews", isOn: $store.filters.showsPermissionReviews)
                .toggleStyle(.checkbox)
                .disabled(store.filters.conversationType != .all)
                .help("Include approval logs when showing chats and subagents")
            Divider()
            HStack {
                Button("Reset Filters") { store.filters = .defaults }
                    .disabled(!store.filters.isActive)
                Spacer()
                Button("Done") { showingFilters = false }
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(16)
        .frame(width: 330)
    }
}
