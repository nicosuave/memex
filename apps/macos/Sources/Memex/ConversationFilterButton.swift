import SwiftUI

struct ConversationFilterControls: View {
    @Bindable var store: Store
    let done: () -> Void

    var body: some View {
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
                Button("Done", action: done)
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(16)
        .frame(width: 330)
    }
}
