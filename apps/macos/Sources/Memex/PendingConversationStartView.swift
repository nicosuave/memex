import SwiftUI

/// The new chat as soon as Home sends: the prompt is visible while its workspace
/// and native session start, and failures keep the Home draft for editing.
struct PendingConversationStartView: View {
    let store: Store
    let pending: PendingConversationStart

    var body: some View {
        VStack(spacing: 0) {
            ScrollView {
                VStack(alignment: .trailing, spacing: 12) {
                    VStack(alignment: .leading, spacing: 6) {
                        Text("You").font(.caption.weight(.semibold)).foregroundStyle(.secondary)
                        Text(pending.prompt)
                            .font(Font(AppPreferences.shared.bodyNSFont))
                            .textSelection(.enabled)
                        if !pending.attachmentNames.isEmpty {
                            Text(pending.attachmentNames.joined(separator: ", "))
                                .font(.caption).foregroundStyle(.secondary)
                        }
                    }
                    .padding(12)
                    .background(Color.primary.opacity(0.05), in: RoundedRectangle(cornerRadius: 16))
                    .frame(maxWidth: 640, alignment: .trailing)
                    status.frame(maxWidth: .infinity, alignment: .leading)
                }
                .frame(maxWidth: 820)
                .padding(24)
                .frame(maxWidth: .infinity)
            }
        }
        .navigationTitle(pending.prompt.split(separator: "\n").first.map(String.init) ?? "New conversation")
    }

    @ViewBuilder private var status: some View {
        if let error = pending.error {
            VStack(alignment: .leading, spacing: 8) {
                Text(error).foregroundStyle(.orange).textSelection(.enabled)
                HStack {
                    Button("Edit draft") { store.returnToHomeDraft() }
                    Button("Try again") { Task { await store.startConversationFromHome() } }
                        .disabled(!store.canStartConversation)
                }
            }.font(.callout)
        } else {
            HStack(spacing: 8) {
                ProgressView().controlSize(.small)
                Text(pending.status)
                if let project = pending.projectName { Text("· \(project)").foregroundStyle(.tertiary) }
            }.font(.callout).foregroundStyle(.secondary)
        }
    }
}
