import SwiftUI

/// The new chat as soon as Home sends: the prompt is visible while its workspace
/// and native session start, and failures keep the Home draft for editing.
struct PendingConversationStartView: View {
    let store: Store
    let pending: PendingConversationStart

    var body: some View {
        VStack(spacing: 0) {
            // The same transcript the created conversation uses, so the prompt keeps
            // its bubble, width and typography when the real conversation replaces this page.
            NativeTranscript(sessionID: "pending-start:" + pending.id.uuidString, records: [promptRecord],
                             provider: pending.providerName, hasMore: false, isLoading: false, onLoadMore: {},
                             startsAtEnd: true)
            status
                .frame(maxWidth: ConversationReadingLane.maximumWidth, alignment: .leading)
                .padding(.horizontal, ConversationReadingLane.minimumMargin).padding(.vertical, 12)
                .frame(maxWidth: .infinity)
        }
        .navigationTitle(pending.prompt.split(separator: "\n").first.map(String.init) ?? "New conversation")
    }

    private var promptRecord: TranscriptRecord {
        var text = pending.prompt
        if !pending.attachmentNames.isEmpty {
            text += (text.isEmpty ? "" : "\n\n") + "Attached: " + pending.attachmentNames.joined(separator: ", ")
        }
        return TranscriptRecord(recordID: "pending-start:" + pending.id.uuidString,
                                record: Message(role: "user", text: text, toolName: nil, toolInput: nil, toolOutput: nil))
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
                ShimmerText(pending.status, font: AppPreferences.shared.bodyNSFont)
                if let project = pending.projectName { Text("· \(project)").foregroundStyle(.tertiary) }
            }.font(.callout)
        }
    }
}
