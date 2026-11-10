import SwiftUI

/// Local send state stays outside the source transcript and its search results.
struct ConversationPendingView: View {
    let conversation: LiveConversation
    @State private var slowSend = false

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            if let notice = conversation.notice {
                HStack(alignment: .top) {
                    Text(notice).font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
                    Spacer()
                    Button { conversation.clearNotice() } label: { Image(systemName: "xmark") }
                        .buttonStyle(.plain).accessibilityLabel("Dismiss")
                }
            }
            // A new-turn prompt in flight is a transcript row. This bubble is for
            // what is not in the transcript: steers, and prompts needing recovery.
            if let pending = conversation.pendingPrompt, conversation.optimisticRecord == nil {
                HStack {
                    Spacer(minLength: 30)
                    VStack(alignment: .trailing, spacing: 6) {
                        if !pending.text.isEmpty {
                            Text(pending.text).font(.system(size: 14)).lineLimit(4)
                                .textSelection(.enabled)
                        }
                        if !pending.attachments.isEmpty {
                            Label(pending.attachments.map(\.title).joined(separator: ", "), systemImage: "paperclip")
                                .font(.caption).lineLimit(2)
                        }
                        if let label = recoveryLabel(pending.phase) {
                            HStack(spacing: 8) {
                                Text(label).font(.caption).foregroundStyle(.secondary)
                                Button("Restore draft") { conversation.restorePendingDraft() }
                                    .buttonStyle(.borderless).font(.caption)
                                    .disabled(conversation.isWorking || (pending.phase == .uncertain && !conversation.snapshot.ready))
                                    .help("Review the conversation first. This restores the message without sending it.")
                            }
                        }
                    }
                    .padding(.horizontal, 12).padding(.vertical, 8)
                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10))
                }
            }
            if conversation.isWorking && conversation.snapshot.approvals.isEmpty && conversation.snapshot.questions.isEmpty {
                // Static under Reduce Motion; the label itself is the accessibility element.
                ShimmerText(activityLabel, font: AppPreferences.shared.bodyNSFont).padding(.vertical, 8)
            }
        }
        .frame(maxWidth: ConversationReadingLane.maximumWidth, alignment: .leading)
        .padding(.horizontal, ConversationReadingLane.minimumMargin)
        .frame(maxWidth: .infinity)
        .task(id: conversation.pendingPrompt?.commandID) {
            slowSend = false
            guard conversation.pendingPrompt != nil else { return }
            do { try await Task.sleep(for: .seconds(2)) } catch { return }
            slowSend = true
        }
    }

    private var activityLabel: String {
        // An ordinary send is acknowledged within a moment; naming it only when
        // it is slow avoids a label that flashes between Enter and the first output.
        if conversation.status == "Sending…" && !slowSend { return "Thinking…" }
        guard conversation.status == "Working…" else { return conversation.status }
        return TranscriptPresentation.liveStatus(conversation.snapshot.records) ?? "Thinking…"
    }

    private func recoveryLabel(_ phase: ConversationPendingPrompt.Phase) -> String? {
        switch phase {
        case .preparing, .awaitingConfirmation: nil
        case .uncertain: "Delivery unconfirmed — review before retrying"
        case .notSent: "Not sent"
        }
    }
}
