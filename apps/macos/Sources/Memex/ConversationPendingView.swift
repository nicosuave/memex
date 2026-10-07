import SwiftUI

#if canImport(SQACPUI)
import SQACPUI
#endif

/// Local send state stays outside the source transcript and its search results.
struct ConversationPendingView: View {
    let conversation: LiveConversation
    @Environment(\.accessibilityReduceMotion) private var systemReduceMotion
    @Environment(\.memexReduceMotion) private var appReduceMotion

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            if let pending = conversation.pendingPrompt {
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
            #if canImport(SQACPUI)
            if conversation.isWorking && conversation.snapshot.approvals.isEmpty && conversation.snapshot.questions.isEmpty {
                Group {
                    if systemReduceMotion || appReduceMotion {
                        Text(activityLabel).foregroundStyle(.secondary)
                    } else {
                        AcpShimmerText(activityLabel)
                    }
                }
                .accessibilityElement(children: .ignore)
                .accessibilityLabel(activityLabel)
                .padding(.vertical, 8)
            }
            #endif
        }
        .frame(maxWidth: ConversationReadingLane.maximumWidth, alignment: .leading)
        .padding(.horizontal, ConversationReadingLane.minimumMargin)
        .frame(maxWidth: .infinity)
    }

    private var activityLabel: String {
        conversation.status == "Working…" ? "Thinking..." : conversation.status
    }

    private func recoveryLabel(_ phase: ConversationPendingPrompt.Phase) -> String? {
        switch phase {
        case .preparing, .awaitingConfirmation: nil
        case .uncertain: "Delivery unconfirmed — review before retrying"
        case .notSent: "Not sent"
        }
    }
}
