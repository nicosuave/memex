import Foundation
import SwiftUI

/// App projection of a child read through its parent's provider connection.
/// Absence of a field means the provider has not supplied it.
struct ConversationChildHistory: Identifiable, Equatable, Sendable {
    let id: String
    var records: [TranscriptRecord] = []
    var loading = false
    var error: String?
    var status = "unknown"
    var model: String?
    var reasoningEffort: String?
}

struct ConversationChildView: View {
    let agent: ConversationWork.Agent
    let conversation: LiveConversation?
    let session: Session
    let sessions: [Session]
    let navigate: (Session) -> Void
    @Environment(\.dismiss) private var dismiss
    @State private var raw = false
    @State private var query = ""

    private var history: ConversationChildHistory? {
        conversation?.snapshot.childHistories.first { $0.id == agent.id }
    }
    private var records: [TranscriptRecord] { history?.records ?? [] }
    private func indexed(_ id: String) -> Session? {
        sessions.first { $0.sessionID == id && $0.source == session.source && $0.machineID == session.machineID }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text(agent.prompt ?? agent.id).font(.headline).textSelection(.enabled)
                Spacer()
                Button("Close") { dismiss() }.keyboardShortcut(.cancelAction)
            }
            Text(agent.id).font(.caption.monospaced()).textSelection(.enabled)
            HStack {
                Text(history?.status ?? agent.status)
                Text("Model: \(history?.model ?? "Unavailable")")
                Text("Reasoning: \(history?.reasoningEffort ?? "Unavailable")")
            }.font(.caption).foregroundStyle(.secondary)
            HStack {
                if let conversation {
                    Button(conversation.snapshot.connected ? "Refresh history" : "Connect parent and read history") {
                        Task {
                            if !conversation.snapshot.connected, !(await conversation.connect()) { return }
                            await conversation.readChild(agent.id)
                        }
                    }
                        .disabled(history?.loading == true)
                }
                if let target = indexed(agent.id) {
                    Button("Open indexed conversation") { dismiss(); navigate(target) }
                }
                if let parentID = agent.parentID, let parent = indexed(parentID) {
                    Button("Parent") { dismiss(); navigate(parent) }
                }
                Spacer()
                Toggle("Raw transcript", isOn: $raw)
            }
            if let error = history?.error { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
            if history?.loading == true { ProgressView("Reading child history…") }
            if records.isEmpty && history?.loading != true {
                ContentUnavailableView("Child history unavailable", systemImage: "bubble.left",
                    description: Text(conversation?.snapshot.connected != true ? "Connect the parent conversation to read provider history before indexing completes." : "Refresh to read this child through its parent connection."))
            } else {
                TextField("Find in child history", text: $query)
                NativeTranscript(sessionID: session.id + "#child:" + agent.id, records: records,
                    provider: session.source, hasMore: false, isLoading: history?.loading == true,
                    onLoadMore: {}, findQuery: query,
                    findHit: query.isEmpty ? nil : ConversationMatcher.matches(records, query: query).first,
                    rawTranscript: raw)

            }
        }.padding(20).frame(minWidth: 650, idealWidth: 800, minHeight: 450, idealHeight: 650)
            .task(id: agent.id) { await conversation?.readChild(agent.id) }
    }
}
