import AppKit
import SwiftUI

struct ConversationQueueView: View {
    @Bindable var conversation: LiveConversation
    @State private var editing: ConversationQueuedPrompt?
    @State private var expanded = true
    @State private var transferring: ConversationQueuedPrompt?

    var body: some View {
        Group {
          if !conversation.queue.isEmpty {
            DisclosureGroup(isExpanded: $expanded) {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(Array(conversation.queue.enumerated()), id: \.element.id) { index, entry in
                        HStack(alignment: .top, spacing: 8) {
                            Text("\(index + 1)").foregroundStyle(.secondary).monospacedDigit()
                            VStack(alignment: .leading, spacing: 3) {
                                if !entry.text.isEmpty { Text(entry.text).lineLimit(3).textSelection(.enabled) }
                                if !entry.attachments.isEmpty {
                                    Label(entry.attachments.map(\.title).joined(separator: ", "), systemImage: "paperclip")
                                        .foregroundStyle(.secondary).lineLimit(2)
                                }
                            }.frame(maxWidth: .infinity, alignment: .leading)
                            Menu {
                                Button("Edit message…") { conversation.holdQueue(); editing = entry }
                                Button("Move earlier") { conversation.moveQueued(id: entry.id, offset: -1) }.disabled(index == 0)
                                Button("Move later") { conversation.moveQueued(id: entry.id, offset: 1) }.disabled(index == conversation.queue.count - 1)
                                if conversation.canSteer {
                                    Button("Steer current turn now") { Task { await conversation.promoteQueued(id: entry.id) } }
                                } else if !conversation.isWorking {
                                    Button("Send next") { Task { await conversation.promoteQueued(id: entry.id) } }
                                }
                                if conversation.canTransferQueuedPrompt {
                                    Button("Move to side-chat draft…") { conversation.holdQueue(); transferring = entry }
                                } else if conversation.isServerOwned || conversation.session.machineID != "local" {
                                    Text("Side-chat transfer unavailable on this host")
                                }
                                Button("Remove queued message", role: .destructive) { conversation.cancelQueued(id: entry.id) }
                            } label: { Image(systemName: "ellipsis") }.menuStyle(.borderlessButton).fixedSize().disabled(conversation.transferringQueue)
                        }
                    }
                    if conversation.queueHeld {
                        HStack {
                            Text("Queue paused. Queued messages will not send until you resume.").foregroundStyle(.secondary)
                            Spacer()
                            Button("Resume queue") { Task { await conversation.resumeQueue() } }
                                .disabled(conversation.pendingPrompt != nil || conversation.submitting || conversation.connecting)
                        }
                    } else {
                        Button("Pause queue") { conversation.holdQueue() }
                    }
                }.padding(.top, 6)
            } label: {
                Label("\(conversation.queue.count) queued\(conversation.queueHeld ? " · Paused" : "")", systemImage: "list.bullet")
            }
            .font(.caption).padding(10)
            .background(.quaternary, in: RoundedRectangle(cornerRadius: 8))
          }
        }
        .sheet(item: $transferring) { entry in
            VStack(alignment: .leading, spacing: 12) {
                Text("Move to side-chat draft").font(.title2)
                Text("This message and its captured attachments will be saved in a separate chat for review. It will leave this queue only after the draft is saved.").font(.callout)
                ScrollView { Text(entry.text).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading) }
                ForEach(entry.attachments) { Label($0.title, systemImage: "paperclip") }
                HStack {
                    Spacer()
                    Button("Cancel") { transferring = nil }
                    Button("Move draft") { transferring = nil; Task { await conversation.transferQueuedPrompt(id: entry.id) } }
                }
            }.padding(20).frame(width: 520, height: 340)
        }
        .sheet(item: $editing) { entry in
            ConversationQueueEditor(conversation: conversation, entry: entry)
        }
    }
}

private struct ConversationQueueEditor: View {
    let conversation: LiveConversation
    let entry: ConversationQueuedPrompt
    @Environment(\.dismiss) private var dismiss
    @State private var text: String
    @State private var attachments: [ConversationAttachment]
    @State private var error: String?
    @State private var loading = false

    init(conversation: LiveConversation, entry: ConversationQueuedPrompt) {
        self.conversation = conversation
        self.entry = entry
        _text = State(initialValue: entry.text)
        _attachments = State(initialValue: entry.attachments)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Edit queued message").font(.title2)
            Text("The queue is paused while you edit. Resume it when you are ready.").font(.caption).foregroundStyle(.secondary)
            TextEditor(text: $text).font(.body).frame(minHeight: 140)
            ForEach(attachments) { item in
                HStack {
                    Label(item.title, systemImage: "paperclip")
                    Spacer()
                    Button("Remove") { attachments.removeAll { $0.id == item.id } }
                }.font(.caption)
            }
            if !stillQueued {
                Text("This message has already left the queue. Copy any edits you want to keep before closing.")
                    .foregroundStyle(.orange)
            }
            if let error { Text(error).foregroundStyle(.orange).font(.caption) }
            HStack {
                Button("Attach files…") { Task { await attach() } }.disabled(loading || !stillQueued)
                Spacer()
                Button("Cancel") { dismiss() }.keyboardShortcut(.cancelAction)
                Button("Save changes") {
                    guard stillQueued else { return }
                    conversation.editQueued(id: entry.id, text: text, attachments: attachments)
                    dismiss()
                }
                .keyboardShortcut(.defaultAction)
                .disabled(!stillQueued || loading || (text.nilIfBlank == nil && attachments.isEmpty))
            }
        }.padding(20).frame(width: 520)
    }

    private var stillQueued: Bool { conversation.queue.contains { $0.id == entry.id } }

    @MainActor private func attach() async {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = true
        guard await panel.begin() == .OK, let controls = conversation.snapshot.controls else { return }
        loading = true
        defer { loading = false }
        do {
            let urls = panel.urls
            let current = attachments
            let captured = try await Task.detached {
                try ConversationAttachment.capture(urls, controls: controls, existing: current)
            }.value
            attachments += captured
            error = nil
        } catch { self.error = error.localizedDescription }
    }
}
