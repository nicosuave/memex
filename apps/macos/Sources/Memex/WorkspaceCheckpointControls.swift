import SwiftUI

struct WorkspaceCheckpointControls: View {
    let directory: URL
    let conversationID: String
    @Binding var scope: WorkspaceDiffScope
    var isolation: (() -> WorkspaceIsolation?)? = nil
    var rewindConversation: ((WorkspaceCheckpoint) async throws -> Void)? = nil
    var didChange: () -> Void = {}
    @State private var checkpoints: [WorkspaceCheckpoint] = []
    @State private var selected: WorkspaceCheckpoint?
    @State private var confirmation = false
    @State private var busy = false
    @State private var error: String?
    private let client = WorkspaceCheckpointClient.shared

    var body: some View {
        HStack {
            Menu("Checkpoints") {
                Button("Capture checkpoint") { Task { await capture() } }
                Button("Refresh checkpoints") { Task { await refresh() } }
                Divider()
                ForEach(Array(checkpoints.enumerated()), id: \.element.id) { index, checkpoint in
                    Menu(checkpoint.label) {
                        Button("Review changes") {
                            scope = .checkpoint(from: index > 0 ? checkpoints[index - 1].tree : checkpoint.head,
                                                to: checkpoint.tree, label: checkpoint.label)
                        }
                        Button("Rewind…") { selected = checkpoint; confirmation = true }
                    }
                }
            }.disabled(busy)
            if busy { ProgressView().controlSize(.small) }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).lineLimit(2).help(error) }
        }
        .task(id: conversationID) { await refresh() }
        .confirmationDialog("Rewind to \(selected?.label ?? "checkpoint")?", isPresented: $confirmation, titleVisibility: .visible) {
            if rewindConversation != nil, selected?.turnID != nil {
                Button("Conversation only") { Task { await restore(files: false) } }
            }
            Button("Restore workspace files", role: .destructive) { Task { await restore(files: true) } }
                .disabled(isolation?() == nil)
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("File restore requires an idle, owned worktree used by no other chat. A recovery checkpoint is saved first. Ignored files and nested repositories are not restored. Git history is unchanged.")
        }
    }

    @MainActor private func refresh() async {
        do { checkpoints = try await client.list(directory: directory, conversationID: conversationID) }
        catch { self.error = error.localizedDescription }
    }

    @MainActor private func capture() async {
        busy = true
        defer { busy = false }
        do {
            _ = try await client.capture(directory: directory, conversationID: conversationID,
                label: "Checkpoint \(Date().formatted(date: .abbreviated, time: .shortened))")
            error = nil
            await refresh()
        } catch { self.error = error.localizedDescription }
    }

    @MainActor private func restore(files: Bool) async {
        guard let selected else { return }
        busy = true
        defer { busy = false }
        do {
            if files {
                guard let isolation = isolation?() else { throw WorkspaceGitError(message: "Workspace ownership is unavailable.") }
                _ = try await client.restore(selected, isolation: isolation)
            } else if let rewindConversation { try await rewindConversation(selected) }
            error = nil
            didChange()
            await refresh()
        } catch {
            self.error = error.localizedDescription
            // A recovery snapshot can have been created before a later failure.
            await refresh()
        }
    }
}
