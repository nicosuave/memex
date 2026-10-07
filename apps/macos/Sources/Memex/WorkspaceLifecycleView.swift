import AppKit
import SwiftUI
import MemexExecutionHostCore

struct WorkspaceLifecycleView: View {
    let client: ConversationWorkspaceClient
    let referencedDirectories: () -> [URL]
    var isWorkspaceBusy: (ConversationWorkspace) -> Bool = { _ in false }
    let didSelect: (ConversationWorkspace) -> Void
    @Environment(\.dismiss) private var dismiss
    @State private var entries: [ManagedWorkspaceEntry] = []
    @State private var snapshots: [String: [ManagedWorkspaceSnapshot]] = [:]
    @State private var showingArchived = false
    @State private var busy = false
    @State private var error: String?
    @State private var cleanup: ConversationWorkspace?

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Managed workspaces").font(.headline)
                Spacer()
                Toggle("Show archived", isOn: $showingArchived)
                Button("Done") { dismiss() }.disabled(busy)
            }
            Text("Archive keeps all files. Snapshots include ignored files and recover into a new worktree. Cleanup snapshots a clean, unreferenced checkout before removing its folder.")
                .font(.caption).foregroundStyle(.secondary)
            List(entries.filter { showingArchived || !$0.archived }) { entry in
                VStack(alignment: .leading, spacing: 6) {
                    Text(entry.workspace.branch ?? entry.id).font(.headline)
                    Text(entry.workspace.workingDirectory.path).font(.caption).textSelection(.enabled)
                    HStack {
                        Button(entry.removed ? "Reattach" : "Use folder") { run {
                            let workspace = try await client.reattach(entry.workspace)
                            didSelect(workspace)
                            dismiss()
                        } }.disabled(entry.workspace.state != .ready)
                        Button(entry.archived ? "Unarchive" : "Archive") { run {
                            try await client.setArchived(entry.workspace, archived: !entry.archived)
                        } }.disabled(entry.workspace.state != .ready)
                        Button("Reveal") { NSWorkspace.shared.activateFileViewerSelecting([entry.workspace.workingDirectory]) }
                            .disabled(entry.removed)
                        if let snapshot = snapshots[entry.id]?.first {
                            Button("Recover snapshot") { run {
                                let recovered = try await client.recoverSnapshot(snapshot, workspace: entry.workspace)
                                didSelect(recovered)
                                dismiss()
                            } }
                        }
                        if !entry.removed {
                            Button("Snapshot") { run {
                                try await client.captureSnapshot(entry.workspace, isBusy: isWorkspaceBusy(entry.workspace))
                            } }.disabled(entry.workspace.state != .ready)
                            Button("Remove clean checkout…", role: .destructive) { cleanup = entry.workspace }
                                .disabled(entry.workspace.state != .ready)
                        }
                    }.font(.caption)
                    if let snapshot = snapshots[entry.id]?.first {
                        Text("Snapshot saved \(snapshot.createdAt.formatted()) · \(snapshot.head.prefix(8)). Dirty or ignored files still prevent cleanup.")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    if entry.workspace.state == .failed { Text(entry.workspace.failure ?? "Preparation failed; resources retained.").font(.caption).foregroundStyle(.orange) }
                }.padding(.vertical, 4)
            }.frame(minHeight: 280).disabled(busy)
            if busy { ProgressView().controlSize(.small) }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled) }
        }.padding(20).frame(width: 650)
        .task { await refresh() }
        .confirmationDialog("Remove this clean checkout?", isPresented: Binding(get: { cleanup != nil }, set: { if !$0 { cleanup = nil } }), titleVisibility: .visible) {
            Button("Remove clean checkout", role: .destructive) {
                guard let workspace = cleanup else { return }
                cleanup = nil
                run { try await client.removeCleanCheckout(workspace, otherWorkspaceDirectories: referencedDirectories(), isBusy: isWorkspaceBusy(workspace)) }
            }
            Button("Cancel", role: .cancel) { cleanup = nil }
        } message: {
            Text("The operation refuses staged, unstaged, untracked or ignored files, and any checkout referenced by another chat. A recoverable snapshot is saved before eligible cleanup. Its branch and commits remain. Dirty, shared, or ignored contents keep the checkout on disk.")
        }
        .interactiveDismissDisabled(busy)
    }

    private func run(_ operation: @escaping @MainActor () async throws -> Void) {
        busy = true
        error = nil
        Task { @MainActor in
            defer { busy = false }
            do { try await operation(); await refresh() }
            catch { self.error = error.localizedDescription }
        }
    }

    @MainActor private func refresh() async {
        do {
            entries = try await client.managedWorkspaces()
            for entry in entries where entry.workspace.state == .ready {
                snapshots[entry.id] = try await client.snapshots(entry.workspace)
            }
        }
        catch { self.error = error.localizedDescription }
    }
}
