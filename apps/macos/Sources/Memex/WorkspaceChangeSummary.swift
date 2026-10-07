import SwiftUI

/// This is current Git status for the workspace, never attribution to a chat turn.
struct WorkspaceChangeSummary: View {
    let directory: URL
    let isWorking: Bool
    let review: (String?) -> Void
    var horizontalInset: CGFloat = ConversationReadingLane.minimumMargin
    @State private var snapshot: WorkspaceChangesSnapshot?
    @State private var loadedDirectory: URL?
    @State private var activeDirectory: URL?
    @State private var requestID = UUID()
    @State private var error: String?

    var body: some View {
        VStack(spacing: 0) {
            if loadedDirectory == directory, let snapshot, !snapshot.files.isEmpty {
                HStack(spacing: 8) {
                    Menu {
                        ForEach(snapshot.files) { file in
                            Button("\(file.label) — \(file.status)") { review(file.path) }
                        }
                    } label: {
                        Label("\(snapshot.files.count) workspace \(snapshot.files.count == 1 ? "file" : "files") changed",
                              systemImage: "doc.text.magnifyingglass")
                    }
                    .menuStyle(.borderlessButton).fixedSize()
                    Spacer()
                    Button("Review changes") { review(nil) }.buttonStyle(.borderless)
                }
                .font(.caption).foregroundStyle(.secondary)
                .padding(.horizontal, 8).padding(.vertical, 6)
                .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 8))
                .help(error ?? "Current staged, unstaged and untracked changes across this Git worktree.")
            } else if loadedDirectory == directory, let error {
                Button("Workspace changes unavailable") { review(nil) }
                    .buttonStyle(.borderless).font(.caption).foregroundStyle(.secondary).help(error)
                    .padding(.horizontal, 8).padding(.vertical, 6)
                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 8))
            }
        }
        .frame(maxWidth: ConversationReadingLane.maximumWidth, alignment: .leading)
        .padding(.horizontal, horizontalInset)
        .frame(maxWidth: .infinity)
        .task(id: directory) {
            activeDirectory = directory
            snapshot = nil
            error = nil
            loadedDirectory = nil
            while !Task.isCancelled {
                await refresh()
                do { try await Task.sleep(for: .seconds(isWorking ? 2 : 10)) }
                catch { return }
            }
        }
        .onChange(of: isWorking) { _, _ in Task { await refresh() } }
    }

    private func refresh() async {
        guard activeDirectory == directory else { return }
        let request = UUID()
        requestID = request
        do {
            let value = try await WorkspaceChangesClient().snapshot(directory: directory)
            guard !Task.isCancelled, activeDirectory == directory, requestID == request else { return }
            snapshot = value
            loadedDirectory = directory
            error = nil
        } catch {
            guard !Task.isCancelled, activeDirectory == directory, requestID == request else { return }
            loadedDirectory = directory
            self.error = error.localizedDescription
        }
    }
}
