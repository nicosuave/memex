import AppKit
import SwiftUI

struct ProjectCreationView: View {
    let didCreate: (URL) -> Void
    @Environment(\.dismiss) private var dismiss
    @State private var kind = ProjectCreationRecord.Kind.repository
    @State private var name = ""
    @State private var remote = ""
    @State private var parent: URL?
    @State private var task: Task<Void, Never>?
    @State private var error: String?
    @State private var retained: URL?
    @State private var cancellationRequested = false

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Create project").font(.headline)
            Picker("Source", selection: $kind) {
                Text("New Git repository").tag(ProjectCreationRecord.Kind.repository)
                Text("Clone repository").tag(ProjectCreationRecord.Kind.clone)
            }.pickerStyle(.segmented).disabled(task != nil)
            TextField("Project folder name", text: $name).disabled(task != nil)
            if kind == .clone { TextField("Git URL or local repository path", text: $remote).disabled(task != nil) }
            HStack {
                Text(parent?.path ?? "Choose a parent folder").font(.caption).textSelection(.enabled)
                Spacer()
                Button("Choose…") { Task { await chooseParent() } }.disabled(task != nil)
            }
            Text(kind == .repository ? "Creates a repository and README. Nothing is committed or pushed automatically."
                 : "Uses your configured Git credentials. Partial clones are retained on failure or cancellation.")
                .font(.caption).foregroundStyle(.secondary)
            if let retained {
                Button("Reveal retained files") { NSWorkspace.shared.activateFileViewerSelecting([retained]) }
            }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled) }
            HStack {
                if task != nil {
                    ProgressView().controlSize(.small)
                    Text(cancellationRequested ? "Cancelling…" : kind == .clone ? "Cloning repository…" : "Creating repository…").font(.caption)
                    Spacer()
                    Button("Cancel operation") { cancellationRequested = true; task?.cancel() }.disabled(cancellationRequested)
                } else {
                    Button("Close") { dismiss() }
                    Spacer()
                    if retained != nil {
                        Button("Retry in new folder") { start(retry: true) }.disabled(!canStart)
                    }
                    Button("Create") { start(retry: false) }.buttonStyle(.borderedProminent).disabled(!canStart)
                }
            }
        }.padding(20).frame(width: 500).interactiveDismissDisabled(task != nil)
    }

    private var canStart: Bool { parent != nil && name.nilIfBlank != nil && (kind == .repository || remote.nilIfBlank != nil) }

    @MainActor private func chooseParent() async {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = true
        panel.canChooseFiles = false
        panel.canCreateDirectories = true
        panel.prompt = "Choose parent folder"
        if await panel.begin() == .OK { parent = panel.url }
    }

    private func start(retry: Bool) {
        guard let parent, task == nil else { return }
        error = nil
        cancellationRequested = false
        task = Task { @MainActor in
            defer { task = nil }
            do {
                let destination = try await ProjectCreationClient().create(parent: parent, name: name, kind: kind,
                    remote: kind == .clone ? remote : nil, retryInNewFolder: retry)
                didCreate(destination)
                dismiss()
            } catch {
                self.error = error.localizedDescription
                retained = (error as? ProjectCreationFailure)?.retainedDirectory
            }
        }
    }
}
