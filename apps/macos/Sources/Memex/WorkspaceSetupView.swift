import SwiftUI

struct WorkspaceSetupView: View {
    let directory: URL
    let script: String
    @Environment(\.dismiss) private var dismiss
    @State private var operation: Task<Void, Never>?
    @State private var output = ""
    @State private var error: String?
    @State private var cancelled = false

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Run project setup").font(.headline)
            Text(directory.path).font(.caption).textSelection(.enabled)
            ScrollView { Text(script).font(.system(.body, design: .monospaced)).textSelection(.enabled)
                .frame(maxWidth: .infinity, alignment: .leading) }.frame(maxHeight: 120)
            Text("Runs this command in the folder above with your user account. Starting or selecting a project does not run it automatically.")
                .font(.caption).foregroundStyle(.secondary)
            if !output.isEmpty {
                ScrollView { Text(output).font(.system(.caption, design: .monospaced)).textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading) }.frame(height: 180)
            }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled) }
            HStack {
                if operation != nil {
                    ProgressView().controlSize(.small)
                    Text(cancelled ? "Cancelling…" : "Running setup…").font(.caption)
                    Spacer()
                    Button("Cancel operation") { cancelled = true; operation?.cancel() }.disabled(cancelled)
                } else {
                    Button("Close") { dismiss() }
                    Spacer()
                    Button(output.isEmpty ? "Run setup" : "Run again") { run() }.buttonStyle(.borderedProminent)
                }
            }
        }.padding(20).frame(width: 560).interactiveDismissDisabled(operation != nil)
    }

    private func run() {
        guard operation == nil else { return }
        error = nil
        output = ""
        cancelled = false
        operation = Task { @MainActor in
            defer { operation = nil }
            do { output = try await WorkspaceSetupClient().run(directory: directory, script: script) }
            catch { self.error = error is CancellationError ? "Setup cancelled. Any files it created remain in place." : error.localizedDescription }
        }
    }
}
