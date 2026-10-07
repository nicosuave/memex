import AppKit
import SwiftUI

struct WorkspaceGitActionsView: View {
    let directory: URL
    var refreshKey: String = ""
    var didChange: () -> Void = {}
    var didCreatePullRequest: ((URL) -> Void)? = nil
    @State private var status: WorkspaceGitStatus?
    @State private var action: Action?
    @State private var message = ""
    @State private var title = ""
    @State private var bodyText = ""
    @State private var base = ""
    @State private var remote = "origin"
    @State private var busy = false
    @State private var error: String?
    @State private var result: String?
    private let client = WorkspaceGitClient()
    private enum Action: String, Identifiable { case commit, push, pullRequest; var id: String { rawValue } }

    var body: some View {
        HStack {
            Text(status?.branch.nilIfBlank ?? "Git").font(.caption).lineLimit(1)
            Menu("Git") {
                Button("Commit staged changes…") { action = .commit }.disabled(status?.hasStagedChanges != true)
                Button("Push branch…") { action = .push }.disabled(status?.branch.isEmpty != false)
                Button("Create draft pull request…") { action = .pullRequest }.disabled(status?.branch.isEmpty != false)
                Button("Refresh status") { Task { await refresh() } }
            }.disabled(busy || status == nil)
            if busy { ProgressView().controlSize(.small) }
            if let result { Text(result).font(.caption).lineLimit(1).help(result) }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).lineLimit(2).help(error) }
        }
        .task(id: directory.path + refreshKey) { await refresh() }
        .sheet(item: $action) { action in
            VStack(alignment: .leading, spacing: 12) {
                switch action {
                case .commit:
                    Text("Commit staged changes").font(.headline)
                    Text("Only the Git index is committed. Unstaged and untracked files stay outside this commit.")
                        .font(.caption).foregroundStyle(.secondary)
                    TextField("Commit message", text: $message, axis: .vertical).lineLimit(3...8)
                case .push:
                    Text("Push \(status?.branch ?? "branch")").font(.headline)
                    Picker("Remote", selection: $remote) {
                        ForEach(status?.remotes ?? [], id: \.self) { Text($0).tag($0) }
                    }
                    Text("Pushes this branch without force. Authentication uses your configured Git tools.").font(.caption)
                case .pullRequest:
                    Text("Create draft pull request").font(.headline)
                    Text("Push the branch first. This action uses the origin repository and your configured GitHub CLI.").font(.caption)
                    TextField("Title", text: $title)
                    TextField("Base branch", text: $base)
                    TextEditor(text: $bodyText).frame(minHeight: 140)
                }
                if let error { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
                HStack {
                    Spacer()
                    Button("Cancel") { self.action = nil }.disabled(busy)
                    Button(action == .commit ? "Commit" : action == .push ? "Push" : "Create draft") {
                        Task { await execute(action) }
                    }.buttonStyle(.borderedProminent).disabled(busy || !valid(action))
                }
            }.padding(20).frame(width: 480).interactiveDismissDisabled(busy)
        }
    }

    private func valid(_ action: Action) -> Bool {
        switch action {
        case .commit: message.nilIfBlank != nil
        case .push: status?.remotes.contains(remote) == true
        case .pullRequest: title.nilIfBlank != nil && base.nilIfBlank != nil
        }
    }

    @MainActor private func refresh() async {
        do {
            status = try await client.status(directory: directory)
            if let status, !status.remotes.contains(remote) { remote = status.remotes.first ?? "" }
            error = nil
        } catch { self.error = error.localizedDescription }
    }

    @MainActor private func execute(_ action: Action) async {
        busy = true
        error = nil
        defer { busy = false }
        do {
            switch action {
            case .commit:
                let commit = try await client.commit(directory: directory, message: message)
                result = "Committed \(commit.prefix(8))"
                message = ""
            case .push:
                try await client.push(directory: directory, remote: remote)
                result = "Pushed to \(remote)"
            case .pullRequest:
                let url = try await client.createPullRequest(directory: directory, title: title, body: bodyText, base: base)
                result = url.absoluteString
                if let didCreatePullRequest { didCreatePullRequest(url) }
                else { NSWorkspace.shared.open(url) }
            }
            self.action = nil
            didChange()
            await refresh()
        } catch { self.error = error.localizedDescription }
    }
}
