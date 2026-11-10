import SwiftUI
import CryptoKit
import MemexExecutionHostCore

/// A remote path is an opaque host identifier, never a local filesystem target.
struct RemoteWorkspacePanel: View {
    let connection: ExecutionHostConnection
    let workspaceID: String
    let panel: Store.WorkspacePanel
    @State private var capabilities: Set<String> = []
    @State private var error: String?

    var body: some View {
        VStack(spacing: 0) {
            Text("\(connection.machineID) · \(workspaceID)").font(.caption).foregroundStyle(.secondary)
                .textSelection(.enabled).padding(8)
            if let error { Text(error).foregroundStyle(.red).textSelection(.enabled).padding() }
            if panel == .files, capabilities.contains("workspace.files") {
                RemoteWorkspaceFiles(connection: connection, workspaceID: workspaceID)
            } else if panel == .changes, capabilities.contains("workspace.diff") {
                RemoteWorkspaceDiff(connection: connection, workspaceID: workspaceID)
            } else if panel == .terminal, capabilities.contains("workspace.terminal") {
                RemoteWorkspaceTerminal(connection: connection, workspaceID: workspaceID)
            } else {
                ContentUnavailableView("Host workspace panel unavailable", systemImage: "network",
                    description: Text("This paired host must advertise support for the selected workspace operation."))
            }
        }.id(connection.id + "\0" + workspaceID)
        .task(id: connection.id + "\0" + workspaceID) {
            capabilities = []; error = nil
            do {
                let info = try await ExecutionHostConnections.shared.client(connection).call("host.info")
                guard info["hostId"].string == connection.id else { throw HostFailure("wrong_host", "The endpoint's host identity changed. Pair it again explicitly.") }
                capabilities = Set(info["capabilities"].array.compactMap(\.string))
            } catch { self.error = error.localizedDescription }
        }
    }
}

private struct RemoteWorkspaceFiles: View {
    let connection: ExecutionHostConnection
    let workspaceID: String
    @State private var directory = ""
    @State private var entries: [HostValue] = []
    @State private var path: String?
    @State private var text = ""
    @State private var revision = ""
    @State private var savedText = ""
    @State private var error: String?
    @State private var busy = false
    @State private var drafts: [String: (String, String, String)] = [:]
    @State private var confirmingReload = false

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack {
                Button("Up", systemImage: "arrow.up") {
                    directory = directory.split(separator: "/").dropLast().joined(separator: "/")
                    run { try await list() }
                }.disabled(directory.isEmpty || busy)
                Text(directory.isEmpty ? "/" : directory).lineLimit(1).truncationMode(.middle)
                Spacer()
                Button("Refresh", systemImage: "arrow.clockwise") { run { try await list() } }.disabled(busy)
            }
            List(Array(entries.enumerated()), id: \.offset) { _, entry in
                Button {
                    guard let next = entry["path"].string else { return }
                    if entry["directory"].bool == true { directory = next; run { try await list() } }
                    else { run { try await open(next) } }
                } label: {
                    Label(entry["name"].string ?? "", systemImage: entry["directory"].bool == true ? "folder" : "doc.text")
                }.buttonStyle(.plain).disabled(busy)
            }.frame(minHeight: 100, idealHeight: 160, maxHeight: 220)
            if let path {
                HStack {
                    Text(path).font(.caption.monospaced()).lineLimit(1).truncationMode(.middle)
                    Spacer()
                    Button("Save") { run { try await save(path) } }.disabled(busy || text == savedText)
                    Button("Reload host file") {
                        if text != savedText { confirmingReload = true }
                        else { run { try await open(path, reload: true) } }
                    }.disabled(busy)
                }
                TextEditor(text: $text).font(.system(.body, design: .monospaced))
                Text("UTF-8 files up to 2 MiB. Reload replaces this draft; Save refuses a changed host revision.")
                    .font(.caption).foregroundStyle(.secondary)
            }
            if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
        }.padding(8).task { run { try await list() } }
        .onChange(of: text) { _, _ in persistDraft() }
        .confirmationDialog("Discard this local draft and reload the host file?", isPresented: $confirmingReload) {
            Button("Discard draft and reload", role: .destructive) {
                guard let path else { return }
                run { try await open(path, reload: true); persistDraft() }
            }
        }
    }
    private func list() async throws {
        entries = try await ExecutionHostConnections.shared.client(connection).call("workspace.files", params: ["workspaceId": .string(workspaceID), "path": .string(directory)]).array
    }
    private func open(_ next: String, reload: Bool = false) async throws {
        if let path { drafts[path] = (text, revision, savedText) }
        if !reload, let draft = drafts[next] { path = next; text = draft.0; revision = draft.1; savedText = draft.2; return }
        if !reload, let draft = try RemoteWorkspaceDrafts.shared.read(hostID: connection.id, workspaceID: workspaceID, path: next) {
            path = next; text = draft.text; revision = draft.revision; savedText = draft.original; return
        }
        let file = try await ExecutionHostConnections.shared.client(connection).call("workspace.file.read", params: ["workspaceId": .string(workspaceID), "path": .string(next)])
        guard let content = file["text"].string, let rev = file["revision"].string else { throw HostFailure("response", "Incomplete file response") }
        path = next; text = content; savedText = content; revision = rev
        drafts[next] = (content, rev, content)
    }
    private func save(_ path: String) async throws {
        let value = text
        let result = try await ExecutionHostConnections.shared.client(connection).call("workspace.file.write", params: ["workspaceId": .string(workspaceID), "path": .string(path), "text": .string(value), "revision": .string(revision)], mutation: true)
        guard let rev = result["revision"].string else { throw HostFailure("response", "Host did not return the saved revision") }
        revision = rev; savedText = value; drafts[path] = (text, revision, savedText)
        persistDraft()
    }
    private func persistDraft() {
        guard let path, !revision.isEmpty else { return }
        do { try RemoteWorkspaceDrafts.shared.save(.init(text: text, revision: revision, original: savedText), hostID: connection.id, workspaceID: workspaceID, path: path) }
        catch { self.error = "Cannot preserve the local draft: " + error.localizedDescription }
    }
    private func run(_ body: @escaping @MainActor () async throws -> Void) {
        guard !busy else { return }; busy = true; error = nil
        Task { defer { busy = false }; do { try await body() } catch { self.error = error.localizedDescription } }
    }
}

private struct RemoteWorkspaceDiff: View {
    let connection: ExecutionHostConnection
    let workspaceID: String
    @State private var staged = false
    @State private var diff = ""
    @State private var error: String?
    @State private var truncated = false
    @State private var revision = 0
    var body: some View {
        VStack {
            HStack {
                Picker("Changes", selection: $staged) { Text("Unstaged").tag(false); Text("Staged").tag(true) }.pickerStyle(.segmented)
                Button("Refresh", systemImage: "arrow.clockwise") { revision += 1 }
            }
            if truncated { Text("Diff exceeds 2 MiB; showing its beginning.").font(.caption).foregroundStyle(.secondary) }
            if let error { Text(error).foregroundStyle(.red) }
            ScrollView([.horizontal, .vertical]) { Text(diff.isEmpty ? "No changes" : diff).font(.system(.body, design: .monospaced)).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading) }
        }.padding(8).task(id: "\(staged)-\(revision)") {
            do {
                let response = try await ExecutionHostConnections.shared.client(connection).call("workspace.diff", params: ["workspaceId": .string(workspaceID), "staged": .bool(staged)])
                diff = response["text"].string ?? ""; truncated = response["truncated"].bool ?? false; error = nil
            } catch { self.error = error.localizedDescription }
        }
    }
}

private struct RemoteWorkspaceTerminal: View {
    let connection: ExecutionHostConnection
    let workspaceID: String
    @State private var output = ""
    @State private var input = ""
    @State private var cursor: Double = 0
    @State private var attached = false
    @State private var error: String?
    @State private var busy = false
    private var terminalID: String { SHA256.hash(data: Data((connection.id + "\0" + workspaceID).utf8)).map { String(format: "%02x", $0) }.joined() }
    var body: some View {
        VStack {
            HStack {
                Button(attached ? "Reconnect" : "Open terminal") { run { try await operation("open"); attached = true } }.disabled(busy)
                Button("Interrupt") { run { try await operation("input", ["input": .string("\u{3}")]) } }.disabled(!attached || busy)
                Button("Close shell") { run { try await operation("close"); attached = false } }.disabled(!attached || busy)
            }
            Text("Host-owned shell; output and commands use a plain-text PTY. Full-screen terminal applications are not supported in this viewer.")
                .font(.caption).foregroundStyle(.secondary)
            ScrollView { Text(output).font(.system(.body, design: .monospaced)).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading) }
            HStack {
                TextField("Command", text: $input).onSubmit(send).disabled(!attached || busy)
                Button("Run", action: send).disabled(!attached || busy || input.isEmpty)
            }
            if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
        }.padding(8).task(id: attached) {
            guard attached else { return }
            do {
                while !Task.isCancelled { try await operation("read"); try await Task.sleep(for: .milliseconds(500)) }
            } catch is CancellationError {} catch { self.error = error.localizedDescription; attached = false }
        }
    }
    private func send() {
        let command = input
        run { try await operation("input", ["input": .string(command + "\n")]); input = "" }
    }
    private func operation(_ method: String, _ extra: [String: HostValue] = [:]) async throws {
        var fields = extra
        fields["workspaceId"] = .string(workspaceID); fields["terminalId"] = .string(terminalID); fields["cursor"] = .number(cursor)
        let response = try await ExecutionHostConnections.shared.client(connection).call("workspace.terminal." + method, params: fields, mutation: method != "read")
        guard method == "read" else { return } // Only the sequential read stream advances its cursor.
        if let inputError = response["inputError"].string { throw HostFailure("terminal_io", inputError) }
        if response["truncated"].bool == true { output += "\n[Earlier terminal output expired]\n" }
        if let encoded = response["data"].string, let bytes = Data(base64Encoded: encoded) { output += String(decoding: bytes, as: UTF8.self) }
        if output.utf8.count > 1024 * 1024 { output = String(output.suffix(250000)) }
        cursor = response["cursor"].number ?? cursor
    }
    private func run(_ body: @escaping @MainActor () async throws -> Void) {
        guard !busy else { return }; busy = true; error = nil
        Task { defer { busy = false }; do { try await body() } catch { self.error = error.localizedDescription } }
    }
}
