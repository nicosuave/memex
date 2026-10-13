import SwiftUI
import MemexExecutionHostCore

@MainActor
struct ExecutionHostConnectionsView: View {
    var onOpen: (Session, ExecutionHostConnection, String) -> Void
    var initialHostID: String? = nil
    @State private var connections = ExecutionHostConnections.shared
    @State private var selected: ExecutionHostConnection?
    @State private var endpoint = "http://127.0.0.1:6363"
    @State private var name = ""
    @State private var machine = "local"
    @State private var token = ""
    @State private var error: String?
    @State private var busy = false
    @State private var conversations: [HostValue] = []
    @State private var workspaces: [HostValue] = []
    @State private var providers: [String] = []
    @State private var workspace = ""
    @State private var provider = "codex"
    @State private var title = ""
    @State private var schedules: [HostValue] = []
    @State private var scheduleRuns: [HostValue] = []
    @State private var capabilities: Set<String> = []
    @State private var worktrees: [HostValue] = []
    @State private var supportsWorktrees = false
    @State private var repository = ""
    @State private var baseRef = ""
    @State private var cleanupWorktree: HostValue?
    @State private var pending: [HostRequest] = []
    @State private var receipt: String?
    @State private var reconciling: HostRequest?

    var body: some View {
        ScrollView {
            Form {
                Section("Execution hosts") {
                    Text("Agents run on the paired host and continue when this window closes. Register permitted folders when starting MemexExecutionHost.")
                        .foregroundStyle(.secondary)
                    ForEach(connections.hosts) { host in
                        HStack {
                            Button { run { try await load(host) } } label: {
                                VStack(alignment: .leading) {
                                    Text(host.name)
                                    Text("\(host.machineID) · \(host.endpoint.host ?? "")").font(.caption).foregroundStyle(.secondary)
                                }
                            }.buttonStyle(.plain)
                            Spacer()
                            if selected?.id == host.id { Image(systemName: "checkmark") }
                            Button("Remove pairing") {
                                do {
                                    try connections.remove(host)
                                    if selected?.id == host.id { selected = nil; conversations = []; schedules = []; pending = [] }
                                } catch { self.error = error.localizedDescription }
                            }.buttonStyle(.borderless)
                        }
                    }
                    TextField("Display name", text: $name)
                    TextField("Exact Memex machine identifier", text: $machine)
                    TextField("HTTPS server or loopback tunnel URL", text: $endpoint)
                    SecureField("Execution pairing token", text: $token)
                    Text("Use the host's private state/execution/control-token. History login tokens do not grant execution. Credentials are stored in Keychain.")
                        .font(.caption).foregroundStyle(.secondary)
                    Button("Pair host") {
                        run {
                            guard let url = URL(string: endpoint) else { throw HostFailure("endpoint", "Enter a valid host URL") }
                            let host = try await connections.pair(name: name, machineID: machine, endpoint: url,
                                                                  token: token.trimmingCharacters(in: .whitespacesAndNewlines))
                            token = ""
                            try await load(host)
                        }
                    }.disabled(busy || token.isEmpty)
                }
                ExecutionHostSSHOnboardingView { host in run { try await load(host) } }
                if let selected {
                    RemoteWorkspaceHandoffView(connection: selected, conversations: conversations,
                        workspaces: workspaces, onOpen: onOpen)
                    Section("Conversations on \(selected.name)") {
                        ForEach(conversations, id: \.identity) { conversation in
                            HStack {
                                VStack(alignment: .leading) {
                                    Text(conversation["title"].string ?? "Conversation")
                                    Text("\(conversation["provider"].string ?? "") · \(conversation["cwd"].string ?? "")").font(.caption).foregroundStyle(.secondary)
                                }
                                Spacer()
                                Button("Open") {
                                    do {
                                        let session = try RemoteConversationRuntime.session(conversation, connection: selected)
                                        onOpen(session, selected, conversation["id"].string ?? "")
                                    } catch { self.error = error.localizedDescription }
                                }
                            }
                        }
                        Picker("Workspace", selection: $workspace) {
                            Text("Choose a registered folder").tag("")
                            ForEach(workspaces, id: \.identity) { item in Text(item["path"].string ?? "").tag(item["id"].string ?? "") }
                        }
                        Picker("Provider", selection: $provider) { ForEach(providers, id: \.self) { Text($0).tag($0) } }
                        TextField("New conversation title", text: $title)
                        Button("Create conversation") {
                            run {
                                let client = try connections.client(selected)
                                let result = try await client.call("conversation.create", params: ["provider": .string(provider),
                                    "workspaceId": .string(workspace), "title": .string(title.isEmpty ? "New conversation" : title)], mutation: true)
                                try await load(selected)
                                let value = result["conversation"]
                                onOpen(try RemoteConversationRuntime.session(value, connection: selected), selected, value["id"].string ?? "")
                            }
                        }.disabled(busy || workspace.isEmpty || !providers.contains(provider))
                    }
                    if supportsWorktrees { worktreeSection(selected) }
                    ExecutionSchedulesView(schedules: schedules, runs: scheduleRuns, conversations: conversations,
                        workspaces: workspaces, providers: providers, capabilities: capabilities, busy: busy,
                        perform: { method, parameters in
                            guard !busy else { return false }
                            busy = true; error = nil
                            defer { busy = false }
                            do {
                                _ = try await connections.client(selected).call(method, params: parameters, mutation: true)
                                try await load(selected)
                                return true
                            } catch { self.error = error.localizedDescription; return false }
                        }, openConversation: { id in
                            guard let conversation = conversations.first(where: { $0["id"].string == id }) else {
                                error = "This run’s exact conversation is unavailable. Refresh the host to retry."; return
                            }
                            do { onOpen(try RemoteConversationRuntime.session(conversation, connection: selected), selected, id) }
                            catch { self.error = error.localizedDescription }
                        })
                        .id(selected.id)
                    if !pending.isEmpty {
                        Section("Outgoing commands awaiting a receipt") {
                            Text("Keep the original command identity when checking or retrying. An uncertain host receipt requires inspecting the native conversation.")
                                .font(.caption).foregroundStyle(.secondary)
                            ForEach(Array(pending.enumerated()), id: \.offset) { _, request in
                                VStack(alignment: .leading) {
                                    Text(request.method)
                                    Text(request.params["commandId"]?.string ?? "").font(.caption.monospaced())
                                    HStack {
                                        Button("Inspect receipt") {
                                            run {
                                                let result = try await connections.client(selected).call("command.read", params: ["commandId": request.params["commandId"] ?? .null])
                                                receipt = "\(result["status"].string ?? "Unknown"): \(result["error"].string ?? "Recorded by host")"
                                            }
                                        }
                                        Button("Retry exact command") {
                                            run {
                                                _ = try await connections.client(selected).call(request.method, params: request.params, mutation: true)
                                                try await load(selected)
                                            }
                                        }
                                        Button("Reconcile after inspection") { reconciling = request }
                                    }
                                }
                            }
                            if let receipt { Text(receipt).textSelection(.enabled) }
                        }
                    }
                    Button("Refresh host") { run { try await load(selected) } }.disabled(busy)
                }
                if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
                if busy { ProgressView() }
            }.formStyle(.grouped)
        }.frame(minWidth: 560, idealWidth: 680, minHeight: 560)
        .task(id: initialHostID) {
            guard let initialHostID, selected?.id != initialHostID else { return }
            guard let host = connections.hosts.first(where: { $0.id == initialHostID }) else {
                error = "Pair this schedule's execution host again to inspect its runs."
                return
            }
            busy = true
            defer { busy = false }
            do { try await load(host) }
            catch { self.error = error.localizedDescription }
        }
        .confirmationDialog("Mark this outgoing command reconciled?", isPresented: Binding(
            get: { reconciling != nil }, set: { if !$0 { reconciling = nil } }
        )) {
            Button("Mark reconciled") {
                guard let selected, let request = reconciling else { return }
                reconciling = nil
                run {
                    try await connections.client(selected).reconcile(request)
                    try await load(selected)
                }
            }
        } message: {
            Text("Confirm that you inspected the host receipt and native conversation. This archives the saved request without sending it again.")
        }
        .confirmationDialog("Remove this unused, clean checkout?", isPresented: Binding(
            get: { cleanupWorktree != nil }, set: { if !$0 { cleanupWorktree = nil } }
        )) {
            Button("Remove clean checkout", role: .destructive) {
                guard let tree = cleanupWorktree else { return }
                cleanupWorktree = nil
                worktreeAction("cleanup", tree)
            }
        } message: { Text("Removal refuses dirty or ignored files and retained chat references. Its branch and commits remain for reattachment.") }
    }

    private func load(_ host: ExecutionHostConnection) async throws {
        let client = try connections.client(host)
        let info = try await client.call("host.info")
        guard info["hostId"].string == host.id else { throw HostFailure("wrong_host", "The endpoint's execution identity changed. Pair it explicitly again.") }
        let nextConversations = try await client.call("conversation.list").array
        let nextWorkspaces = try await client.call("workspace.list").array
        let nextSchedules = try await client.call("schedule.list").array
        let nextCapabilities = Set(info["capabilities"].array.compactMap(\.string))
        let nextRuns = nextCapabilities.contains("schedules.runs") ? try await client.call("schedule.runs").array : []
        let nextPending = try await client.pending()
        let nextWorktrees = info["capabilities"].array.contains(.string("worktree.lifecycle")) ? try await client.call("worktree.list").array : []
        selected = host; conversations = nextConversations; workspaces = nextWorkspaces; schedules = nextSchedules; pending = nextPending
        worktrees = nextWorktrees; capabilities = nextCapabilities; scheduleRuns = nextRuns
        supportsWorktrees = info["capabilities"].array.contains(.string("worktree.lifecycle"))
        providers = info["providers"].array.compactMap(\.string)
        if !providers.contains(provider) { provider = providers.first ?? "" }
        if !workspaces.contains(where: { $0["id"].string == workspace }) { workspace = workspaces.first?["id"].string ?? "" }
        if !workspaces.contains(where: { $0["id"].string == repository && $0["worktreeID"] == .null }) {
            repository = workspaces.first(where: { $0["worktreeID"] == .null })?["id"].string ?? ""
        }
    }
    @ViewBuilder private func worktreeSection(_ host: ExecutionHostConnection) -> some View {
        Section("Managed worktrees") {
            Text("Create from an explicitly registered repository root. Archive keeps every file. Cleanup refuses dirty or ignored files and retained chat references.")
                .font(.caption).foregroundStyle(.secondary)
            Picker("Repository", selection: $repository) {
                Text("Choose a registered repository").tag("")
                ForEach(workspaces.filter { $0["worktreeID"] == .null }, id: \.identity) { item in
                    Text(item["path"].string ?? "").tag(item["id"].string ?? "")
                }
            }
            TextField("Base ref (blank uses recorded default)", text: $baseRef)
            Button("Create worktree") {
                run {
                    var parameters: [String: HostValue] = ["workspaceId": .string(repository)]
                    if !baseRef.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty { parameters["baseRef"] = .string(baseRef) }
                    let created = try await connections.client(host).call("worktree.create", params: parameters, mutation: true)
                    try await load(host)
                    workspace = created["workspaceId"].string ?? workspace
                }
            }.disabled(busy || repository.isEmpty)
            ForEach(worktrees, id: \.identity) { tree in
                VStack(alignment: .leading) {
                    Text(tree["branch"].string ?? tree["id"].string ?? "Worktree")
                    Text(tree["path"].string ?? "").font(.caption).textSelection(.enabled)
                    if let failure = tree["failure"].string { Text(failure).font(.caption).foregroundStyle(.red) }
                    Text(tree["removed"].bool == true ? "Checkout removed" : tree["archived"].bool == true ? "Archived · files retained" : tree["state"].string ?? "")
                        .font(.caption).foregroundStyle(.secondary)
                    if !tree["referencedBy"].array.isEmpty { Text("Retained chat references: \(tree["referencedBy"].array.count)").font(.caption) }
                    HStack {
                        if tree["removed"].bool == true { Button("Reattach") { worktreeAction("reattach", tree) } }
                        else {
                            Button(tree["archived"].bool == true ? "Unarchive" : "Archive") {
                                worktreeAction("archive", tree, ["archived": .bool(tree["archived"].bool != true)])
                            }
                            Button("Remove clean checkout…", role: .destructive) { cleanupWorktree = tree }
                                .disabled(!tree["referencedBy"].array.isEmpty)
                        }
                    }.buttonStyle(.borderless).disabled(busy || tree["state"].string != "ready")
                }
            }
        }
    }

    private func worktreeAction(_ action: String, _ tree: HostValue, _ fields: [String: HostValue] = [:]) {
        guard let selected else { return }
        run {
            var parameters = fields; parameters["worktreeId"] = tree["id"]
            _ = try await connections.client(selected).call("worktree." + action, params: parameters, mutation: true)
            try await load(selected)
        }
    }
    private func run(_ operation: @escaping @MainActor () async throws -> Void) {
        guard !busy else { return }
        busy = true; error = nil
        Task {
            defer { busy = false }
            do { try await operation() } catch { self.error = error.localizedDescription }
        }
    }
}

private extension HostValue { var identity: String { self["id"].string ?? "" } }
