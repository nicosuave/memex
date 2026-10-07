import SwiftUI
import MemexExecutionHostCore

/// The viewer coordinates signed receipts; only hosts decide ownership. A lost
/// response stops the flow, and Continue inspects durable phases before acting.
@MainActor
struct RemoteWorkspaceHandoffView: View {
    let connection: ExecutionHostConnection
    let conversations: [HostValue]
    let workspaces: [HostValue]
    let onOpen: (Session, ExecutionHostConnection, String) -> Void
    @State private var conversationID = ""
    @State private var destinationHostID = ""
    @State private var destinationWorkspaceID = ""
    @State private var destinationWorkspaces: [HostValue] = []
    @State private var inspection: HostValue = .null
    @State private var handoffs: [HostValue] = []
    @State private var error: String?
    @State private var busy = false
    @State private var reviewing = false
    private var hosts: [ExecutionHostConnection] { ExecutionHostConnections.shared.hosts }
    private var destination: ExecutionHostConnection? { hosts.first { $0.id == destinationHostID } }

    var body: some View {
        Section("Move the same chat") {
            Text("Move an idle Codex chat with its native session and Git state. The source checkout and recovery snapshots remain. Other providers are offered only when they support a verified native transfer.")
                .font(.caption).foregroundStyle(.secondary)
            Picker("Conversation", selection: $conversationID) {
                Text("Choose a conversation").tag("")
                ForEach(Array(conversations.enumerated()), id: \.offset) { _, item in
                    Text(item["title"].string ?? "Conversation").tag(item["id"].string ?? "")
                }
            }
            Picker("Destination host", selection: $destinationHostID) {
                Text("Choose a paired host").tag("")
                ForEach(hosts) { host in Text(host.machineID).tag(host.id) }
            }
            Picker("Registered destination", selection: $destinationWorkspaceID) {
                Text("Choose a clean or retained checkout").tag("")
                ForEach(Array(destinationWorkspaces.enumerated()), id: \.offset) { _, item in
                    Text(item["path"].string ?? "Workspace").tag(item["id"].string ?? "")
                }
            }
            if let reason = inspection["reason"].string { Text(reason).font(.caption).foregroundStyle(.secondary) }
            Button("Review move…") { reviewing = true }
                .disabled(busy || inspection["available"].bool != true || destinationWorkspaceID.isEmpty || destination == nil)
            ForEach(Array(handoffs.enumerated()), id: \.offset) { _, record in
                VStack(alignment: .leading, spacing: 4) {
                    Text(record["source"]["title"].string ?? "Handoff")
                    Text("\(record["phase"].string ?? "Unknown") · \(record["id"].string ?? "")").font(.caption.monospaced())
                    if let failure = record["error"].string { Text(failure).font(.caption).foregroundStyle(.red).textSelection(.enabled) }
                    HStack {
                        if record["supersededBy"].string == nil, record["role"].string == "source", ["exported", "retiring", "committed"].contains(record["phase"].string ?? "") {
                            Button("Continue from receipts") { run { try await continueTransfer(record) } }
                        }
                        if record["supersededBy"].string == nil, record["role"].string == "source", ["exported", "retiring", "aborting", "needs_inspection", "preparing"].contains(record["phase"].string ?? "") {
                            Button("Cancel transfer and restore source") { run { try await abortTransfer(record) } }
                        }
                        if record["role"].string == "local", !["active", "restored"].contains(record["phase"].string ?? "") {
                            Button("Restore source") { run {
                                _ = try await call(connection, "recover", ["handoffId": record["id"]], mutation: true)
                            } }
                        }
                        if record["supersededBy"].string == nil, record["phase"].string == "active" {
                            Button("Open moved chat") { run { try open(record["destination"], host: connection) } }
                        }
                    }.buttonStyle(.borderless).disabled(busy)
                }
            }
            Button("Refresh handoff receipts") { run {} }.disabled(busy)
            if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
        }
        .task(id: connection.id) {
            conversationID = ""; destinationHostID = ""; destinationWorkspaceID = ""
            inspection = .null; error = nil
            await refresh()
        }
        .task(id: conversationID) {
            inspection = .null
            guard !conversationID.isEmpty else { return }
            do { inspection = try await call(connection, "inspect", ["conversationId": .string(conversationID)]) }
            catch { self.error = error.localizedDescription }
        }
        .task(id: destinationHostID) {
            destinationWorkspaces = []; destinationWorkspaceID = ""
            guard let destination else { return }
            do {
                try await verify(destination)
                destinationWorkspaces = destination.id == connection.id ? workspaces : try await ExecutionHostConnections.shared.client(destination).call("workspace.list").array
            } catch { self.error = error.localizedDescription }
        }
        .confirmationDialog("Move this same native chat?", isPresented: $reviewing, titleVisibility: .visible) {
            Button("Move chat and Git state") { run { try await move() } }
        } message: {
            Text("\(connection.machineID): \(inspection["conversation"]["cwd"].string ?? "") → \(destination?.machineID ?? ""): \(destinationWorkspaceID)\nNative session: \(inspection["conversation"]["nativeSessionID"].string ?? "")\nThe destination must be clean and unused, or exactly match its retained snapshot from an earlier move. Its previous state is retained; it becomes detached at the transferred commit. Dirty, staged, untracked and ignored source files are copied together within the size bound. No prompt is sent.")
        }
    }

    private func verify(_ host: ExecutionHostConnection) async throws {
        let info = try await ExecutionHostConnections.shared.client(host).call("host.info")
        guard info["hostId"].string == host.id, let key = host.handoffPublicKey, info["handoffPublicKey"].string == key else {
            throw HostFailure("handoff_identity", "Pair \(host.machineID) again to verify its handoff signing identity")
        }
        guard info["capabilities"].array.contains(.string("conversation.handoff")) else { throw HostFailure("capability_unavailable", "This host does not support native handoff") }
    }
    private func call(_ host: ExecutionHostConnection, _ method: String, _ fields: [String: HostValue] = [:], mutation: Bool = false) async throws -> HostValue {
        try await ExecutionHostConnections.shared.client(host).call("conversation.handoff." + method, params: fields, mutation: mutation)
    }
    private func move() async throws {
        guard let destination else { return }
        try await verify(connection); try await verify(destination)
        let id = UUID().uuidString
        var fields: [String: HostValue] = ["handoffId": .string(id), "conversationId": .string(conversationID),
            "destinationWorkspaceId": .string(destinationWorkspaceID)]
        if destination.id == connection.id {
            let result = try await call(connection, "move", fields, mutation: true)
            try open(result["destination"], host: connection)
        } else {
            fields["destinationHostId"] = .string(destination.id)
            fields["destinationPublicKey"] = .string(destination.handoffPublicKey!)
            let prepared = try await call(connection, "prepare", fields, mutation: true)
            try await continueTransfer(prepared)
        }
    }
    private func continueTransfer(_ record: HostValue) async throws {
        guard let target = hosts.first(where: { $0.id == record["destinationHostID"].string }) else {
            throw HostFailure("pairing", "Pair the exact destination host again before continuing this handoff")
        }
        try await verify(connection); try await verify(target)
        let fields = ["handoffId": record["id"]]
        var source = try await call(connection, "read", fields)
        if ["exported", "retiring"].contains(source["phase"].string ?? "") {
            let exported = try await call(connection, "export", fields)
            let installed = try await call(target, "install", ["handoffId": record["id"], "package": exported["package"],
                "certificate": exported["certificate"], "sourcePublicKey": .string(connection.handoffPublicKey!)], mutation: true)
            source = try await call(connection, "commit", ["handoffId": record["id"], "certificate": installed["certificate"]], mutation: true)
        }
        guard source["phase"].string == "committed" else { throw HostFailure("handoff_state", "Inspect the recorded source phase before continuing") }
        let active = try await call(target, "activate", ["handoffId": record["id"], "certificate": source["certificate"]], mutation: true)
        try open(active["destination"], host: target)
    }
    private func abortTransfer(_ record: HostValue) async throws {
        guard let target = hosts.first(where: { $0.id == record["destinationHostID"].string }) else {
            throw HostFailure("pairing", "The destination must be paired to confirm cancellation before restoring source ownership")
        }
        try await verify(connection); try await verify(target)
        let fields = ["handoffId": record["id"]]
        let decision = try await call(connection, "abort", fields, mutation: true)
        if decision["phase"].string == "aborted" { return }
        let exported = try await call(connection, "export", fields)
        let stopped = try await call(target, "abort", ["handoffId": record["id"], "package": exported["package"],
            "sourcePublicKey": .string(connection.handoffPublicKey!), "certificate": decision["certificate"]], mutation: true)
        _ = try await call(connection, "abort", ["handoffId": record["id"], "certificate": stopped["certificate"]], mutation: true)
    }
    private func open(_ value: HostValue, host: ExecutionHostConnection) throws {
        guard let id = value["id"].string else { throw HostFailure("response", "The host did not return a moved conversation") }
        onOpen(try RemoteConversationRuntime.session(value, connection: host), host, id)
    }
    private func refresh() async {
        do { handoffs = try await call(connection, "list").array }
        catch { self.error = error.localizedDescription }
    }
    private func run(_ body: @escaping @MainActor () async throws -> Void) {
        guard !busy else { return }; busy = true; error = nil
        Task { defer { busy = false }; do { try await body() } catch { self.error = error.localizedDescription }; await refresh() }
    }
}
