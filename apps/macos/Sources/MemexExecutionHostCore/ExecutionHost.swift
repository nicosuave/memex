import Foundation
import Darwin

public final class ExecutionHost: @unchecked Sendable {
    struct Workspace: Codable { var id: String; var path: String; var worktreeID: String?; var repositoryWorkspaceID: String? }
    struct Receipt: Codable {
        var request: HostValue
        var status: String
        var result: HostValue?
        var error: String?
    }
    struct Queued: Codable {
        var command: HostedCommand
        var status: String
        var error: String?
    }
    struct Schedule: Codable {
        var id: String
        var conversationID: String
        var prompt: String
        var intervalSeconds: Double?
        var wallClock: HostWallClockSchedule?
        var nextRunAt: Date
        var paused: Bool
        var lastCommandID: String?
        var lastError: String?
        var lastSkippedAt: Date?
        var newConversation: HostScheduleTarget?
        var eventName: String?
        var notificationPolicy: String?
    }
    struct Catalog: Codable {
        var schemaVersion = 1
        var hostID: String
        var workspaces: [Workspace] = []
        var conversations: [HostedConversation] = []
        var receipts: [String: Receipt] = [:]
        var queue: [Queued] = []
        var heldConversations: Set<String> = []
        var schedules: [Schedule] = []
        var scheduleRuns: [HostScheduleRun]?
        var handoffs: [HostHandoffRecord]?
    }

    public let directory: URL
    public var hostID: String { lock.withLock { catalog.hostID } }
    private let lock = NSRecursiveLock()
    private let provider: any ExecutionProvider
    private let worktrees: ManagedWorkspaceStore
    private let workspaceAccess = RemoteWorkspaceAccess()
    private let handoffSigner: HostHandoffSigner
    private var catalog: Catalog
    private let now: () -> Date

    public init(directory: URL, workspaceRoots: [URL], providerFactory: (String) throws -> any ExecutionProvider,
                now: @escaping () -> Date = Date.init) throws {
        self.directory = directory
        self.now = now
        worktrees = ManagedWorkspaceStore(managedRoot: directory.appendingPathComponent("worktrees"))
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                                                attributes: [.posixPermissions: 0o700])
        try Self.requirePrivateDirectory(directory)
        handoffSigner = try HostHandoffSigner(directory: directory)
        let file = directory.appendingPathComponent("catalog.json")
        if FileManager.default.fileExists(atPath: file.path) {
            catalog = try JSONDecoder().decode(Catalog.self, from: Data(contentsOf: file))
            guard catalog.schemaVersion == 1 else { throw HostFailure("version", "Unsupported execution catalog version") }
        } else { catalog = Catalog(hostID: UUID().uuidString) }
        let roots = try workspaceRoots.map { url -> Workspace in
            let canonical = url.standardizedFileURL.resolvingSymlinksInPath()
            var isDirectory: ObjCBool = false
            guard FileManager.default.fileExists(atPath: canonical.path, isDirectory: &isDirectory), isDirectory.boolValue else {
                throw HostFailure("workspace", "Registered workspace does not exist: \(canonical.path)")
            }
            return Workspace(id: canonical.path, path: canonical.path)
        }
        // Startup arguments are the authority. Removing a grant never leaves a stale root authorized.
        catalog.workspaces = roots
        for key in catalog.receipts.keys where catalog.receipts[key]?.status == "executing" {
            catalog.receipts[key]?.status = "uncertain"
            catalog.receipts[key]?.error = "Host restarted before command outcome was recorded; inspect native history before retrying."
        }
        for index in catalog.queue.indices where ["queued", "dispatching"].contains(catalog.queue[index].status) {
            catalog.queue[index].status = catalog.queue[index].status == "dispatching" ? "uncertain" : "held"
            catalog.heldConversations.insert(catalog.queue[index].command.conversationID)
        }
        if catalog.scheduleRuns != nil {
            for index in catalog.scheduleRuns!.indices where catalog.scheduleRuns![index].status == "creating" {
                catalog.scheduleRuns![index].status = "uncertain"
                catalog.scheduleRuns![index].error = "Host restarted while creating the run's native conversation. Inspect it before starting another run."
            }
        }
        provider = try providerFactory(catalog.hostID)
        if catalog.handoffs != nil {
            for index in catalog.handoffs!.indices {
                let handoff = catalog.handoffs![index]
                if handoff.supersededBy == nil && (handoff.fencesConversation || (handoff.role == "destination" && handoff.phase == "aborted" && handoff.destination != nil)) {
                    do { try provider.detachForHandoff(handoff.destination ?? handoff.source) }
                    catch { catalog.handoffs![index].error = "Native ownership fence could not be restored: " + error.localizedDescription }
                }
            }
        }
        try save()
    }

    public func handle(_ request: HostRequest) -> HostResponse {
        do {
            if request.method == "conversation.wait" { return HostResponse(id: request.id, result: try wait(request)) }
            return try lock.withLock { HostResponse(id: request.id, result: try route(request)) }
        } catch let error as HostFailure { return HostResponse(id: request.id, error: error) }
        catch { return HostResponse(id: request.id, error: HostFailure("host_error", error.localizedDescription)) }
    }

    private func authorize(_ request: HostRequest) throws {
        guard request.method == "host.info" || request.params["hostId"]?.string == catalog.hostID else {
            throw HostFailure("wrong_host", "This command does not identify the paired execution host.")
        }
    }

    private func route(_ request: HostRequest) throws -> HostValue {
        try authorize(request)
        let p = request.params
        if request.method.hasPrefix("conversation.handoff."), Self.handoffReadMethods.contains(request.method) {
            return try handoffRead(request)
        }
        if RemoteWorkspaceAccess.readMethods.contains(request.method) {
            return try workspaceAccess.handle(request, root: URL(fileURLWithPath: authorizedWorkspace(required(p, "workspaceId"))))
        }
        switch request.method {
        case "host.info": return .object([
            "hostId": .string(catalog.hostID), "schemaVersion": .number(1),
            "handoffPublicKey": .string(handoffSigner.publicKey),
            "providers": .array(provider.providers.map(HostValue.string)),
            "capabilities": .array((["conversation", "queue", "schedules", "schedules.wall_clock", "schedules.runs", "schedules.new_conversation", "schedules.events", "workspace.read", "worktree.lifecycle", "context_fork", "conversation.handoff"] + RemoteWorkspaceAccess.capabilities).map(HostValue.string)),
            "browserAvailable": .bool(FileManager.default.fileExists(atPath: directory.appendingPathComponent("desktop.sock").path)),
            "executionPersistsWithoutClients": .bool(true)
        ])
        case "workspace.list": return try .encoded(availableWorkspaces())
        case "worktree.list":
            let repository = p["workspaceId"]?.string
            if let repository { _ = try registeredWorkspace(repository) }
            return .array(try managedEntries().filter { repository == nil || $0.workspace.sourceDirectory.path == repository }.map(worktreeValue))
        case "conversation.list":
            return .array(try catalog.conversations.map { conversation in
                var value = try HostValue.encoded(conversation).object
                value["connected"] = .bool(provider.isConnected(conversation.id))
                value["queueHeld"] = .bool(catalog.heldConversations.contains(conversation.id))
                value["handoffBlocked"] = .bool(isHandoffFenced(conversation.id))
                return .object(value)
            })
        case "conversation.read": return try read(try required(p, "conversationId"))
        case "conversation.child.read":
            let c = try conversation(required(p, "conversationId"))
            _ = try authorizedWorkspace(c.workspaceID)
            return try provider.readChild(c, childID: required(p, "childId"))
        case "command.read":
            guard let receipt = catalog.receipts[try required(p, "commandId")] else { throw HostFailure("not_found", "Command receipt not found") }
            return try .encoded(receipt)
        case "schedule.list": return try .encoded(catalog.schedules)
        case "schedule.runs":
            return .array(try (catalog.scheduleRuns ?? []).filter { p["scheduleId"]?.string == nil || $0.scheduleID == p["scheduleId"]?.string }.reversed().map { run in
                var value = try HostValue.encoded(run).object
                value["needsAttention"] = .bool(run.needsAttention)
                return .object(value)
            })
        case "conversation.queue.list":
            let id = try required(p, "conversationId")
            _ = try conversation(id)
            return try .encoded(catalog.queue.filter { $0.command.conversationID == id })
        case "browser.describe": return try browser(request)
        default: return try mutate(request)
        }
    }

    /// Save the exact operation before any process boundary. Reusing a command ID with
    /// changed parameters is an error; a crash never turns an uncertain operation into a new send.
    private func mutate(_ request: HostRequest) throws -> HostValue {
        let allowed: Set<String> = ["conversation.create", "conversation.import", "conversation.resume", "conversation.send", "conversation.steer",
            "conversation.interrupt", "conversation.approval", "conversation.userInput", "conversation.model", "conversation.configuration",
            "conversation.queue.add", "conversation.queue.edit", "conversation.queue.cancel", "conversation.queue.reorder", "conversation.queue.resume",
            "conversation.queue.promote", "conversation.fork", "conversation.delegate", "schedule.upsert", "schedule.pause", "schedule.delete", "schedule.run", "browser.dispatch",
            "worktree.create", "worktree.archive", "worktree.reattach", "worktree.cleanup", "schedule.event", "schedule.run.read"]
        guard allowed.contains(request.method) || RemoteWorkspaceAccess.mutationMethods.contains(request.method) || Self.handoffMutationMethods.contains(request.method) else { throw HostFailure("method_not_found", "Unknown execution operation: \(request.method)") }
        let id = try required(request.params, "commandId")
        let identity: HostValue = .object(["method": .string(request.method), "params": .object(request.params)])
        if let existing = catalog.receipts[id] {
            guard existing.request == identity else { throw HostFailure("id_conflict", "Command ID was already used with different parameters") }
            if existing.status == "completed", let result = existing.result { return result }
            throw HostFailure(existing.status, existing.error ?? "Command delivery is still in progress; inspect its receipt.")
        }
        catalog.receipts[id] = Receipt(request: identity, status: "executing")
        try save()
        do {
            let result = try execute(request)
            catalog.receipts[id]?.status = "completed"
            catalog.receipts[id]?.result = result
            try save()
            return result
        } catch {
            // The provider may already have accepted a command. Keep the original ID and
            // explicit uncertainty even when an acknowledgement or persistence write fails.
            let rejectedCodes: Set<String> = ["invalid_params", "workspace_denied", "unsupported_provider", "not_found", "queue_state"]
            catalog.receipts[id]?.status = (error as? HostFailure).map { rejectedCodes.contains($0.code) } == true ? "rejected" : "uncertain"
            catalog.receipts[id]?.error = error.localizedDescription
            try save()
            throw error
        }
    }

    private func execute(_ request: HostRequest) throws -> HostValue {
        let p = request.params
        if Self.handoffMutationMethods.contains(request.method) { return try handoffMutation(request) }
        if let id = p["conversationId"]?.string { try requireHandoffAvailable(id) }
        if let workspace = p["workspaceId"]?.string { try requireWorkspaceNotMoving(workspace) }
        if RemoteWorkspaceAccess.mutationMethods.contains(request.method) {
            return try workspaceAccess.handle(request, root: URL(fileURLWithPath: authorizedWorkspace(required(p, "workspaceId"))))
        }
        switch request.method {
        case "worktree.create":
            let root = URL(fileURLWithPath: try registeredWorkspace(required(p, "workspaceId")))
            let command = CommandRun()
            guard let repository = try worktrees.inspect(directory: root, command: command), repository.root.path == root.path else {
                throw HostFailure("workspace_denied", "Worktree creation requires an explicitly registered repository root")
            }
            try FileManager.default.createDirectory(at: worktrees.managedRoot, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
            try Self.requirePrivateDirectory(worktrees.managedRoot)
            let workspace = try worktrees.prepare(directory: root, newWorktree: true, baseRef: p["baseRef"]?.string, command: command)
            return try worktreeValue(ManagedWorkspaceEntry(workspace: workspace, archived: false, removed: false))
        case "worktree.archive", "worktree.reattach", "worktree.cleanup":
            let id = try required(p, "worktreeId")
            guard let entry = try managedEntries().first(where: { $0.id == id }) else { throw HostFailure("not_found", "Managed worktree is not registered under a currently granted repository") }
            _ = try registeredWorkspace(entry.workspace.sourceDirectory.path)
            let command = CommandRun()
            switch request.method {
            case "worktree.archive": try worktrees.setArchived(entry.workspace, archived: p["archived"]?.bool ?? true, command: command)
            case "worktree.reattach": _ = try worktrees.reattach(entry.workspace, command: command)
            default:
                // Every retained native conversation is a reference, including a
                // disconnected one with schedules or queued work. Removing the
                // checkout must never silently break its future resume location.
                let references = catalog.conversations.map { URL(fileURLWithPath: $0.cwd) }
                try worktrees.removeCleanCheckout(entry.workspace, otherWorkspaceDirectories: references, isBusy: false, command: command)
            }
            guard let updated = try managedEntries().first(where: { $0.id == id }) else { throw HostFailure("not_found", "Worktree recovery record is unavailable") }
            return try worktreeValue(updated)
        case "browser.dispatch": return try browser(request)
        case "conversation.import":
            let workspace = try required(p, "workspaceId")
            let cwd = try authorizedWorkspace(workspace)
            let chosen = try required(p, "provider")
            let native = try required(p, "nativeSessionId")
            guard !(catalog.handoffs ?? []).contains(where: { $0.source.nativeSessionID == native && $0.source.provider == chosen && ($0.fencesConversation || ($0.role == "destination" && $0.phase == "aborted")) }) else {
                throw HostFailure("handoff_ownership", "This native session has a retained handoff ownership fence")
            }
            let sourcePath = try required(p, "sourcePath")
            if let existing = catalog.conversations.first(where: { $0.provider == chosen && $0.nativeSessionID == native && $0.transcriptPath == sourcePath }) {
                guard existing.workspaceID == workspace else { throw HostFailure("identity_conflict", "Native session is already bound to another workspace") }
                return .object(["conversation": try .encoded(existing)])
            }
            guard !catalog.conversations.contains(where: { $0.provider == chosen && $0.nativeSessionID == native }) else {
                throw HostFailure("identity_conflict", "A different native transcript already uses this provider session ID; its original binding is preserved")
            }
            let imported = try provider.importConversation(id: "host-" + required(p, "commandId"), provider: chosen,
                nativeSessionID: native, sourcePath: sourcePath, workspaceID: workspace, cwd: cwd,
                title: p["title"]?.string ?? native)
            catalog.conversations.append(imported)
            return .object(["conversation": try .encoded(imported)])
        case "conversation.create", "conversation.fork", "conversation.delegate":
            let parent = try p["conversationId"]?.string.map(conversation)
            guard request.method == "conversation.create" || parent != nil else {
                throw HostFailure("invalid_params", "conversationId is required for a fork or delegation")
            }
            let workspace = p["workspaceId"]?.string ?? parent?.workspaceID
            guard let workspace else { throw HostFailure("invalid_params", "workspaceId is required") }
            let cwd = try authorizedWorkspace(workspace)
            let chosen = p["provider"]?.string ?? parent?.provider ?? "codex"
            guard provider.providers.contains(chosen) else { throw HostFailure("unsupported_provider", "Provider is not configured on this host") }
            let id = "host-" + (try required(p, "commandId"))
            var created = try provider.create(id: id, provider: chosen, workspaceID: workspace, cwd: cwd,
                                              title: p["title"]?.string ?? "New conversation")
            created.parentID = parent?.id
            catalog.conversations.append(created)
            try save()
            if request.method != "conversation.create" {
                guard let parent else { throw HostFailure("invalid_params", "conversationId is required for a fork or delegation") }
                // Context handoff is explicit, not a fabricated native fork. The original
                // provider session remains untouched and both identities are recorded.
                let snapshot = try provider.read(parent)
                let messages = snapshot["thread"]["messages"].array
                let context = messages.suffix(100).compactMap { message -> String? in
                    guard let text = message["content"].string else { return nil }
                    return "\(message["role"].string ?? "message"): \(text)"
                }.joined(separator: "\n\n")
                guard context.utf8.count <= 512_000 else { throw HostFailure("context_too_large", "Select a smaller context before forking") }
                let text = "Context handed off from conversation \(parent.id):\n\n\(context)\n\n\(p["text"]?.string ?? "Continue from this context.")"
                let command = HostedCommand(id: "handoff-" + (try required(p, "commandId")), issuedAt: timestamp(),
                    conversationID: created.id, action: "prompt", text: text)
                catalog.queue.append(Queued(command: command, status: "queued"))
            }
            return .object(["conversation": try .encoded(created), "forkKind": parent == nil ? .null : .string("context_handoff")])
        case "conversation.resume":
            var c = try conversation(required(p, "conversationId"))
            _ = try authorizedWorkspace(c.workspaceID)
            if c.provider == "claude", let mode = p["claudePermissionMode"]?.string {
                guard ["auto", "default", "acceptEdits", "read-only", "dontAsk", "full-access"].contains(mode) else {
                    throw HostFailure("invalid_params", "Unsupported Claude permission mode")
                }
                c.claudePermissionMode = mode
            }
            if !provider.isConnected(c.id) { try provider.resume(c) }
            return try read(c.id)
        case "conversation.send", "conversation.steer", "conversation.interrupt", "conversation.approval", "conversation.userInput", "conversation.model", "conversation.configuration":
            let actions = ["conversation.send": "prompt", "conversation.steer": "steer", "conversation.interrupt": "cancel",
                           "conversation.approval": "approval", "conversation.userInput": "userInput", "conversation.model": "model", "conversation.configuration": "configuration"]
            let command = try command(p, action: actions[request.method]!)
            _ = try authorizedWorkspace(conversation(command.conversationID).workspaceID)
            if command.action == "cancel" {
                catalog.heldConversations.insert(command.conversationID)
                try save() // Stop holds the queue even when native interruption is rejected.
            }
            return try provider.perform(command)
        case "conversation.queue.add":
            let command = try command(p, action: "prompt")
            _ = try conversation(command.conversationID)
            catalog.queue.append(Queued(command: command, status: "queued"))
            return try .encoded(catalog.queue.last!)
        case "conversation.queue.resume":
            let id = try required(p, "conversationId")
            _ = try conversation(id)
            // Only held, never dispatching/uncertain, entries may be rearmed automatically.
            for i in catalog.queue.indices where catalog.queue[i].command.conversationID == id && catalog.queue[i].status == "held" {
                catalog.queue[i].status = "queued"
            }
            catalog.heldConversations.remove(id)
            return .bool(true)
        case "conversation.queue.edit", "conversation.queue.cancel", "conversation.queue.promote":
            let id = try required(p, "queuedCommandId")
            guard let index = catalog.queue.firstIndex(where: { $0.command.id == id }),
                  catalog.queue[index].command.conversationID == (try required(p, "conversationId")),
                  ["queued", "held"].contains(catalog.queue[index].status) else {
                throw HostFailure("queue_state", "Only an undispatched queue entry can be edited, cancelled, or promoted")
            }
            if request.method == "conversation.queue.cancel" { catalog.queue[index].status = "cancelled" }
            else if request.method == "conversation.queue.edit" {
                let text = try required(p, "text")
                catalog.queue[index].command.text = text
                if let replacement = p["promptContent"] {
                    catalog.queue[index].command.promptContent = replacement
                } else if let content = catalog.queue[index].command.promptContent {
                    var blocks = content.array
                    let primary: HostValue = .object(["type": .string("text"), "text": .string(text)])
                    if blocks.first?["type"].string == "text" { blocks[0] = primary }
                    else { blocks.insert(primary, at: 0) }
                    catalog.queue[index].command.promptContent = .array(blocks)
                }
            }
            else {
                _ = try authorizedWorkspace(conversation(catalog.queue[index].command.conversationID).workspaceID)
                catalog.queue[index].command.action = "steer"
                catalog.queue[index].status = "dispatching"
                try save()
                do { _ = try provider.perform(catalog.queue[index].command); catalog.queue[index].status = "dispatched" }
                catch {
                    catalog.queue[index].status = "uncertain"
                    catalog.queue[index].error = error.localizedDescription
                    catalog.heldConversations.insert(catalog.queue[index].command.conversationID)
                    throw error
                }
            }
            return try .encoded(catalog.queue[index])
        case "conversation.queue.reorder":
            let conversationID = try required(p, "conversationId")
            let ids = p["commandIds"]?.array.compactMap(\.string) ?? []
            let entries = catalog.queue.filter { $0.command.conversationID == conversationID && ["queued", "held"].contains($0.status) }
            guard Set(ids).count == ids.count, Set(ids) == Set(entries.map { $0.command.id }) else {
                throw HostFailure("invalid_params", "Supply every undispatched queue ID exactly once")
            }
            let ordered = ids.compactMap { id in entries.first { $0.command.id == id } }
            var next = 0
            for index in catalog.queue.indices where catalog.queue[index].command.conversationID == conversationID && ["queued", "held"].contains(catalog.queue[index].status) {
                catalog.queue[index] = ordered[next]; next += 1
            }
            return try .encoded(ordered)
        case "schedule.upsert":
            let id = try required(p, "scheduleId")
            let c: String
            let target: HostScheduleTarget?
            if let value = p["newConversation"] {
                guard p["conversationId"] == nil else { throw HostFailure("invalid_params", "Choose one schedule target") }
                let workspace = try required(value.object, "workspaceId")
                _ = try authorizedWorkspace(workspace)
                let chosen = try required(value.object, "provider")
                guard provider.providers.contains(chosen) else { throw HostFailure("unsupported_provider", "Provider is not configured on this host") }
                target = HostScheduleTarget(workspaceID: workspace, provider: chosen, title: value["title"].string ?? "Scheduled conversation")
                c = ""
            } else {
                c = try required(p, "conversationId")
                _ = try authorizedWorkspace(conversation(c).workspaceID)
                target = nil
            }
            let eventName = try p["eventName"].map { _ in try required(p, "eventName") }
            let notificationPolicy = p["notificationPolicy"]?.string ?? "attention"
            guard ["all", "attention", "never"].contains(notificationPolicy) else { throw HostFailure("invalid_params", "Unknown notification policy") }
            let interval: Double?
            let wallClock: HostWallClockSchedule?
            if eventName != nil {
                guard p["wallClock"] == nil, p["intervalSeconds"] == nil, p["nextRunAt"] == nil else { throw HostFailure("invalid_params", "Event schedules cannot also have a recurrence") }
                interval = nil; wallClock = nil
            } else if let value = p["wallClock"] {
                guard p["intervalSeconds"] == nil, p["nextRunAt"] == nil,
                      let localTime = value["localTime"].string, let zone = value["timeZone"].string else {
                    throw HostFailure("invalid_params", "A wallClock schedule requires localTime, weekdays and timeZone; omit intervalSeconds and nextRunAt")
                }
                let numbers = value["weekdays"].array.compactMap(\.number)
                guard numbers.count == value["weekdays"].array.count, numbers.allSatisfy({ $0.isFinite && $0.rounded() == $0 && (1...7).contains($0) }) else {
                    throw HostFailure("invalid_params", "weekdays must contain ISO weekday integers 1 through 7")
                }
                wallClock = try HostWallClockSchedule(localTime: localTime, weekdays: numbers.map(Int.init), timeZone: zone)
                interval = nil
            } else {
                let seconds = p["intervalSeconds"]?.number ?? 0
                guard seconds.isFinite, seconds >= 60, seconds <= 31_536_000 else {
                    throw HostFailure("invalid_params", "Schedule interval must be between 60 seconds and one year")
                }
                interval = seconds
                wallClock = nil
            }
            let previous = catalog.schedules.first { $0.id == id }
            let next: Date
            if let value = p["nextRunAt"] {
                guard let text = value.string,
                      let parsed = ISO8601DateFormatter().date(from: text) ?? Self.fractionalDate(text) else {
                    throw HostFailure("invalid_params", "nextRunAt must be an RFC3339 timestamp")
                }
                next = parsed
            } else if eventName != nil { next = .distantFuture }
            else if let previous, previous.intervalSeconds == interval, previous.wallClock == wallClock {
                next = previous.nextRunAt
            } else if let wallClock { next = try wallClock.next(after: now()) }
            else { next = now().addingTimeInterval(interval!) }
            let schedule = Schedule(id: id, conversationID: c, prompt: try required(p, "text"), intervalSeconds: interval,
                                    wallClock: wallClock, nextRunAt: next, paused: p["paused"]?.bool ?? previous?.paused ?? false,
                                    lastCommandID: previous?.lastCommandID, lastError: previous?.lastError, lastSkippedAt: previous?.lastSkippedAt,
                                    newConversation: target, eventName: eventName, notificationPolicy: notificationPolicy)
            catalog.schedules.removeAll { $0.id == id }; catalog.schedules.append(schedule)
            return try .encoded(schedule)
        case "schedule.pause", "schedule.delete", "schedule.run":
            let id = try required(p, "scheduleId")
            guard let index = catalog.schedules.firstIndex(where: { $0.id == id }) else { throw HostFailure("not_found", "Schedule not found") }
            if request.method == "schedule.delete" { catalog.schedules.remove(at: index); return .bool(true) }
            if request.method == "schedule.pause" { catalog.schedules[index].paused = p["paused"]?.bool ?? true }
            else { try enqueueSchedule(index, commandID: "schedule-manual-" + (try required(p, "commandId"))) }
            return try .encoded(catalog.schedules[index])
        case "schedule.event":
            let name = try required(p, "eventName")
            let eventID = try required(p, "eventId")
            let indices = catalog.schedules.indices.filter { catalog.schedules[$0].eventName == name && !catalog.schedules[$0].paused }
            for index in indices {
                let scheduleID = catalog.schedules[index].id
                try enqueueSchedule(index, commandID: "schedule-event-\(scheduleID.utf8.count):\(scheduleID)-\(eventID.utf8.count):\(eventID)")
            }
            return .number(Double(indices.count))
        case "schedule.run.read":
            let id = try required(p, "runId")
            guard let index = catalog.scheduleRuns?.firstIndex(where: { $0.id == id }) else { throw HostFailure("not_found", "Schedule run not found") }
            catalog.scheduleRuns![index].read = p["read"]?.bool ?? true
            return try .encoded(catalog.scheduleRuns![index])
        default: throw HostFailure("method_not_found", request.method)
        }
    }

    public func tick() throws {
        try lock.withLock {
            for index in catalog.schedules.indices where !catalog.schedules[index].paused && catalog.schedules[index].eventName == nil && catalog.schedules[index].nextRunAt <= now() {
                let schedule = catalog.schedules[index]
                let id = "schedule-\(schedule.id)-\(Int64(schedule.nextRunAt.timeIntervalSince1970))"
                if let wallClock = schedule.wallClock {
                    // A late fixed-time run is skipped, not replayed later in a burst.
                    // The one-minute grace allows normal scheduler latency.
                    if now().timeIntervalSince(schedule.nextRunAt) < 60 { try enqueueSchedule(index, commandID: id) }
                    else { catalog.schedules[index].lastSkippedAt = schedule.nextRunAt }
                    catalog.schedules[index].nextRunAt = try wallClock.next(after: now())
                } else if let interval = schedule.intervalSeconds {
                    try enqueueSchedule(index, commandID: id)
                    // Preserve legacy interval coalescing. Advancing and enqueueing
                    // share one durable write, so restart cannot duplicate an occurrence.
                    catalog.schedules[index].nextRunAt = now().addingTimeInterval(interval)
                } else { throw HostFailure("schedule_time", "Schedule has no recurrence") }
                try save()
            }
            for index in catalog.queue.indices where catalog.queue[index].status == "queued" {
                let command = catalog.queue[index].command
                guard !catalog.heldConversations.contains(command.conversationID), !isHandoffFenced(command.conversationID), provider.isConnected(command.conversationID) else { continue }
                let c = try conversation(command.conversationID)
                _ = try authorizedWorkspace(c.workspaceID)
                let snapshot = try provider.read(c)
                guard snapshot["ready"].bool == true, snapshot["running"].bool != true else { continue }
                catalog.queue[index].status = "dispatching"
                try save()
                do {
                    _ = try provider.perform(command)
                    catalog.queue[index].status = "dispatched"
                } catch {
                    catalog.queue[index].status = "uncertain"
                    catalog.queue[index].error = error.localizedDescription
                    catalog.heldConversations.insert(command.conversationID)
                }
                try save()
            }
            if catalog.scheduleRuns != nil {
                let beforeRuns = catalog.scheduleRuns
                for index in catalog.scheduleRuns!.indices {
                    let run = catalog.scheduleRuns![index]
                    guard let queued = catalog.queue.first(where: { $0.command.id == run.id }),
                          !["completed", "failed", "interrupted", "cancelled"].contains(run.status) else { continue }
                    let snapshot = try run.conversationID.flatMap { id in
                        provider.isConnected(id) ? try provider.read(conversation(id)) : nil
                    }
                    let queueStatus = queued.status == "queued" && catalog.heldConversations.contains(queued.command.conversationID) ? "held" : queued.status
                    catalog.scheduleRuns![index].reconcile(queueStatus: queueStatus, snapshot: snapshot, at: now())
                }
                if beforeRuns != catalog.scheduleRuns { try save() }
            }
        }
    }

    private func enqueueSchedule(_ index: Int, commandID: String) throws {
        let schedule = catalog.schedules[index]
        guard !(catalog.scheduleRuns ?? []).contains(where: { $0.id == commandID }),
              !catalog.queue.contains(where: { $0.command.id == commandID }) else { return }
        var run = HostScheduleRun(id: commandID, scheduleID: schedule.id, createdAt: now(), updatedAt: now(),
            status: "creating", notificationPolicy: schedule.notificationPolicy ?? "attention")
        catalog.scheduleRuns = (catalog.scheduleRuns ?? []) + [run]
        catalog.schedules[index].lastCommandID = commandID
        try save() // Persist occurrence identity before provider creation; never replay an uncertain create.
        do {
            let id: String
            if let target = schedule.newConversation {
                try requireWorkspaceNotMoving(target.workspaceID)
                let cwd = try authorizedWorkspace(target.workspaceID)
                let created = try provider.create(id: "host-" + commandID, provider: target.provider,
                    workspaceID: target.workspaceID, cwd: cwd, title: target.title)
                catalog.conversations.append(created)
                id = created.id
            } else {
                id = schedule.conversationID
                try requireHandoffAvailable(id)
                _ = try authorizedWorkspace(conversation(id).workspaceID)
            }
            run.conversationID = id
            run.status = catalog.heldConversations.contains(id) ? "held" : "queued"
            let command = HostedCommand(id: commandID, issuedAt: timestamp(), conversationID: id, action: "prompt", text: schedule.prompt)
            catalog.queue.append(Queued(command: command, status: run.status))
            catalog.schedules[index].lastError = nil
        } catch {
            run.status = "uncertain"; run.error = error.localizedDescription
            catalog.schedules[index].lastError = error.localizedDescription
        }
        catalog.scheduleRuns![catalog.scheduleRuns!.count - 1] = run
        try save()
    }

    private func browser(_ request: HostRequest) throws -> HostValue {
        let c = try conversation(required(request.params, "conversationId"))
        _ = try authorizedWorkspace(c.workspaceID)
        guard let source = c.transcriptPath else { throw HostFailure("capability_unavailable", "The provider has not established a native transcript identity for desktop attachment") }
        let desktopID = ["local", c.provider, c.nativeSessionID, source].joined(separator: "\u{1f}")
        var params: [String: HostValue] = ["conversationId": .string(desktopID)]
        if request.method == "browser.dispatch" {
            guard let payload = request.params["request"], payload["conversationID"].string == desktopID else {
                throw HostFailure("browser_scope", "Browser request does not match this hosted conversation's exact native identity")
            }
            params["request"] = payload
        }
        let response = try HostSocketClient.request(HostRequest(id: request.id, method: request.method, params: params), directory: directory)
        if let error = response.error { throw error }
        return response.result ?? .null
    }

    private func read(_ id: String) throws -> HostValue {
        var c = try conversation(id)
        _ = try authorizedWorkspace(c.workspaceID)
        var result = try provider.read(c).object
        // Retain only provider-confirmed modes, never an unacknowledged request.
        // The headless host can reconnect queued work without a desktop viewer.
        if c.provider == "claude", result["ready"]?.bool == true,
           result["controls"]?["pendingControlCommandIds"].array.isEmpty == true,
           let mode = result["controls"]?["configOptions"].array.first(where: { $0["id"].string == "permission_mode" })?["currentValue"].string,
           mode != c.claudePermissionMode,
           let index = catalog.conversations.firstIndex(where: { $0.id == id }) {
            c.claudePermissionMode = mode
            catalog.conversations[index] = c
            try save()
        }
        result["conversation"] = try .encoded(c)
        result["queueHeld"] = .bool(catalog.heldConversations.contains(id))
        result["handoffBlocked"] = .bool(isHandoffFenced(id))
        if isHandoffFenced(id) {
            result["ready"] = .bool(false); result["actions"] = .array([])
            result["warning"] = .string("This conversation is fenced for handoff. Inspect its handoff receipt before resuming work.")
        }
        result["queue"] = try .encoded(catalog.queue.filter { $0.command.conversationID == id })
        return .object(result)
    }

    private func wait(_ request: HostRequest) throws -> HostValue {
        let timeout = min(30, max(0, request.params["timeoutSeconds"]?.number ?? 20))
        let deadline = Date().addingTimeInterval(timeout)
        let after = request.params["afterSequence"]?.number
        repeat {
            let value = try lock.withLock { () throws -> HostValue in
                try authorize(request)
                return try read(required(request.params, "conversationId"))
            }
            if value["thread"]["snapshotSequence"].number != after || value["running"].bool != true || Date() >= deadline { return value }
            Thread.sleep(forTimeInterval: 0.1)
        } while true
    }

    private func command(_ p: [String: HostValue], action: String) throws -> HostedCommand {
        let issuedAt = try required(p, "issuedAt")
        guard ISO8601DateFormatter().date(from: issuedAt) != nil || Self.fractionalDate(issuedAt) != nil else { throw HostFailure("invalid_params", "issuedAt must be an RFC3339 timestamp") }
        return HostedCommand(id: try required(p, "commandId"), issuedAt: issuedAt, conversationID: try required(p, "conversationId"),
            action: action, text: p["text"]?.string ?? "", optionID: p["optionId"]?.string,
            requestID: p["requestId"]?.string, promptContent: p["promptContent"])
    }
    private func conversation(_ id: String) throws -> HostedConversation {
        guard let value = catalog.conversations.first(where: { $0.id == id }) else { throw HostFailure("not_found", "Conversation not found on this host") }
        return value
    }
    private func authorizedWorkspace(_ id: String) throws -> String {
        if catalog.workspaces.contains(where: { $0.id == id }) { return try registeredWorkspace(id) }
        guard let entry = try managedEntries().first(where: { $0.workspace.workingDirectory.path == id && !$0.removed && $0.workspace.state == .ready }) else {
            throw HostFailure("workspace_denied", "Workspace is not registered on this execution host")
        }
        _ = try registeredWorkspace(entry.workspace.sourceDirectory.path)
        try ManagedWorkspaceStore.validateManaged(entry.workspace, managedRoot: worktrees.managedRoot, requireCheckout: true, command: CommandRun())
        return entry.workspace.workingDirectory.path
    }
    private func registeredWorkspace(_ id: String) throws -> String {
        guard let workspace = catalog.workspaces.first(where: { $0.id == id }),
              URL(fileURLWithPath: workspace.path).resolvingSymlinksInPath().path == workspace.path else {
            throw HostFailure("workspace_denied", "Workspace is not registered on this execution host")
        }
        return workspace.path
    }
    private func managedEntries() throws -> [ManagedWorkspaceEntry] {
        if FileManager.default.fileExists(atPath: worktrees.managedRoot.path) {
            try Self.requirePrivateDirectory(worktrees.managedRoot)
        }
        return try worktrees.managedWorkspaces().filter { entry in
            let workspace = entry.workspace
            if workspace.state == .ready {
                try ManagedWorkspaceStore.validateManaged(workspace, managedRoot: worktrees.managedRoot, requireCheckout: false, command: CommandRun())
            }
            return workspace.repositoryRoot?.path == workspace.sourceDirectory.path
                && catalog.workspaces.contains(where: { $0.id == workspace.sourceDirectory.path })
        }
    }
    private func availableWorkspaces() throws -> [Workspace] {
        catalog.workspaces + (try managedEntries().filter { !$0.archived && !$0.removed && $0.workspace.state == .ready }.map { entry in
            Workspace(id: entry.workspace.workingDirectory.path, path: entry.workspace.workingDirectory.path,
                worktreeID: entry.id, repositoryWorkspaceID: entry.workspace.sourceDirectory.path)
        })
    }
    private func worktreeValue(_ entry: ManagedWorkspaceEntry) throws -> HostValue {
        let workspace = entry.workspace
        return .object(["id": .string(entry.id), "workspaceId": .string(workspace.workingDirectory.path),
            "repositoryWorkspaceId": .string(workspace.sourceDirectory.path), "path": .string(workspace.workingDirectory.path),
            "branch": workspace.branch.map(HostValue.string) ?? .null, "baseRef": workspace.baseRef.map(HostValue.string) ?? .null,
            "state": .string(workspace.state.rawValue), "failure": workspace.failure.map(HostValue.string) ?? .null,
            "archived": .bool(entry.archived), "removed": .bool(entry.removed),
            "referencedBy": .array(catalog.conversations.filter { $0.cwd == workspace.workingDirectory.path || $0.cwd.hasPrefix(workspace.workingDirectory.path + "/") }.map { .string($0.id) })])
    }
    private func required(_ p: [String: HostValue], _ key: String) throws -> String {
        let limit = key == "text" ? 1_048_576 : (["commandId", "scheduleId"].contains(key) ? 128 : 512)
        guard let value = p[key]?.string, !value.isEmpty, value.utf8.count <= limit,
              !value.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) && key != "text" }) else {
            throw HostFailure("invalid_params", "Missing or invalid \(key)")
        }
        return value
    }
    private func timestamp() -> String { now().ISO8601Format() }
    private static func fractionalDate(_ value: String) -> Date? {
        let format = ISO8601DateFormatter(); format.formatOptions.insert(.withFractionalSeconds); return format.date(from: value)
    }
    private func save() throws {
        let file = directory.appendingPathComponent("catalog.json")
        try JSONEncoder().encode(catalog).write(to: file, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
        let fd = Darwin.open(file.path, O_RDONLY | O_NOFOLLOW)
        guard fd >= 0 else { throw HostFailure("persistence", "Cannot open execution catalog for synchronization") }
        defer { Darwin.close(fd) }
        guard fsync(fd) == 0 else { throw HostFailure("persistence", "Cannot synchronize execution catalog") }
        let directoryFD = Darwin.open(directory.path, O_RDONLY)
        guard directoryFD >= 0 else { throw HostFailure("persistence", "Cannot synchronize execution directory") }
        defer { Darwin.close(directoryFD) }
        guard fsync(directoryFD) == 0 else { throw HostFailure("persistence", "Cannot synchronize execution directory") }
    }
    public static func requirePrivateDirectory(_ directory: URL) throws {
        var info = stat()
        guard lstat(directory.path, &info) == 0, (info.st_mode & S_IFMT) == S_IFDIR,
              info.st_uid == getuid(), info.st_mode & 0o077 == 0 else {
            throw HostFailure("permissions", "Execution directory must be owned by this user, mode 0700, and not a symlink")
        }
    }
}

private extension ExecutionHost {
    static let handoffReadMethods: Set<String> = ["conversation.handoff.inspect", "conversation.handoff.list", "conversation.handoff.read", "conversation.handoff.export"]
    static let handoffMutationMethods: Set<String> = ["conversation.handoff.prepare", "conversation.handoff.install", "conversation.handoff.commit", "conversation.handoff.activate", "conversation.handoff.abort", "conversation.handoff.move", "conversation.handoff.recover"]

    var handoffFiles: RemoteWorkspaceHandoff { RemoteWorkspaceHandoff(directory: directory.appendingPathComponent("handoffs")) }
    func isHandoffFenced(_ conversationID: String) -> Bool {
        (catalog.handoffs ?? []).contains { $0.source.id == conversationID && $0.fencesConversation }
    }
    func retainedWorkspace(_ conversationID: String, workspaceID: String) throws -> WorkspaceTransfer? {
        guard let prior = (catalog.handoffs ?? []).last(where: {
            $0.source.id == conversationID && $0.source.workspaceID == workspaceID
                && (($0.role == "source" && $0.phase == "committed") || ($0.role == "local" && $0.phase == "active"))
        }) else { return nil }
        return try loadHandoffPackage(prior).workspace
    }
    func requireLatestHandoff(_ index: Int) throws {
        let record = catalog.handoffs![index]
        guard record.supersededBy == nil, !(catalog.handoffs ?? []).dropFirst(index + 1).contains(where: {
            $0.source.id == record.source.id && !["aborted", "restored"].contains($0.phase)
        }) else { throw HostFailure("handoff_ownership", "A newer ownership operation supersedes this receipt") }
    }
    func supersedeEarlierHandoffs(before index: Int) {
        let current = catalog.handoffs![index]
        for earlier in 0..<index where catalog.handoffs![earlier].source.id == current.source.id {
            catalog.handoffs![earlier].supersededBy = current.id
        }
    }
    func requireHandoffAvailable(_ conversationID: String) throws {
        guard !isHandoffFenced(conversationID) else { throw HostFailure("handoff_ownership", "This conversation has a pending or committed handoff. Inspect the ownership receipt before continuing.") }
    }
    func requireWorkspaceNotMoving(_ workspaceID: String) throws {
        guard !(catalog.handoffs ?? []).contains(where: {
            !["committed", "active", "aborted", "restored"].contains($0.phase)
                && ($0.source.workspaceID == workspaceID || $0.destinationWorkspaceID == workspaceID)
        }) else { throw HostFailure("handoff_ownership", "This workspace is retained by an incomplete handoff") }
    }
    func handoffIndex(_ id: String) throws -> Int {
        guard let index = catalog.handoffs?.firstIndex(where: { $0.id == id }) else { throw HostFailure("not_found", "Handoff operation not found") }
        return index
    }
    func decodeHandoff<T: Decodable>(_ value: HostValue?, as type: T.Type) throws -> T {
        guard let value else { throw HostFailure("invalid_params", "Missing handoff payload") }
        return try JSONDecoder().decode(type, from: JSONEncoder().encode(value))
    }
    func handoffValue(_ record: HostHandoffRecord) throws -> HostValue {
        var result = try HostValue.encoded(record).object
        if ["exported", "installed", "committed", "aborting", "aborted"].contains(record.phase), record.digest != nil {
            result["certificate"] = try .encoded(handoffSigner.certificate(record, stage: record.phase))
        }
        return .object(result)
    }
    func requireIdleForHandoff(_ c: HostedConversation) throws {
        _ = try authorizedWorkspace(c.workspaceID)
        if let reason = provider.handoffUnavailableReason(c) { throw HostFailure("handoff_unsupported", reason) }
        let snapshot = try provider.read(c)
        guard snapshot["running"].bool == false, snapshot["operations"].array.isEmpty,
              snapshot["thread"]["pendingRequests"].array.isEmpty,
              !snapshot["thread"]["turns"].array.contains(where: { ["queued", "running", "cancelling", "cancel_requested"].contains($0["status"].string ?? "") }),
              !catalog.queue.contains(where: { $0.command.conversationID == c.id && ["queued", "held", "dispatching", "uncertain"].contains($0.status) }),
              !catalog.schedules.contains(where: { $0.conversationID == c.id && !$0.paused }),
              !workspaceAccess.hasRunningTerminal(root: URL(fileURLWithPath: c.cwd)) else {
            throw HostFailure("handoff_busy", "Finish active turns and requests, resolve queued work, pause schedules, and close host terminals before handoff")
        }
    }
    func requireHandoffDestination(_ workspaceID: String, excluding: String? = nil) throws -> URL {
        let root = URL(fileURLWithPath: try authorizedWorkspace(workspaceID))
        guard !catalog.conversations.contains(where: { $0.workspaceID == workspaceID && $0.id != excluding }),
              !workspaceAccess.hasRunningTerminal(root: root) else {
            throw HostFailure("handoff_busy", "Destination has another retained conversation or an open terminal")
        }
        return root
    }
    func handoffPackageURL(_ id: String) throws -> URL {
        guard UUID(uuidString: id) != nil else { throw HostFailure("invalid_params", "Handoff identity must be a UUID") }
        let path = directory.appendingPathComponent("handoff-packages")
        try FileManager.default.createDirectory(at: path, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        return path.appendingPathComponent(id + ".json")
    }
    func saveHandoffPackage(_ package: HostHandoffPackage) throws {
        let url = try handoffPackageURL(package.operationID)
        try HostHandoffSigner.encoded(package).write(to: url, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
        let file = try FileHandle(forWritingTo: url); try file.synchronize(); try file.close()
    }
    func loadHandoffPackage(_ record: HostHandoffRecord) throws -> HostHandoffPackage {
        let package = try JSONDecoder().decode(HostHandoffPackage.self, from: Data(contentsOf: handoffPackageURL(record.id)))
        guard try HostHandoffSigner.digest(package) == record.digest else { throw HostFailure("handoff_identity", "Retained handoff snapshot checksum changed") }
        return package
    }
    func handoffRead(_ request: HostRequest) throws -> HostValue {
        let p = request.params
        if request.method == "conversation.handoff.list" { return .array(try (catalog.handoffs ?? []).map(handoffValue)) }
        if request.method == "conversation.handoff.inspect" {
            let c = try conversation(required(p, "conversationId"))
            _ = try authorizedWorkspace(c.workspaceID)
            var reason = provider.handoffUnavailableReason(c)
            do { try requireHandoffAvailable(c.id); try requireIdleForHandoff(c) } catch { reason = error.localizedDescription }
            return .object(["conversation": try .encoded(c), "available": .bool(reason == nil), "reason": reason.map(HostValue.string) ?? .null,
                "sourceRetained": .bool(true), "publicKey": .string(handoffSigner.publicKey)])
        }
        let record = catalog.handoffs![try handoffIndex(required(p, "handoffId"))]
        _ = try authorizedWorkspace(record.role == "destination" ? record.destinationWorkspaceID : record.source.workspaceID)
        if request.method == "conversation.handoff.export" {
            guard record.role == "source", ["exported", "retiring", "committed", "aborting"].contains(record.phase) else {
                throw HostFailure("handoff_state", "The source does not have a verified fenced export")
            }
            return .object(["package": try .encoded(loadHandoffPackage(record)),
                "certificate": try .encoded(handoffSigner.certificate(record, stage: "exported"))])
        }
        return try handoffValue(record)
    }
    func handoffMutation(_ request: HostRequest) throws -> HostValue {
        let p = request.params
        let id = try required(p, "handoffId")
        guard UUID(uuidString: id) != nil else { throw HostFailure("invalid_params", "Handoff identity must be a UUID") }
        switch request.method {
        case "conversation.handoff.prepare", "conversation.handoff.move":
            let c = try conversation(required(p, "conversationId"))
            let destinationID = try required(p, "destinationWorkspaceId")
            let local = request.method == "conversation.handoff.move"
            let destinationHost = local ? catalog.hostID : try required(p, "destinationHostId")
            guard local || destinationHost != catalog.hostID else { throw HostFailure("invalid_params", "Use a same-host move for this destination") }
            guard !local || destinationID != c.workspaceID else { throw HostFailure("invalid_params", "Choose a different workspace") }
            if let existing = catalog.handoffs?.first(where: { $0.id == id }) {
                guard existing.source.id == c.id, existing.destinationWorkspaceID == destinationID, existing.destinationHostID == destinationHost else {
                    throw HostFailure("id_conflict", "Handoff identity already names another move")
                }
                return try handoffValue(existing)
            }
            try requireHandoffAvailable(c.id); try requireWorkspaceNotMoving(c.workspaceID); try requireIdleForHandoff(c)
            let destination = local ? try requireHandoffDestination(destinationID) : nil
            let retained = local ? try retainedWorkspace(c.id, workspaceID: destinationID) : nil
            if let destination {
                if let retained { try handoffFiles.requireSnapshot(retained, at: destination) }
                else { try handoffFiles.requireCleanDestination(destination) }
            }
            let peerKey = local ? handoffSigner.publicKey : try required(p, "destinationPublicKey")
            guard let key = Data(base64Encoded: peerKey), key.count == 32 else { throw HostFailure("invalid_params", "A pinned destination public key is required") }
            var record = HostHandoffRecord(id: id, role: local ? "local" : "source", phase: "preparing",
                sourceHostID: catalog.hostID, destinationHostID: destinationHost, peerPublicKey: peerKey, source: c,
                destinationWorkspaceID: destinationID)
            catalog.handoffs = (catalog.handoffs ?? []) + [record]
            catalog.heldConversations.insert(c.id)
            try save() // Fence every writer/queue path before native detach and capture.
            let index = try handoffIndex(id)
            do {
                try provider.detachForHandoff(c)
                let package = HostHandoffPackage(operationID: id, sourceHostID: catalog.hostID, destinationHostID: destinationHost,
                    destinationWorkspaceID: destinationID, native: try provider.exportForHandoff(c),
                    workspace: try handoffFiles.export(root: URL(fileURLWithPath: authorizedWorkspace(c.workspaceID))))
                let digest = try HostHandoffSigner.digest(package)
                try saveHandoffPackage(package)
                record.digest = digest
                record.phase = "exported"; catalog.handoffs![index] = record; try save()
                if let destination {
                    record.recoveryID = id; catalog.handoffs![index] = record; try save()
                    _ = try handoffFiles.install(package.workspace, destination: destination, operationID: id, retainedSnapshot: retained)
                    record.phase = "installed"; catalog.handoffs![index] = record; try save()
                    let relocated = try provider.relocate(c, workspaceID: destinationID, cwd: destination.path)
                    record.destination = relocated; record.phase = "active"
                    catalog.conversations.removeAll { $0.id == c.id }; catalog.conversations.append(relocated)
                    catalog.handoffs![index] = record; supersedeEarlierHandoffs(before: index)
                    catalog.heldConversations.remove(c.id); try save()
                    try provider.releaseHandoffFence(relocated)
                    try provider.resume(relocated)
                }
                return try handoffValue(record)
            } catch {
                if record.phase != "active" { record.phase = "needs_inspection" }
                record.error = error.localizedDescription; catalog.handoffs![index] = record; try save()
                throw error
            }
        case "conversation.handoff.install":
            let package = try decodeHandoff(p["package"], as: HostHandoffPackage.self)
            let sourceKey = try required(p, "sourcePublicKey")
            var record = HostHandoffRecord(id: id, role: "destination", phase: "installing", sourceHostID: package.sourceHostID,
                destinationHostID: package.destinationHostID, peerPublicKey: sourceKey, source: package.native.conversation,
                destinationWorkspaceID: package.destinationWorkspaceID, digest: try HostHandoffSigner.digest(package))
            guard package.operationID == id, package.destinationHostID == catalog.hostID, package.sourceHostID != catalog.hostID else {
                throw HostFailure("handoff_identity", "Transfer names another operation or destination host")
            }
            try HostHandoffSigner.verify(decodeHandoff(p["certificate"], as: HostHandoffCertificate.self), publicKey: sourceKey, record: record, stage: "exported")
            if let existing = catalog.handoffs?.first(where: { $0.id == id }) {
                guard existing.digest == record.digest, existing.peerPublicKey == sourceKey, ["installed", "active"].contains(existing.phase) else {
                    throw HostFailure("handoff_state", "Destination already has an incomplete or aborted operation; inspect its receipt")
                }
                return try handoffValue(existing)
            }
            let existingConversation = catalog.conversations.first(where: {
                $0.id == record.source.id || ($0.provider == record.source.provider && $0.nativeSessionID == record.source.nativeSessionID)
            })
            let latest = (catalog.handoffs ?? []).last { $0.source.id == record.source.id && !["aborted", "restored"].contains($0.phase) }
            if let existingConversation {
                guard existingConversation.id == record.source.id, existingConversation.nativeSessionID == record.source.nativeSessionID,
                      latest?.role == "source", latest?.phase == "committed" else {
                    throw HostFailure("handoff_identity", "Destination already owns this native conversation")
                }
                record.replacedConversation = existingConversation
            }
            let retained = try retainedWorkspace(record.source.id, workspaceID: record.destinationWorkspaceID)
            let destination = try requireHandoffDestination(record.destinationWorkspaceID, excluding: existingConversation?.id)
            try requireWorkspaceNotMoving(record.destinationWorkspaceID)
            if let reason = provider.handoffUnavailableReason(record.source) { throw HostFailure("handoff_unsupported", reason) }
            try saveHandoffPackage(package)
            record.recoveryID = id
            catalog.handoffs = (catalog.handoffs ?? []) + [record]; try save()
            let index = try handoffIndex(id)
            do {
                _ = try handoffFiles.install(package.workspace, destination: destination, operationID: id, retainedSnapshot: retained)
                record.destination = try provider.adoptHandoff(package.native, id: record.source.id,
                    workspaceID: record.destinationWorkspaceID, cwd: destination.path)
                catalog.conversations.removeAll { $0.id == record.source.id }
                catalog.conversations.append(record.destination!)
                catalog.heldConversations.insert(record.source.id)
                record.phase = "installed"; catalog.handoffs![index] = record; try save()
                return try handoffValue(record)
            } catch {
                record.phase = "needs_inspection"; record.error = error.localizedDescription
                catalog.handoffs![index] = record; try save(); throw error
            }
        case "conversation.handoff.commit":
            let index = try handoffIndex(id)
            var record = catalog.handoffs![index]
            _ = try authorizedWorkspace(record.source.workspaceID)
            guard record.role == "source", ["exported", "retiring", "committed"].contains(record.phase) else {
                throw HostFailure("handoff_state", "Source is not ready to commit this ownership transfer")
            }
            try HostHandoffSigner.verify(decodeHandoff(p["certificate"], as: HostHandoffCertificate.self), publicKey: record.peerPublicKey, record: record, stage: "installed")
            try requireLatestHandoff(index)
            if record.phase != "committed" {
                let package = try loadHandoffPackage(record)
                record.phase = "retiring"; record.error = nil; catalog.handoffs![index] = record; try save()
                do { try provider.retireHandoff(package.native, operationID: id) }
                catch { record.error = error.localizedDescription; catalog.handoffs![index] = record; try save(); throw error }
                record.phase = "committed"; catalog.handoffs![index] = record; try save()
            }
            return try handoffValue(record)
        case "conversation.handoff.activate":
            let index = try handoffIndex(id)
            var record = catalog.handoffs![index]
            guard record.role == "destination", ["installed", "activating", "active"].contains(record.phase), var c = record.destination else {
                throw HostFailure("handoff_state", "Destination has no complete native transfer to activate")
            }
            _ = try authorizedWorkspace(c.workspaceID)
            try requireLatestHandoff(index)
            try HostHandoffSigner.verify(decodeHandoff(p["certificate"], as: HostHandoffCertificate.self), publicKey: record.peerPublicKey, record: record, stage: "committed")
            if record.phase != "active" {
                record.phase = "activating"; record.error = nil; catalog.handoffs![index] = record; try save()
                do { c = try provider.activateHandoff(loadHandoffPackage(record).native, conversation: c) }
                catch { record.error = error.localizedDescription; catalog.handoffs![index] = record; try save(); throw error }
                record.destination = c
                catalog.conversations.removeAll { $0.id == c.id }; catalog.conversations.append(c)
            }
            record.phase = "active"; record.error = nil; catalog.handoffs![index] = record
            supersedeEarlierHandoffs(before: index)
            catalog.heldConversations.remove(c.id); try save() // Ownership before native process startup.
            try provider.releaseHandoffFence(c)
            if !provider.isConnected(c.id) { try provider.resume(c) }
            return try handoffValue(record)
        case "conversation.handoff.abort":
            return try abortHandoff(id: id, params: p)
        case "conversation.handoff.recover":
            let index = try handoffIndex(id)
            var record = catalog.handoffs![index]
            guard record.role == "local", record.phase != "active" else { throw HostFailure("handoff_state", "Only an incomplete same-host move can be restored here") }
            try requireLatestHandoff(index)
            if record.phase == "restored" { return try handoffValue(record) }
            let destination = URL(fileURLWithPath: try authorizedWorkspace(record.destinationWorkspaceID))
            _ = try authorizedWorkspace(record.source.workspaceID)
            var current = record.destination ?? record.source
            if record.recoveryID != nil { current.workspaceID = record.destinationWorkspaceID; current.cwd = destination.path }
            try provider.detachForHandoff(current)
            if record.recoveryID != nil { try handoffFiles.recover(operationID: id, destination: destination) }
            _ = try provider.relocate(current, workspaceID: record.source.workspaceID, cwd: record.source.cwd)
            catalog.conversations.removeAll { $0.id == record.source.id }; catalog.conversations.append(record.source)
            record.phase = "restored"; record.error = nil; catalog.handoffs![index] = record
            catalog.heldConversations.remove(record.source.id); try save()
            try provider.releaseHandoffFence(record.source)
            return try handoffValue(record)
        default: throw HostFailure("method_not_found", request.method)
        }
    }
    func abortHandoff(id: String, params: [String: HostValue]) throws -> HostValue {
        if catalog.handoffs?.contains(where: { $0.id == id }) != true {
            // An authenticated destination can issue a durable tombstone even
            // when transfer never arrived. It then refuses any later install.
            let package = try decodeHandoff(params["package"], as: HostHandoffPackage.self)
            let sourceKey = try required(params, "sourcePublicKey")
            let record = HostHandoffRecord(id: id, role: "destination", phase: "aborted", sourceHostID: package.sourceHostID,
                destinationHostID: package.destinationHostID, peerPublicKey: sourceKey, source: package.native.conversation,
                destinationWorkspaceID: package.destinationWorkspaceID, digest: try HostHandoffSigner.digest(package))
            guard package.operationID == id, package.destinationHostID == catalog.hostID else { throw HostFailure("handoff_identity", "Wrong destination for cancellation") }
            try HostHandoffSigner.verify(decodeHandoff(params["certificate"], as: HostHandoffCertificate.self), publicKey: sourceKey, record: record, stage: "aborting")
            catalog.handoffs = (catalog.handoffs ?? []) + [record]; try save()
            return try handoffValue(record)
        }
        let index = try handoffIndex(id)
        var record = catalog.handoffs![index]
        guard record.role != "local", !["committed", "activating", "active"].contains(record.phase) else {
            throw HostFailure("handoff_state", "Ownership is already committed. Complete activation, then use a reverse handoff to move back.")
        }
        if record.phase == "aborted" { return try handoffValue(record) }
        if record.role == "source" {
            _ = try authorizedWorkspace(record.source.workspaceID)
            if params["certificate"] == nil {
                // The source is the decision coordinator. Recording aborting
                // makes any previously issued installed receipt uncommittable.
                record.phase = record.digest == nil ? "aborted" : "aborting"
                catalog.handoffs![index] = record; try save()
                if record.phase == "aborted" {
                    catalog.heldConversations.remove(record.source.id); try save()
                    try provider.releaseHandoffFence(record.source)
                }
                return try handoffValue(record)
            }
            guard record.phase == "aborting" else { throw HostFailure("handoff_state", "Record the source abort decision before accepting the destination tombstone") }
            try HostHandoffSigner.verify(decodeHandoff(params["certificate"], as: HostHandoffCertificate.self), publicKey: record.peerPublicKey, record: record, stage: "aborted")
            try provider.restoreRetiredHandoff(loadHandoffPackage(record).native, operationID: id)
            record.phase = "aborted"; catalog.handoffs![index] = record
            catalog.heldConversations.remove(record.source.id); try save()
            try provider.releaseHandoffFence(record.source)
        } else {
            _ = try authorizedWorkspace(record.destinationWorkspaceID)
            try HostHandoffSigner.verify(decodeHandoff(params["certificate"], as: HostHandoffCertificate.self), publicKey: record.peerPublicKey, record: record, stage: "aborting")
            // Installed history remains private until verified commitment;
            // an abort never leaves a second native-discoverable rollout.
            if let destination = record.destination { try provider.detachForHandoff(destination) }
            if record.recoveryID != nil { try handoffFiles.recover(operationID: id, destination: URL(fileURLWithPath: record.destinationWorkspaceID)) }
            catalog.conversations.removeAll { $0.id == record.source.id }
            if let previous = record.replacedConversation { catalog.conversations.append(previous) }
            record.phase = "aborted"; record.error = nil; catalog.handoffs![index] = record; try save()
            // Keep any imported native writer fence: source will regain ownership.
        }
        return try handoffValue(record)
    }
}
