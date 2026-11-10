import SwiftUI
import MemexExecutionHostCore

@MainActor struct ExecutionSchedulesView: View {
    let schedules: [HostValue]
    let runs: [HostValue]
    let conversations: [HostValue]
    let workspaces: [HostValue]
    let providers: [String]
    let capabilities: Set<String>
    let busy: Bool
    let perform: (String, [String: HostValue]) async -> Bool
    let openConversation: (String) -> Void

    @State private var draft = ExecutionScheduleDraft()
    private var supportsRuns: Bool { capabilities.contains("schedules.runs") }

    var body: some View {
        Section("Schedules") {
            Text("Schedules use the host’s durable queue. Recovered or stopped queues remain held until resumed.").font(.caption).foregroundStyle(.secondary)
            ForEach(schedules, id: \.scheduleIdentity) { schedule in
                VStack(alignment: .leading) {
                    Text(schedule["prompt"].string ?? "Scheduled prompt").lineLimit(2)
                    Text(ExecutionScheduleDraft.description(schedule)).font(.caption).foregroundStyle(.secondary)
                    if let error = schedule["lastError"].string { Text(error).font(.caption).foregroundStyle(.red) }
                    HStack {
                        Button("Edit") { draft = ExecutionScheduleDraft(schedule: schedule) }
                        Button(schedule["paused"].bool == true ? "Resume" : "Pause") {
                            action("pause", schedule, ["paused": .bool(schedule["paused"].bool != true)])
                        }
                        Button("Run now") { action("run", schedule) }
                        Button("Delete", role: .destructive) { action("delete", schedule) }
                    }.buttonStyle(.borderless).disabled(busy)
                }
            }
            if capabilities.contains("schedules.new_conversation") {
                Picker("Run in", selection: $draft.freshConversation) {
                    Text("Existing conversation").tag(false)
                    Text("New conversation each run").tag(true)
                }
            }
            if draft.freshConversation {
                Picker("Schedule workspace", selection: $draft.workspaceID) {
                    Text("Choose a registered folder").tag("")
                    ForEach(workspaces, id: \.scheduleIdentity) { Text($0["path"].string ?? "").tag($0["id"].string ?? "") }
                }
                Picker("Schedule provider", selection: $draft.provider) {
                    Text("Choose provider").tag("")
                    ForEach(providers, id: \.self) { Text($0).tag($0) }
                }
                TextField("Run conversation title", text: $draft.title)
            } else {
                Picker("Conversation", selection: $draft.conversationID) {
                    Text("Choose a conversation").tag("")
                    ForEach(conversations, id: \.scheduleIdentity) { Text($0["title"].string ?? "Conversation").tag($0["id"].string ?? "") }
                }
            }
            TextField("Scheduled prompt", text: $draft.text, axis: .vertical).lineLimit(3...6)
            Picker("Trigger", selection: $draft.kind) {
                Text("Fixed interval").tag("interval")
                if capabilities.contains("schedules.wall_clock") { Text("Local time and weekdays").tag("wallClock") }
                if capabilities.contains("schedules.events") { Text("Named event").tag("event") }
            }
            if draft.kind == "event" {
                TextField("Event name", text: $draft.eventName)
                Text("Runs when an authenticated integration sends this exact event name to schedule.event. No recurring timer is added.").font(.caption).foregroundStyle(.secondary)
            } else if draft.kind == "wallClock" {
                TextField("Local time (HH:mm)", text: $draft.localTime)
                TextField("Time zone", text: $draft.timeZone)
                HStack {
                    ForEach(Array(ExecutionScheduleDraft.weekdays.enumerated()), id: \.offset) { index, day in
                        Toggle(day, isOn: Binding(get: { draft.days.contains(index + 1) }, set: { selected in
                            if selected { draft.days.insert(index + 1) } else { draft.days.remove(index + 1) }
                        }))
                    }
                }
                Text("Daylight saving gaps and occurrences missed by more than a minute are skipped. Repeated local times run once.").font(.caption).foregroundStyle(.secondary)
            } else { TextField("Interval in seconds", value: $draft.interval, format: .number) }
            if supportsRuns {
                Picker("Run notifications", selection: $draft.notificationPolicy) {
                    Text("Completions and attention").tag("all")
                    Text("Only when attention is needed").tag("attention")
                    Text("Never").tag("never")
                }
                Text("Controls which unread runs need attention in the run inbox. System alerts also depend on this device’s notification settings.").font(.caption).foregroundStyle(.secondary)
            }
            HStack {
                Button("Save schedule") {
                    Task {
                        if await perform("schedule.upsert", draft.parameters(capabilities: capabilities)) { draft = ExecutionScheduleDraft() }
                    }
                }.disabled(busy || !draft.isValid(capabilities: capabilities))
                Button("New schedule") { draft = ExecutionScheduleDraft() }.disabled(busy)
            }
        }
        if supportsRuns {
            Section("Schedule run inbox") {
                if runs.isEmpty { Text("No runs yet.").foregroundStyle(.secondary) }
                ForEach(runs, id: \.scheduleIdentity) { run in
                    VStack(alignment: .leading) {
                        Text(run["status"].string ?? "Unknown")
                            .fontWeight(run["read"].bool == true ? .regular : .semibold)
                        Text(run["createdAt"].string ?? "").font(.caption).foregroundStyle(.secondary)
                        if run["needsAttention"].bool == true { Text("Needs attention").font(.caption).foregroundStyle(.orange) }
                        if let error = run["error"].string { Text(error).font(.caption).foregroundStyle(.red).textSelection(.enabled) }
                        HStack {
                            if let id = run["conversationID"].string {
                                Button("Open run conversation") { openConversation(id) }
                            }
                            Button(run["read"].bool == true ? "Mark unread" : "Mark read") {
                                Task { _ = await perform("schedule.run.read", ["runId": run["id"], "read": .bool(run["read"].bool != true)]) }
                            }
                        }.buttonStyle(.borderless).disabled(busy)
                    }
                }
            }
        }
    }

    private func action(_ action: String, _ schedule: HostValue, _ values: [String: HostValue] = [:]) {
        var parameters = values; parameters["scheduleId"] = schedule["id"]
        Task { _ = await perform("schedule." + action, parameters) }
    }
}

/// The UI and tests share the real wire payload builder; mutually exclusive
/// targets and triggers never leak stale fields when editing a schedule.
struct ExecutionScheduleDraft {
    var id = UUID().uuidString
    var conversationID = ""
    var freshConversation = false
    var workspaceID = ""
    var provider = ""
    var title = "Scheduled conversation"
    var text = ""
    var kind = "interval"
    var interval = 3600
    var localTime = "09:00"
    var timeZone = TimeZone.current.identifier
    var days: Set<Int> = [1, 2, 3, 4, 5]
    var eventName = ""
    var notificationPolicy = "attention"
    static let weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

    init() {}
    init(schedule: HostValue) {
        id = schedule["id"].string ?? id
        conversationID = schedule["conversationID"].string ?? ""
        text = schedule["prompt"].string ?? ""
        freshConversation = schedule["newConversation"] != .null
        workspaceID = schedule["newConversation"]["workspaceID"].string ?? ""
        provider = schedule["newConversation"]["provider"].string ?? ""
        title = schedule["newConversation"]["title"].string ?? title
        kind = schedule["eventName"].string != nil ? "event" : schedule["wallClock"] != .null ? "wallClock" : "interval"
        interval = Int(schedule["intervalSeconds"].number ?? 3600)
        localTime = schedule["wallClock"]["localTime"].string ?? localTime
        timeZone = schedule["wallClock"]["timeZone"].string ?? timeZone
        if schedule["wallClock"] != .null { days = Set(schedule["wallClock"]["weekdays"].array.compactMap { $0.number.map(Int.init) }) }
        eventName = schedule["eventName"].string ?? ""
        notificationPolicy = schedule["notificationPolicy"].string ?? "attention"
    }

    func isValid(capabilities: Set<String>) -> Bool {
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return false }
        if freshConversation {
            guard capabilities.contains("schedules.new_conversation"), !workspaceID.isEmpty, !provider.isEmpty else { return false }
        } else if conversationID.isEmpty { return false }
        switch kind {
        case "event": return capabilities.contains("schedules.events") && !eventName.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
        case "wallClock": return capabilities.contains("schedules.wall_clock") && !localTime.isEmpty && !timeZone.isEmpty && !days.isEmpty
        default: return interval >= 60
        }
    }

    func parameters(capabilities: Set<String>) -> [String: HostValue] {
        var values: [String: HostValue] = ["scheduleId": .string(id), "text": .string(text)]
        if freshConversation {
            values["newConversation"] = .object(["workspaceId": .string(workspaceID), "provider": .string(provider), "title": .string(title)])
        } else { values["conversationId"] = .string(conversationID) }
        if kind == "event" { values["eventName"] = .string(eventName) }
        else if kind == "wallClock" {
            values["wallClock"] = .object(["localTime": .string(localTime), "weekdays": .array(days.sorted().map { .number(Double($0)) }), "timeZone": .string(timeZone)])
        } else { values["intervalSeconds"] = .number(Double(interval)) }
        if capabilities.contains("schedules.runs") { values["notificationPolicy"] = .string(notificationPolicy) }
        return values
    }

    static func description(_ schedule: HostValue) -> String {
        let trigger: String
        if let event = schedule["eventName"].string { trigger = "Event: " + event }
        else if schedule["wallClock"] != .null {
            trigger = "\(schedule["wallClock"]["localTime"].string ?? "") · \(schedule["wallClock"]["timeZone"].string ?? "")"
        } else { trigger = "Every \(Int(schedule["intervalSeconds"].number ?? 0)) seconds" }
        let target = schedule["newConversation"] == .null ? "Existing conversation" : "New conversation each run"
        return "\(trigger) · \(target) · \(schedule["paused"].bool == true ? "Paused" : "Active")"
    }
}

private extension HostValue { var scheduleIdentity: String { self["id"].string ?? "" } }
