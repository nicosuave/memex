import Foundation

struct Session: Decodable, Identifiable, Hashable, Sendable {
    let source: String
    let sessionID: String
    let sourcePath: String
    let project: String
    var label: String?
    var lastAt: String?
    var resumeCommand: String?
    var cwd: String?
    var snippet: String?
    var repoProject: String?
    var machine: String?
    var searchRecordID: String?
    var messageCount: Int?

    // A session ID alone is not unique across machines, providers or transcript files.
    var machineID: String { machine ?? "local" }
    var id: String { [machineID, source, sessionID, sourcePath].joined(separator: "\u{1f}") }
    var title: String { label?.nilIfBlank ?? "Untitled conversation" }
    var projectName: String { repoProject?.nilIfBlank ?? project.nilIfBlank ?? "No project" }
    var date: Date? { lastAt.flatMap { try? Date.ISO8601FormatStyle().parse($0) } }

    func applyingMetadata(_ metadata: Session) -> Session {
        guard metadata.id == id else { return self }
        var value = metadata
        value.snippet = snippet
        value.searchRecordID = searchRecordID
        if value.label == nil { value.label = label }
        if value.lastAt == nil { value.lastAt = lastAt }
        if value.messageCount == nil { value.messageCount = messageCount }
        return value
    }

    enum CodingKeys: String, CodingKey {
        case source, project, label, cwd, snippet, machine
        case sessionID = "session_id", sourcePath = "source_path"
        case lastAt = "last_at", resumeCommand = "resume_cmd"
        case repoProject = "repo_project"
        case searchRecordID = "search_record_id"
        case messageCount = "message_count"
    }
}

struct SearchHit: Decodable, Sendable {
    let source: String
    let sessionID: String
    let sourcePath: String
    let project: String
    let snippet: String?
    let ts: String?
    var machine: String?
    var recordID: String?

    enum CodingKeys: String, CodingKey {
        case source, project, snippet, ts, machine
        case sessionID = "session_id", sourcePath = "source_path"
        case recordID = "record_id"
    }

    func session(known: [String: Session]) -> Session {
        var value = Session(source: source, sessionID: sessionID, sourcePath: sourcePath,
                            project: project, label: nil, lastAt: nil, machine: machine)
        if let existing = known[value.id] { value = existing }
        value.snippet = snippet
        value.searchRecordID = recordID
        if value.label == nil { value.label = snippet?.nilIfBlank }
        if value.lastAt == nil, let ts {
            value.lastAt = ts
        }
        return value
    }
}

struct TranscriptRecord: Decodable, Identifiable, Equatable, Sendable {
    let recordID: String
    let record: Message
    var sourceRecordID: String? = nil
    var rawJSON: String? = nil
    var sourceID: String { sourceRecordID ?? recordID }
    var id: String { recordID }
    enum CodingKeys: String, CodingKey { case recordID = "record_id", record }

    init(recordID: String, record: Message, sourceRecordID: String? = nil, rawJSON: String? = nil) {
        self.recordID = recordID
        self.record = record
        self.sourceRecordID = sourceRecordID
        self.rawJSON = rawJSON
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        recordID = try container.decode(String.self, forKey: .recordID)
        record = try container.decode(Message.self, forKey: .record)
        rawJSON = try RawTranscriptJSON(from: decoder).prettyPrinted()
    }

    var rawTranscriptBody: String {
        if let rawJSON { return rawJSON }
        var fields: [String: RawTranscriptJSON] = ["role": .string(record.role), "text": .string(record.text)]
        for (key, value) in [("tool_name", record.toolName), ("tool_input", record.toolInput),
                             ("tool_output", record.toolOutput), ("event_id", record.eventID),
                             ("parent_tool_use_id", record.parentToolUseID), ("source_turn_id", record.sourceTurnID),
                             ("assistant_phase", record.assistantPhase), ("lifecycle_event", record.lifecycleEvent),
                             ("source_record_type", record.sourceRecordType)] {
            if let value { fields[key] = .string(value) }
        }
        if let content = record.sourceContent { fields["source_content"] = .string(content) }
        return (try? RawTranscriptJSON.object(["record_id": .string(sourceID), "record": .object(fields)]).prettyPrinted()) ?? record.text
    }
}

struct Message: Decodable, Equatable, Sendable {
    let role: String
    var text: String
    let toolName: String?
    let toolInput: String?
    let toolOutput: String?
    var eventID: String? = nil
    var parentToolUseID: String? = nil
    var sourceTurnID: String? = nil
    var assistantPhase: String? = nil
    var lifecycleEvent: String? = nil
    var sourceRecordType: String? = nil
    var sourceContent: String? = nil
    // Display-only classification; source records and serialized content stay intact.
    var contextLabel: String? = nil

    enum CodingKeys: String, CodingKey {
        case role, text
        case toolName = "tool_name", toolInput = "tool_input", toolOutput = "tool_output"
        case eventID = "event_id", parentToolUseID = "parent_tool_use_id"
        case sourceTurnID = "source_turn_id", assistantPhase = "assistant_phase"
        case lifecycleEvent = "lifecycle_event", sourceRecordType = "source_record_type"
        case sourceContent = "source_content"
    }

    var isActivity: Bool { ["tool_use", "tool_result", "tool", "reasoning"].contains(role) }
    var isEnvironmentContext: Bool {
        guard role == "user" else { return false }
        let value = text.trimmingCharacters(in: .whitespacesAndNewlines)
        return value.hasPrefix("<environment_context>") && value.hasSuffix("</environment_context>")
    }
    var isInstruction: Bool { contextLabel != nil || ["system", "developer"].contains(role) || isEnvironmentContext }
    var activityTitle: String {
        if role == "reasoning" { return "Reasoning" }
        guard let toolName = toolName?.nilIfBlank else {
            return role == "tool_result" ? "Tool result" : "Tool activity"
        }
        return toolName.split { $0 == "_" || $0 == "-" || $0.isWhitespace }
            .map { $0.prefix(1).uppercased() + $0.dropFirst() }.joined(separator: " ")
    }
}

struct TranscriptActivity: Identifiable, Sendable {
    let records: [TranscriptRecord]
    var id: String { records.first(where: { $0.record.role == "tool_use" })?.id ?? records[0].id }
    var title: String {
        let message = records.first(where: { $0.record.role == "tool_use" })?.record ?? records[0].record
        if let label = message.contextLabel { return label }
        if message.isEnvironmentContext { return "Environment context" }
        if message.isInstruction { return message.role.capitalized + " instructions" }
        return message.activityTitle
    }
    var body: String {
        records.map { entry in
            let message = entry.record
            // Providers commonly repeat input/output in text. Preserve distinct content,
            // including identical call input and result output as separate sections.
            var parts: [String] = []
            for value in [message.toolInput, message.toolOutput, message.text] {
                if let value, value.nilIfBlank != nil, !parts.contains(value) { parts.append(value) }
            }
            let content = parts.joined(separator: "\n\n")
            guard records.count > 1 else { return content }
            return (message.role == "tool_use" ? "Input" : "Output") + "\n" + content
        }.joined(separator: "\n\n")
    }

    static func pair(_ records: [TranscriptRecord]) -> [TranscriptActivity] {
        var result: [TranscriptActivity] = []
        var tools: [TranscriptRecord] = []
        func flush() {
            guard !tools.isEmpty else { return }
            var partners: [Int: Int] = [:]
            var usedResults: Set<Int> = []
            let calls = tools.indices.filter { tools[$0].record.role == "tool_use" }
            let outputs = tools.indices.filter { tools[$0].record.role == "tool_result" }
            let linkedCalls = Dictionary(grouping: calls.filter { tools[$0].record.eventID?.nilIfBlank != nil }) {
                tools[$0].record.eventID!
            }
            // RecordLinks is flattened into the CLI record. event_id identifies the
            // invocation; parent_tool_use_id points to it. Session ancestry links do not.
            for output in outputs {
                guard let link = tools[output].record.parentToolUseID?.nilIfBlank,
                      let matches = linkedCalls[link], matches.count == 1,
                      let call = matches.first, partners[call] == nil else { continue }
                partners[call] = output
                usedResults.insert(output)
            }
            for output in outputs where !usedResults.contains(output) && output > 0 {
                let call = output - 1
                guard tools[call].record.role == "tool_use", partners[call] == nil else { continue }
                let input = tools[call].record
                let answer = tools[output].record
                if let a = input.eventID?.nilIfBlank, let b = answer.parentToolUseID?.nilIfBlank, a != b { continue }
                if let a = input.toolName?.nilIfBlank, let b = answer.toolName?.nilIfBlank, a != b { continue }
                // Without links, concurrent calls cannot safely be assigned by name.
                guard !calls.contains(where: { $0 < call && (partners[$0] == nil || partners[$0]! > output) }) else { continue }
                partners[call] = output
                usedResults.insert(output)
            }
            for index in tools.indices where !usedResults.contains(index) {
                let members = partners[index].map { [tools[index], tools[$0]] } ?? [tools[index]]
                result.append(TranscriptActivity(records: members))
            }
            tools = []
        }
        for entry in records {
            if ["tool_use", "tool_result"].contains(entry.record.role) { tools.append(entry) }
            else {
                flush()
                result.append(TranscriptActivity(records: [entry]))
            }
        }
        flush()
        return result
    }
}

struct TranscriptItem: Identifiable, Sendable {
    let records: [TranscriptRecord]
    // Preserve pairing when routine spans are separated around explicit failures.
    var groupedActivities: [TranscriptActivity]? = nil
    var isCompletedWork = false
    var activities: [TranscriptActivity] { groupedActivities ?? TranscriptActivity.pair(records) }
    var id: String { records[0].id }
    var isActivity: Bool { isCompletedWork || records[0].record.isActivity }
    var isInstructions: Bool { records[0].record.isInstruction }
    var activitySummary: String {
        if isCompletedWork { return "Completed work" }
        if isInstructions { return title }
        let entries = activities
        guard entries.count > 1 else { return entries.first?.presentation.title ?? title }
        if entries.allSatisfy({ $0.presentation.category == nil && $0.records[0].record.role != "reasoning" }) {
            return "\(entries.count) tool calls"
        }
        var categories: [(String, Int)] = []
        for entry in entries {
            let category = entry.presentation.category?.lowercased() ?? (entry.records[0].record.role == "reasoning" ? "reasoning" : "tool")
            if let index = categories.firstIndex(where: { $0.0 == category }) { categories[index].1 += 1 }
            else { categories.append((category, 1)) }
        }
        let details = categories.prefix(3).map { name, count in
            name == "reasoning" ? "reasoning" : "\(count) \(name)\(count == 1 ? "" : name == "search" ? "es" : "s")"
        }.joined(separator: ", ")
        return "\(entries.count) activities · \(details)" + (categories.count > 3 ? ", …" : "")
    }
    var title: String {
        if isInstructions { return "Session context" }
        var names: [String] = []
        // Use the same readable labels for group summaries and individual tools.
        for item in records where item.record.role != "tool_result" {
            let name = item.record.activityTitle
            if !names.contains(name) { names.append(name) }
        }
        if names.isEmpty { return "Tool results" }
        let visible = names.prefix(3).joined(separator: ", ")
        return names.count > 3 ? "\(visible) +\(names.count - 3) more" : visible
    }

    static func group(_ records: [TranscriptRecord]) -> [TranscriptItem] {
        TranscriptPresentation.group(records)
    }

    static func groupConsecutive(_ records: [TranscriptRecord]) -> [TranscriptItem] {
        var result: [TranscriptItem] = []
        var pending: [TranscriptRecord] = []
        func flush() {
            guard !pending.isEmpty else { return }
            let entries = TranscriptActivity.pair(pending)
            guard pending[0].record.isActivity, entries.contains(where: { $0.presentation.needsAttention }) else {
                result.append(TranscriptItem(records: pending))
                pending = []
                return
            }
            var routine: [TranscriptActivity] = []
            func appendRoutine() {
                if !routine.isEmpty {
                    result.append(TranscriptItem(records: routine.flatMap(\.records), groupedActivities: routine))
                    routine = []
                }
            }
            for entry in entries {
                if entry.presentation.needsAttention {
                    appendRoutine()
                    result.append(TranscriptItem(records: entry.records, groupedActivities: [entry]))
                } else { routine.append(entry) }
            }
            appendRoutine()
            pending = []
        }
        for record in records {
            let canGroup = record.record.isActivity || record.record.isInstruction
            if let previous = pending.first,
               !canGroup || previous.record.isActivity != record.record.isActivity {
                flush()
            }
            if canGroup { pending.append(record) }
            else {
                result.append(TranscriptItem(records: [record]))
            }
        }
        flush()
        return result
    }
}

extension String {
    var nilIfBlank: String? { trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : self }
}
