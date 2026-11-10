import Foundation

#if canImport(SQACPHost)
import SQACPHost

extension ConversationChildHistory {
    init(_ child: AgentChildConversation) {
        self.init(id: child.id,
                  records: ConversationChildProjection.records(child),
                  loading: child.snapshot.isLoadingHistory,
                  error: child.error,
                  status: child.session.status,
                  model: child.snapshot.session.modelID ?? child.session.modelID,
                  reasoningEffort: child.snapshot.session.reasoningID ?? child.session.reasoningID)
    }
}

/// Converts typed provider projections; raw evidence is the encoded projection,
/// not a fabricated native transcript or a claim of an indexed source record.
enum ConversationChildProjection {
    static func records(_ child: AgentChildConversation) -> [TranscriptRecord] {
        let snapshot = child.snapshot
        var seen = Set<AgentTimelineItemProjection>()
        var records: [TranscriptRecord] = []
        // History loaders can omit timeline entries while retaining canonical
        // message/activity order. Append only those absent from the timeline.
        let order = snapshot.timelineItems + snapshot.messageOrder.map(AgentTimelineItemProjection.message)
            + snapshot.activityOrder.map(AgentTimelineItemProjection.activity)
        for item in order where seen.insert(item).inserted {
            switch item {
            case .message(let id):
                guard let native = snapshot.messagesByID[id] else { continue }
                let message = Message(role: native.role.rawValue, text: native.content,
                    toolName: nil, toolInput: nil, toolOutput: nil,
                    eventID: native.id.rawValue, sourceTurnID: native.turnID?.rawValue,
                    assistantPhase: native.assistantPhase, sourceRecordType: "child_runtime_projection",
                    sourceContent: attachments(native.attachments ?? []))
                records.append(TranscriptRecord(recordID: "child:\(child.id):message:\(id.rawValue)", record: message,
                    sourceRecordID: native.providerMessageID ?? id.rawValue, rawJSON: json(native)))
            case .activity(let id):
                guard let native = snapshot.activitiesByID[id] else { continue }
                var call = Message(role: "tool_use", text: native.summary ?? "",
                    toolName: native.title ?? native.kind, toolInput: native.rawInputJSON, toolOutput: nil,
                    eventID: id.rawValue, parentToolUseID: native.parentToolUseId,
                    sourceTurnID: native.turnID?.rawValue, sourceRecordType: "child_runtime_projection")
                call.structuredActivity = native.payload.flatMap(json)
                call.activityStatus = native.status
                call.mcpAppJSON = native.mcpApp.flatMap(json)
                records.append(TranscriptRecord(recordID: "child:\(child.id):tool:\(id.rawValue)", record: call,
                    sourceRecordID: id.rawValue, rawJSON: json(native)))
                let output = native.rawOutputJSON ?? (native.details.isEmpty ? nil : native.details.joined(separator: "\n\n"))
                if let output {
                    let result = Message(role: "tool_result", text: "", toolName: nil, toolInput: nil, toolOutput: output,
                        parentToolUseID: id.rawValue, sourceTurnID: native.turnID?.rawValue,
                        sourceRecordType: "child_runtime_projection", toolResultIsError: native.status == "failed")
                    records.append(TranscriptRecord(recordID: "child:\(child.id):result:\(id.rawValue)", record: result,
                        sourceRecordID: id.rawValue, rawJSON: json(native)))
                }
            }
        }
        return records
    }

    private static func json<T: Encodable>(_ value: T) -> String? {
        let encoder = JSONEncoder()
        encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
        return (try? encoder.encode(value)).map { String(decoding: $0, as: UTF8.self) }
    }

    private static func attachments(_ values: [AgentAttachmentProjection]) -> String? {
        guard !values.isEmpty else { return nil }
        let content: [[String: String]] = values.map { value in
            var item = ["type": value.kind == .image ? "image" : "attachment", "title": value.name,
                        "file_id": value.id]
            if let uri = value.uri { item["path"] = uri; item["url"] = uri }
            if let mime = value.mimeType { item["mimeType"] = mime }
            if value.kind == .image, let preview = value.previewData {
                // Preview bytes have their own documented JPEG encoding.
                item["data"] = preview
                item["mimeType"] = "image/jpeg"
            }
            return item
        }
        return json(content)
    }
}
#endif
