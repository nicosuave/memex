import Foundation

#if canImport(SQACPHost)
import SQACPHost
#endif

/// Projects the runtime's display subset. Its complete persisted catalog is evidence,
/// not an additional timeline: rendering both would duplicate the active turn.
enum ConversationProjection {
    static func conversation(in json: String, sessionID: String) throws -> RawTranscriptJSON? {
        let value = try JSONDecoder().decode(RawTranscriptJSON.self, from: Data(json.utf8))
        if value["conversation"]["session_id"].string == sessionID { return value["conversation"] }
        return value["conversations"].array.first { $0["session_id"].string == sessionID }
    }

    static func snapshot(_ conversation: RawTranscriptJSON, thread: RawTranscriptJSON = .null,
                         operations: RawTranscriptJSON = .array([]), ready: Bool, canCancel: Bool,
                         sentPromptIDs: Set<String> = [], ownedLiveUserTurns: Set<String> = []) -> ConversationSnapshot {
        let pendingIDs = Set(conversation["state"]["pending_interactions"].array.compactMap(\.string))
        let pending = thread["pendingRequests"].array.filter { pendingIDs.contains($0["requestId"].string ?? "") }
        let approvals = pending.filter { $0["kind"].string == "approval" }.compactMap { request -> ConversationApproval? in
            guard let id = request["requestId"].string else { return nil }
            let payload = request["payload"]
            return ConversationApproval(id: id, title: payload["title"].string ?? "Approval required",
                detail: payload["rawInputJSON"].string, options: payload["options"].array.compactMap {
                    guard let id = $0["id"].string else { return nil }
                    return .init(id: id, title: $0["name"].string ?? id, kind: $0["kind"].string ?? "")
                }, kind: payload["kind"].string)
        }
        let questions = pending.filter { $0["kind"].string == "user_input" }.compactMap { request -> ConversationQuestion? in
            guard let id = request["requestId"].string else { return nil }
            let payload = request["payload"]
            return ConversationQuestion(id: id, title: payload["title"].string, prompt: payload["prompt"].string ?? "Your input is needed",
                placeholder: payload["placeholder"].string, choices: payload["choices"].array.compactMap {
                    guard let id = $0["id"].string, let title = $0["title"].string else { return nil }
                    return .init(id: id, title: title, value: $0["value"].string ?? title,
                                 description: $0["description"].string)
                }, defaultValue: payload["defaultValue"].string, multiSelect: payload["multiSelect"].bool ?? false,
                   isSecret: payload["isSecret"].bool ?? false)
        }
        let pendingPrompts = operations.array.filter {
            ["thread.turn.start", "thread.turn.steer"].contains($0["command"]["type"].string ?? "")
                && !["completed", "failed"].contains($0["status"].string ?? "")
        }
        let running = conversation["state"]["running"].bool ?? false
        // A command sent by this connection is ordinary work, even before the
        // provider reports a running turn. Only recovered operations need review.
        let recoveredPrompt = pendingPrompts.contains {
            !sentPromptIDs.contains($0["command"]["commandId"].string ?? "")
        }
        var renderer = Renderer(conversation: conversation, ownedLiveUserTurns: ownedLiveUserTurns)
        return ConversationSnapshot(records: renderer.render(), connected: conversation["connected"].bool ?? false,
            ready: ready, running: running, pendingPrompt: !pendingPrompts.isEmpty,
            canCancel: canCancel, approvals: approvals, questions: questions,
            warning: recoveredPrompt && !running
                ? "A previous prompt has no confirmed outcome. It will not be sent again automatically. Check the native session before continuing."
                : nil)
    }

    private struct Renderer {
        let persisted: [RawTranscriptJSON]
        let live: [RawTranscriptJSON]
        var entities: [String: RawTranscriptJSON] = [:]
        var liveTools: [String: RawTranscriptJSON] = [:]
        var liveResults: [String: RawTranscriptJSON] = [:]
        var liveUserTurns: Set<String> = []
        var sourceTurns: [String: String] = [:]
        var sourceMetadata: [String: RawTranscriptJSON] = [:]
        var liveMetadata: [String: RawTranscriptJSON] = [:]
        var emitted: Set<String> = []
        var records: [TranscriptRecord] = []
        var emittedPlanIDs: Set<String> = []

        init(conversation: RawTranscriptJSON, ownedLiveUserTurns: Set<String>) {
            persisted = conversation["presentation"].array
            live = conversation["ephemeral"].array.sorted { $0["source_order"].integer < $1["source_order"].integer }
            liveUserTurns = Set(live.compactMap { entity -> String? in
                guard entity["body"]["kind"].string == "message",
                      entity["body"]["data"]["role"].string == "user",
                      let turn = entity["native_turn_id"].string,
                      ownedLiveUserTurns.contains(turn) else { return nil }
                return turn
            })
            let catalog = conversation["persisted"].array.isEmpty ? persisted : conversation["persisted"].array
            sourceTurns = Self.codexSourceTurns(catalog)
            sourceMetadata = Self.metadataByEntity(catalog)
            for entity in persisted {
                if let id = entity["entity_id"].string { entities[id] = entity }
            }
            for entity in live {
                if let id = entity["item_id"].string { entities[id] = entity }
                if entity["body"]["kind"].string == "tool_invocation", let call = entity["body"]["data"]["native_call_id"].string {
                    liveTools[call] = entity
                }
                if entity["body"]["kind"].string == "tool_result", let call = entity["body"]["data"]["native_correlation_key"].string {
                    liveResults[call] = entity
                }
                let payload = entity["body"]["data"]["payload"]
                if payload["data"]["namespace"].string == "memex/live",
                   let target = payload["data"]["value"]["item_id"].string {
                    liveMetadata[target] = Self.merge(liveMetadata[target] ?? .null, payload["data"]["value"])
                }
            }
        }

        private static func merge(_ older: RawTranscriptJSON, _ newer: RawTranscriptJSON) -> RawTranscriptJSON {
            guard case .object(let next) = newer else { return older }
            guard case .object(let previous) = older else { return newer }
            return .object(previous.merging(next) { _, new in new })
        }

        /// Metadata is joined by exact retained evidence, never text or proximity.
        /// Some tools aggregate call and result evidence; conflicting envelopes are
        /// intentionally left unclassified rather than assigning a guessed phase.
        private static func metadataByEntity(_ catalog: [RawTranscriptJSON]) -> [String: RawTranscriptJSON] {
            var byEvidence: [String: [RawTranscriptJSON]] = [:]
            for entity in catalog {
                let payload = entity["body"]["data"]["payload"]
                guard payload["type"].string == "metadata",
                      ["codex", "claude"].contains(payload["data"]["namespace"].string ?? "") else { continue }
                for evidence in entity["evidence"].array {
                    if let key = evidence.jsonText { byEvidence[key, default: []].append(payload["data"]["value"]) }
                }
            }
            var result: [String: RawTranscriptJSON] = [:]
            for entity in catalog {
                guard let id = entity["entity_id"].string else { continue }
                let candidates = entity["evidence"].array.flatMap { byEvidence[$0.jsonText ?? ""] ?? [] }
                if let first = candidates.first, candidates.allSatisfy({ $0 == first }) { result[id] = first }
            }
            return result
        }

        /// Canonical content and its Codex metadata share the same retained
        /// source occurrence. Associate turns by that evidence, never by text,
        /// timestamps, adjacency, or the provider's unrelated UI/model-input IDs.
        private static func codexSourceTurns(_ entities: [RawTranscriptJSON]) -> [String: String] {
            var evidenceTurns: [String: Set<String>] = [:]
            for entity in entities {
                let payload = entity["body"]["data"]["payload"]
                guard entity["body"]["kind"].string == "entry",
                      payload["type"].string == "metadata",
                      payload["data"]["namespace"].string == "codex",
                      let turn = payload["data"]["value"]["associated_turn_id"].string,
                      !turn.isEmpty else { continue }
                for evidence in entity["evidence"].array {
                    if let key = evidence.jsonText { evidenceTurns[key, default: []].insert(turn) }
                }
            }
            var turns: [String: String] = [:]
            for entity in entities {
                guard let id = entity["entity_id"].string else { continue }
                let matches = entity["evidence"].array.reduce(into: Set<String>()) { result, evidence in
                    if let key = evidence.jsonText, let candidates = evidenceTurns[key] { result.formUnion(candidates) }
                }
                if matches.count == 1 { turns[id] = matches.first }
            }
            return turns
        }

        mutating func render() -> [TranscriptRecord] {
            let entries = persisted.filter { $0["body"]["kind"].string == "entry" }
                .sorted { $0["body"]["data"]["source_order"].integer < $1["body"]["data"]["source_order"].integer }
            for entry in entries {
                let data = entry["body"]["data"]
                let payload = data["payload"]
                if payload["type"].string == "metadata" || payload["type"].string == "opaque" {
                    emitMetadata(entry, payload: payload)
                } else if data["display"].bool == false {
                    append(entry, suffix: "raw", role: "system", text: "", rawOnly: true)
                } else if payload["type"].string == "entity" {
                    if let id = payload["data"]["entity_id"].string, let entity = entities[id] { emit(entity) }
                } else if payload["type"].string == "branch_summary", let summary = payload["data"]["summary"].string {
                    append(entry, suffix: "summary", role: "system", text: summary, context: "Conversation summary")
                } else if payload["type"].string == "model_change", let model = payload["data"]["model"].string {
                    append(entry, suffix: "model", role: "system", text: model, context: "Model")
                } else {
                    append(entry, suffix: "raw", role: "system", text: "", rawOnly: true)
                }
            }
            for entity in live { emit(entity) }
            // A completed native plan can be restored as metadata without a
            // surviving tool row. Retain the exact plan ID and full text once.
            for key in liveMetadata.keys.sorted() {
                guard let metadata = liveMetadata[key], let planID = metadata["planDocument"]["id"].string,
                      !emittedPlanIDs.contains(planID), metadata["planDocument"]["text"].string != nil else { continue }
                let entity: RawTranscriptJSON = .object(["item_id": .string(key), "body": .object([
                    "kind": .string("tool_invocation"), "data": .object(["native_call_id": .string(key), "name": .string("Plan")])])])
                append(entity, suffix: "plan", role: "tool_use", text: "", nativeID: key, toolName: "Plan")
            }
            return records
        }

        mutating func emitMetadata(_ entity: RawTranscriptJSON, payload: RawTranscriptJSON) {
            let value = payload["data"]["value"]
            if let id = value["child"]["id"].string {
                let child = value["child"]
                append(entity, suffix: "child", role: "subagent",
                       text: (child["title"].string ?? id) + " · " + (child["status"].string ?? "unknown"),
                       activity: RawTranscriptJSON.object(["child": child]).jsonText)
                return
            }
            let native = value["payload"].jsonText == nil ? value : value["payload"]
            let event = native["type"].string ?? value["subtype"].string ?? ""
            let lifecycle = ["task_started", "task_complete", "turn_complete", "turn_aborted", "context_compacted",
                             "compact_boundary", "retry", "retrying", "handoff", "turn_failed"].contains(event)
            if lifecycle {
                let normalized = event == "turn_complete" ? "task_complete" : event
                if let turn = native["turn_id"].string,
                   !emitted.insert("lifecycle:\(turn):\(normalized)").inserted { return }
                append(entity, suffix: "event", role: "lifecycle",
                       text: native["message"].string ?? native["reason"].string ?? value["content"].string ?? "",
                       turnID: native["turn_id"].string, lifecycle: normalized)
            } else {
                append(entity, suffix: "raw", role: "system", text: "", rawOnly: true)
            }
        }

        mutating func emit(_ original: RawTranscriptJSON) {
            let kind = original["body"]["kind"].string ?? ""
            let originalData = original["body"]["data"]
            let entity: RawTranscriptJSON
            let nativeKey: String?
            switch kind {
            case "tool_invocation":
                nativeKey = originalData["native_call_id"].string
                entity = nativeKey.flatMap { liveTools[$0] } ?? original
            case "tool_result":
                nativeKey = originalData["native_correlation_key"].string
                entity = nativeKey.flatMap { liveResults[$0] } ?? original
            default:
                nativeKey = nil
                entity = original
            }
            guard let id = entity["entity_id"].string ?? entity["item_id"].string else { return }
            let identity = nativeKey.map { "\(kind):\($0)" } ?? "entity:\(id)"
            guard emitted.insert(identity).inserted else { return }
            let data = entity["body"]["data"]
            switch kind {
            case "message":
                let role = data["role"].string ?? "assistant"
                // A locally started turn has a complete native user-message
                // stream. Display that sequence until the source's terminal
                // fence retires it, then display the persisted sequence. This
                // selects one representation without claiming cross-ID matches.
                if role == "user", entity["item_id"].string == nil,
                   let turn = sourceTurns[id], liveUserTurns.contains(turn) { return }
                let initialCount = records.count
                // Provider attachment manifests belong to a single native message.
                // Reuse its transport's presentation parser, retaining the complete
                // entity as raw evidence rather than showing generated wire text.
                if role == "user", let presentation = Self.attachmentPresentation(data) {
                    append(entity, suffix: "attachments", role: role, text: presentation.text,
                           nativeID: data["native_message_id"].string, sourceContent: presentation.content)
                    return
                }
                for (index, part) in data["parts"].array.enumerated() {
                    switch part["type"].string {
                    case "text", "reasoning":
                        append(entity, suffix: "part-\(index)", role: part["type"].string == "reasoning" ? "reasoning" : role,
                               text: part["data"].string ?? "", nativeID: data["native_message_id"].string)
                    case "tool_call":
                        if let ref = part["data"]["invocation_id"].string, let tool = entities[ref] { emit(tool) }
                    case "tool_result":
                        if let ref = part["data"]["result_id"].string, let result = entities[ref] { emit(result) }
                    case "artifact":
                        if let ref = part["data"]["entity_id"].string, let artifact = entities[ref] {
                            appendArtifact(artifact, owner: entity, suffix: "part-\(index)", role: role)
                        }
                    case "opaque":
                        let value = part["data"]["value"]
                        // Opaque is not a text format. Only recognized media blocks
                        // enter the existing attachment renderer; encrypted reasoning
                        // and transport metadata remain in the raw source record.
                        let sourceContent = RawTranscriptJSON.array([value]).jsonText
                        let media = Message(role: role, text: "", toolName: nil, toolInput: nil, toolOutput: nil,
                                            sourceContent: sourceContent)
                        if !SourceContent.blocks(media).isEmpty {
                            append(entity, suffix: "part-\(index)", role: role, text: "", sourceContent: sourceContent)
                        }
                    default: break
                    }
                }
                if records.count == initialCount {
                    append(entity, suffix: "raw", role: role, text: "", rawOnly: true)
                }
            case "tool_invocation":
                append(entity, suffix: "call", role: "tool_use", text: "", nativeID: data["native_call_id"].string,
                       toolName: data["name"].string, input: data["raw_arguments"].string ?? data["decoded_arguments"].jsonText,
                       parentTool: data["parent_invocation_id"].string)
            case "tool_result":
                let text = data["parts"].array.compactMap { part -> String? in
                    if ["text", "reasoning"].contains(part["type"].string ?? "") { return part["data"].string }
                    if part["type"].string == "structured" { return part["data"]["value"].jsonText }
                    return nil
                }.joined(separator: "\n")
                var media = data["parts"].array.compactMap { part -> RawTranscriptJSON? in
                    if part["type"].string == "opaque" { return part["data"]["value"] }
                    if part["type"].string == "artifact", let id = part["data"]["entity_id"].string,
                       let artifact = entities[id] { return Self.artifactContent(artifact, title: "Tool attachment") }
                    return nil
                }
                if let ref = data["full_artifact"]["entity_id"].string, let artifact = entities[ref] {
                    media.append(Self.artifactContent(artifact, title: "Full output"))
                }
                append(entity, suffix: "result", role: "tool_result", text: "", output: text,
                       parentTool: data["native_correlation_key"].string, isError: data["outcome"].string == "error",
                       sourceContent: media.isEmpty ? nil : RawTranscriptJSON.array(media).jsonText)
            case "artifact":
                appendArtifact(entity, owner: entity, suffix: "artifact", role: "assistant")
            case "context_boundary":
                append(entity, suffix: "boundary", role: "lifecycle", text: "",
                       lifecycle: data["kind"].string == "compaction" ? "context_compacted" : data["kind"].string)
                if let summary = data["summary"]["entity_id"].string, let value = entities[summary] { emit(value) }
            case "entry": emitMetadata(entity, payload: data["payload"])
            default: append(entity, suffix: "raw", role: "system", text: "", rawOnly: true)
            }
        }

        private static func attachmentPresentation(_ data: RawTranscriptJSON) -> (text: String, content: String)? {
            #if canImport(SQACPHost)
            let parts = data["parts"].array
            // Do not discard unknown or interleaved non-message content.
            guard parts.allSatisfy({ ["text", "artifact", "opaque"].contains($0["type"].string ?? "") }) else { return nil }
            let presentation = AgentPromptAttachments.presentation(textBlocks: parts.compactMap {
                $0["type"].string == "text" ? $0["data"].string : nil
            })
            guard !presentation.attachments.isEmpty else { return nil }
            let blocks = presentation.attachments.map { attachment -> RawTranscriptJSON in
                var fields: [String: RawTranscriptJSON] = [
                    "type": .string(attachment.kind == .image ? "image" : "attachment"),
                    "title": .string(attachment.name),
                ]
                if let uri = attachment.uri {
                    fields["path"] = .string(uri)
                    fields["url"] = .string(uri)
                }
                return .object(fields)
            }
            guard let content = RawTranscriptJSON.array(blocks).jsonText else { return nil }
            return (presentation.text, content)
            #else
            return nil
            #endif
        }

        mutating func appendArtifact(_ entity: RawTranscriptJSON, owner: RawTranscriptJSON, suffix: String, role: String) {
            let data = entity["body"]["data"]
            let location = data["content"]["data"]["uri"].string ?? data["native_locations"].array.first?.string
            if let location {
                let isImage = data["media_type"].string?.hasPrefix("image/") == true || location.hasPrefix("data:image/")
                let content = RawTranscriptJSON.array([.object([
                    "type": .string(isImage ? "image" : "attachment"), "url": .string(location), "path": .string(location)
                ])]).jsonText
                append(owner, suffix: suffix, role: role, text: "", sourceContent: content)
            } else {
                append(owner, suffix: suffix, role: role, text: "Attachment preview unavailable")
            }
        }

        private static func artifactContent(_ entity: RawTranscriptJSON, title: String) -> RawTranscriptJSON {
            let data = entity["body"]["data"]
            let location = data["content"]["data"]["uri"].string ?? data["native_locations"].array.first?.string
            var content: [String: RawTranscriptJSON] = ["type": .string("attachment"), "title": .string(title)]
            if let location {
                content["path"] = .string(location)
                content["url"] = .string(location)
                if data["media_type"].string?.hasPrefix("image/") == true { content["type"] = .string("image") }
            }
            return .object(content)
        }

        mutating func append(_ entity: RawTranscriptJSON, suffix: String, role: String, text: String,
                             nativeID: String? = nil, toolName: String? = nil, input: String? = nil,
                             output: String? = nil, parentTool: String? = nil, isError: Bool? = nil, context: String? = nil,
                             sourceContent: String? = nil, rawOnly: Bool = false,
                             turnID: String? = nil, lifecycle: String? = nil, activity: String? = nil) {
            guard let identity = entity["entity_id"].string ?? entity["item_id"].string else { return }
            let metadata = sourceMetadata[identity] ?? .null
            let data = entity["body"]["data"]
            let message = Message(role: role, text: text, toolName: toolName, toolInput: input, toolOutput: output,
                eventID: nativeID, parentToolUseID: parentTool,
                sourceTurnID: turnID ?? entity["native_turn_id"].string ?? sourceTurns[identity],
                assistantPhase: data["assistant_phase"].string ?? metadata["payload"]["phase"].string,
                lifecycleEvent: lifecycle,
                sourceRecordType: entity["item_id"].string == nil ? "agent_history" : "agent_live",
                sourceContent: sourceContent,
                toolResultIsError: isError, contextLabel: context)
            var decorated = message
            let callID = data["native_call_id"].string ?? data["native_correlation_key"].string
            let liveDetails = Self.merge(liveMetadata[identity] ?? .null, callID.flatMap { liveMetadata[$0] } ?? .null)
            if let phase = liveDetails["phase"].string { decorated.assistantPhase = phase }
            decorated.structuredActivity = activity ?? liveDetails["activity"].jsonText
            if let planID = liveDetails["planDocument"]["id"].string {
                let original = decorated.structuredActivity.flatMap { try? JSONDecoder().decode(RawTranscriptJSON.self, from: Data($0.utf8)) } ?? .null
                decorated.structuredActivity = Self.merge(original, .object(["planDocument": liveDetails["planDocument"]])).jsonText
                emittedPlanIDs.insert(planID)
            }
            decorated.mcpAppJSON = liveDetails["mcpApp"].jsonText
            decorated.activityStatus = liveDetails["status"].string ?? data["status"].string
            decorated.outputCompleteness = data["completeness"].string
            records.append(TranscriptRecord(recordID: "runtime:\(identity):\(suffix)", record: decorated,
                                            rawJSON: try? entity.prettyPrinted(), isRawOnly: rawOnly))
        }
    }
}

extension RawTranscriptJSON {
    subscript(_ key: String) -> Self {
        if case .object(let object) = self { return object[key] ?? .null }
        return .null
    }
    var string: String? { if case .string(let value) = self { return value }; return nil }
    var array: [Self] { if case .array(let value) = self { return value }; return [] }
    var bool: Bool? { if case .bool(let value) = self { return value }; return nil }
    var integer: Int { if case .number(let value) = self { return NSDecimalNumber(decimal: value).intValue }; return 0 }
    var jsonText: String? { if case .null = self { return nil }; return try? prettyPrinted() }
}
