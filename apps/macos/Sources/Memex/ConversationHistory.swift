import Foundation

struct ConversationHistoryBoundary: Codable, Equatable, Identifiable, Sendable {
    let recordID: String
    let userMessageID: String
    let turnID: String?
    let nextUserMessageID: String?
    let throughEnd: Bool
    let title: String
    var id: String { recordID }

    static func choices(in records: [TranscriptRecord]) -> [Self] {
        let users = records.filter { $0.record.role == "user" && !$0.record.isInstruction && !$0.isRawOnly }
        var seen: Set<String> = []
        return users.enumerated().compactMap { index, record in
            guard let nativeID = record.record.eventID?.nilIfBlank,
                  seen.insert(record.record.sourceTurnID ?? nativeID).inserted else { return nil }
            let next = users.dropFirst(index + 1).first {
                $0.record.eventID != nativeID && $0.record.sourceTurnID != record.record.sourceTurnID
                    || (record.record.sourceTurnID == nil && $0.record.eventID != nativeID)
            }
            return Self(recordID: record.id, userMessageID: nativeID, turnID: record.record.sourceTurnID,
                        nextUserMessageID: next?.record.eventID, throughEnd: next == nil,
                        title: String(record.record.text.replacingOccurrences(of: "\n", with: " ").prefix(100)))
        }
    }
}

struct ConversationHistoryMutation: Sendable {
    enum Operation: String, Codable, Sendable { case fork, revert }
    let operation: Operation
    let boundary: ConversationHistoryBoundary
}

extension ConversationRuntime {
    func mutateHistory(_ request: ConversationHistoryMutation) async throws -> Session {
        throw ConversationRuntimeError(message: "This provider does not support native history changes.")
    }
}

enum ConversationContextCapture {
    static let maximumBytes = 4 * 1024 * 1024

    /// A captured reference has exact record identities and an explicit scope.
    /// It does not impersonate provider history or issue an instruction to send.
    static func text(session: Session, records: [TranscriptRecord], through recordID: String? = nil) throws -> String {
        let selected: ArraySlice<TranscriptRecord>
        if let recordID {
            guard let index = records.firstIndex(where: { $0.id == recordID }) else {
                throw ConversationRuntimeError(message: "The selected context boundary is no longer available. Reload the conversation.")
            }
            selected = records[...index]
        } else { selected = records[...] }
        var text = "Conversation reference: \(session.title)\nProvider: \(session.source)\nMachine: \(session.machineID)\nNative session: \(session.sessionID)\nSource: \(session.sourcePath)\n\n"
        for row in selected where !row.isRawOnly {
            let message = row.record
            let content = [message.text.nilIfBlank, message.toolName, message.toolInput, message.toolOutput, message.sourceContent]
                .compactMap { $0 }.joined(separator: "\n")
            if content.isEmpty { continue }
            text += "[\(message.role) · record \(row.sourceID)]\n\(content)\n\n"
            guard text.utf8.count <= maximumBytes else {
                throw ConversationRuntimeError(message: "This conversation exceeds the 4 MB context limit. Select an earlier boundary or attach a smaller excerpt.")
            }
        }
        return text
    }
}

enum NativeConversationLocation {
    /// Locate only the acknowledged native identity under its original home.
    /// Never use a guessed runtime ID or another installation's transcript.
    static func session(nativeID: String, source: InAppResumeTarget) throws -> Session {
        if nativeID == source.session.sessionID { return source.session }
        guard UUID(uuidString: nativeID) != nil else {
            throw ConversationRuntimeError(message: "The provider returned an invalid native conversation identity.")
        }
        let path: URL
        switch source.session.source {
        case "claude":
            path = source.providerHome.appendingPathComponent("projects")
                .appendingPathComponent(NewConversationRuntime.claudeProjectDirectory(source.workingDirectory.path))
                .appendingPathComponent(nativeID + ".jsonl")
            guard FileManager.default.fileExists(atPath: path.path) else {
                throw ConversationRuntimeError(message: "The provider acknowledged session \(nativeID), but its native transcript is not available yet. Inspect provider history before retrying.")
            }
        case "codex":
            let root = source.providerHome.appendingPathComponent("sessions")
            guard let files = FileManager.default.enumerator(at: root, includingPropertiesForKeys: [.isRegularFileKey], options: [.skipsHiddenFiles]) else {
                throw ConversationRuntimeError(message: "The original Codex history directory is unavailable.")
            }
            var matches: [URL] = []
            for case let file as URL in files where file.lastPathComponent.lowercased().hasSuffix(nativeID.lowercased() + ".jsonl") {
                guard try file.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile == true else { continue }
                matches.append(file)
            }
            guard matches.count == 1, let match = matches.first else {
                throw ConversationRuntimeError(message: "The provider acknowledged session \(nativeID), but its native transcript could not be identified uniquely. Inspect provider history before retrying.")
            }
            path = match
        default:
            throw ConversationRuntimeError(message: "Use a context branch for this provider; native history mutation is unavailable.")
        }
        return Session(source: source.session.source, sessionID: nativeID, sourcePath: path.path,
                       project: source.session.project, label: "Branch of " + source.session.title,
                       lastAt: Date().ISO8601Format(), cwd: source.workingDirectory.path,
                       repoProject: source.session.repoProject, machine: source.session.machine,
                       conversationKind: "main")
    }
}
