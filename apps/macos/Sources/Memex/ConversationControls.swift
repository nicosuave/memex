import Foundation

/// UI-facing values keep history-only builds independent of the local runtime.
struct ConversationControls: Equatable, Sendable {
    struct SlashCommand: Identifiable, Equatable, Sendable {
        let name: String
        let description: String
        let hint: String?
        var id: String { name }
    }
    struct Choice: Identifiable, Equatable, Sendable {
        let id: String
        let title: String
    }

    struct Configuration: Identifiable, Equatable, Sendable {
        let id: String
        let title: String
        let category: String?
        let selectedID: String?
        let choices: [Choice]

        var selectedTitle: String? { choices.first { $0.id == selectedID }?.title ?? selectedID }
    }

    var models: [Choice] = []
    var selectedModelID: String?
    var configurations: [Configuration] = []
    var supportsImages = false
    var supportsAudio = false
    var supportsFileContents = false
    var pendingChanges = false
    var appliesToNextTurn = false
    var slashCommands: [SlashCommand] = []

    var modelTitle: String? { models.first { $0.id == selectedModelID }?.title ?? selectedModelID }
    var reasoning: Configuration? {
        configurations.first { $0.category == "thought_level" || $0.id == "reasoning_effort" }
    }
}

/// Explicit file attachments capture their bytes when selected. The opaque,
/// versioned provider block survives draft restoration without rereading the file.
struct ConversationAttachment: Identifiable, Codable, Equatable, Sendable {
    let id: String
    let title: String
    let path: String
    let content: Data
    var annotation: ConversationAnnotation? = nil
    var transcriptSelection: TranscriptSelection? = nil
}

struct ConversationDelivery: Equatable, Sendable {
    let commandID: String
    let status: String
    let error: String?
    let nativeTurnID: String?
    let nativeMessageID: String?

    /// `completed` is the provider-operation acknowledgement, not the local
    /// enqueue receipt. Its exact native identity is independent of how a
    /// transcript importer represents (or omits) the echoed user message.
    var isAccepted: Bool {
        status == "completed" && (nativeTurnID?.isEmpty == false || nativeMessageID?.isEmpty == false)
    }

    func hasNativeEcho(in records: [TranscriptRecord]) -> Bool {
        records.contains {
            $0.record.role == "user" && (
                (nativeMessageID != nil && $0.record.eventID == nativeMessageID)
                || (nativeTurnID != nil && $0.record.sourceTurnID == nativeTurnID)
            )
        }
    }
}

/// An outgoing intent is separate from provider history and from the next draft.
/// Its ID survives relaunch so a native echo can confirm this exact submission.
struct ConversationPendingPrompt: Codable, Equatable, Sendable {
    enum Phase: String, Codable, Sendable { case preparing, awaitingConfirmation, uncertain, notSent }
    let commandID: String
    let issuedAt: String
    let text: String
    let attachments: [ConversationAttachment]
    var phase: Phase
    var isSteer: Bool? = nil

    init(_ command: ConversationCommand) {
        commandID = command.id
        issuedAt = command.issuedAt
        text = command.text
        attachments = command.attachments
        phase = .preparing
        isSteer = command.action == .steer
    }
}

/// Undispatched local intent. Its identity becomes the provider command identity
/// when dispatched, including after queue edits, reordering, or app relaunch.
struct ConversationQueuedPrompt: Identifiable, Codable, Equatable, Sendable {
    let id: String
    let issuedAt: String
    var text: String
    var attachments: [ConversationAttachment]

    init(_ command: ConversationCommand) {
        id = command.id
        issuedAt = command.issuedAt
        text = command.text
        attachments = command.attachments
    }

    func command(steer: Bool = false) -> ConversationCommand {
        ConversationCommand(steer ? .steer : .prompt, text: text, attachments: attachments,
                            id: id, issuedAt: issuedAt)
    }
}
