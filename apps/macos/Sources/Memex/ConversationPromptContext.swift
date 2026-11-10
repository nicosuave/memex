import AppKit
import Foundation
import UniformTypeIdentifiers

#if canImport(SQACPHost)
import SQACP
import SQACPHost
#endif

extension ConversationAttachment {
    func selectedTranscriptText(in sessionID: String) -> TranscriptSelection? {
        if let selection = transcriptSelection {
            return selection.sessionID == sessionID ? selection : nil
        }
        // Previously saved chips retain the exact source and captured text in
        // their provider block. Recover those bytes without rereading any file.
        #if canImport(SQACPHost)
        let prefix = "\(sessionID)#"
        guard title == "Selected text", path.hasPrefix(prefix),
              case .text(let block) = try? promptContent() else { return nil }
        let header = "Attached context: \(title)\nSource: \(path)\n\n"
        guard block.text.hasPrefix(header) else { return nil }
        let sourceIDs = String(path.dropFirst(prefix.count)).components(separatedBy: ", \(prefix)")
        guard sourceIDs.allSatisfy({ !$0.isEmpty }) else { return nil }
        return TranscriptSelection(text: String(block.text.dropFirst(header.count)), sourceIDs: sourceIDs, sessionID: sessionID)
        #else
        return nil
        #endif
    }

    /// Immutable context is sent as a real text block, not a filesystem reference
    /// that could change between composition and provider delivery.
    static func text(title: String, text: String, source: String) throws -> Self {
        #if canImport(SQACPHost)
        guard text.utf8.count <= AgentPromptAttachments.maximumBytes else {
            throw ConversationRuntimeError(message: "The selected context exceeds 20 MB. Select a smaller excerpt.")
        }
        let block = AcpPromptContentBlock.text("Attached context: \(title)\nSource: \(source)\n\n\(text)")
        return Self(id: UUID().uuidString, title: title, path: source, content: try JSONEncoder().encode(block))
        #else
        throw ConversationRuntimeError(message: "This build does not include context attachments.")
        #endif
    }

    static func image(title: String, pngData: Data, source: String) throws -> Self {
        #if canImport(SQACPHost)
        let image = try ConversationImageNormalization.normalize(pngData)
        let block = AcpPromptContentBlock.image(.init(data: image.data.base64EncodedString(), mimeType: image.mimeType, uri: source))
        return Self(id: UUID().uuidString, title: title, path: source, content: try JSONEncoder().encode(block))
        #else
        throw ConversationRuntimeError(message: "This build does not include image attachments.")
        #endif
    }

    static func validate(_ attachments: [Self], controls: ConversationControls) throws {
        #if canImport(SQACPHost)
        guard attachments.count <= AgentPromptAttachments.maximumCount else {
            throw ConversationRuntimeError(message: "Attach at most \(AgentPromptAttachments.maximumCount) context items to one message.")
        }
        // Include text in the retained payload budget as well as media. The
        // provider validator intentionally excludes text from its media limit.
        guard attachments.reduce(0, { $0 + $1.content.count }) <= AgentPromptAttachments.maximumBytes * 2 else {
            throw ConversationRuntimeError(message: "The message has too much attached context. Remove an attachment before continuing.")
        }
        try AgentPromptAttachments.validate(try attachments.map { try $0.promptContent() }, capabilities: controls.promptCapabilities)
        #else
        throw ConversationRuntimeError(message: "This build does not include context attachments.")
        #endif
    }
}

@MainActor extension LiveConversation {
    func appendTranscriptSelection(_ selection: TranscriptSelection) -> String? {
        let source = selection.sourceIDs.map { "\(session.id)#\($0)" }.joined(separator: ", ")
        do {
            var attachment = try ConversationAttachment.text(title: "Selected text", text: selection.text, source: source)
            var captured = selection
            captured.sessionID = session.id
            attachment.transcriptSelection = captured
            guard appendCapturedContext([attachment]) else { return attachmentError }
            return nil
        } catch {
            reportAttachmentError(error.localizedDescription)
            return attachmentError
        }
    }

    @discardableResult
    func appendContext(title: String, text: String, source: String) -> Bool {
        do { return appendCapturedContext([try .text(title: title, text: text, source: source)]) }
        catch { reportAttachmentError(error.localizedDescription); return false }
    }

    @discardableResult
    func appendImageContext(title: String, pngData: Data, source: String) -> Bool {
        do { return appendCapturedContext([try .image(title: title, pngData: pngData, source: source)]) }
        catch { reportAttachmentError(error.localizedDescription); return false }
    }

    @discardableResult
    func appendCapturedContext(_ items: [ConversationAttachment]) -> Bool {
        let combined = attachments + items
        do {
            if isOpenElsewhere {
                throw ConversationRuntimeError(message: "This conversation is open in another app. Close it there before adding context here.")
            }
            if let ownershipError { throw ConversationRuntimeError(message: ownershipError) }
            try ConversationAttachment.validate(combined, controls: snapshot.controls ?? ConversationControls())
            guard replaceDraft(text: draft, attachments: combined) else {
                throw ConversationRuntimeError(message: "The draft is busy. Add this context after the current operation finishes.")
            }
            reportAttachmentError(nil)
            return true
        } catch { reportAttachmentError(error.localizedDescription); return false }
    }
}

/// File drops and image-only clipboard items share the explicit chooser's
/// capture/validation path. The returned items are installed atomically.
enum ConversationClipboard {
    static let supportedTypes: [UTType] = [.fileURL, .png, .tiff, .jpeg, .heic]

    @MainActor static func capture(_ providers: [NSItemProvider], controls: ConversationControls) async throws -> [ConversationAttachment] {
        var items: [ConversationAttachment] = []
        for provider in providers {
            if provider.hasItemConformingToTypeIdentifier(UTType.fileURL.identifier) {
                let data = try await load(provider, type: .fileURL)
                guard let url = URL(dataRepresentation: data, relativeTo: nil), url.isFileURL else {
                    throw ConversationRuntimeError(message: "The dropped file URL could not be read.")
                }
                items += try await Task.detached {
                    try ConversationAttachment.capture([url], controls: controls, existing: [])
                }.value
            } else if let type = [UTType.png, .tiff, .jpeg, .heic].first(where: { provider.hasItemConformingToTypeIdentifier($0.identifier) }) {
                let data = try await load(provider, type: type)
                guard data.count <= 20 * 1024 * 1024 else {
                    throw ConversationRuntimeError(message: "The clipboard image exceeds 20 MB. Resize it before attaching.")
                }
                items.append(try .image(title: provider.suggestedName ?? "Pasted image", pngData: data,
                    source: "memex-context://clipboard/\(UUID().uuidString)"))
            }
        }
        try ConversationAttachment.validate(items, controls: controls)
        return items
    }

    @MainActor private static func load(_ provider: NSItemProvider, type: UTType) async throws -> Data {
        try await withCheckedThrowingContinuation { continuation in
            provider.loadDataRepresentation(forTypeIdentifier: type.identifier) { data, error in
                if let data { continuation.resume(returning: data) }
                else { continuation.resume(throwing: error ?? ConversationRuntimeError(message: "The clipboard attachment is unavailable.")) }
            }
        }
    }
}
