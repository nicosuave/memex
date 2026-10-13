import Foundation
import UniformTypeIdentifiers

#if canImport(SQACPHost)
import SQACP
import SQACPHost
#endif

extension ConversationAttachment {
    static func capture(_ urls: [URL], controls: ConversationControls,
                        existing: [Self]) throws -> [Self] {
        #if canImport(SQACPHost)
        let capabilities = controls.promptCapabilities
        let captured = try urls.map { url in
            let values = try url.resourceValues(forKeys: [.contentTypeKey, .isRegularFileKey, .fileSizeKey])
            let block: AcpPromptContentBlock
            if (values.contentType ?? UTType(filenameExtension: url.pathExtension))?.conforms(to: .image) == true {
                guard capabilities.image else { throw ConversationRuntimeError(message: "The selected provider/model does not support image attachments.") }
                guard values.isRegularFile == true, (values.fileSize ?? 0) <= ConversationImageNormalization.maximumInputBytes else {
                    throw ConversationRuntimeError(message: "Attach an image file no larger than 20 MB.")
                }
                let image = try ConversationImageNormalization.normalize(Data(contentsOf: url))
                block = .image(.init(data: image.data.base64EncodedString(), mimeType: image.mimeType, uri: url.absoluteString))
            } else {
                block = try AgentPromptAttachments.loadFile(url, capabilities: capabilities)
            }
            return Self(id: UUID().uuidString, title: url.lastPathComponent, path: url.path,
                        content: try JSONEncoder().encode(block))
        }
        try AgentPromptAttachments.validate(try (existing + captured).map { try $0.promptContent() },
                                            capabilities: capabilities)
        return captured
        #else
        throw ConversationRuntimeError(message: "This build does not include attachment support.")
        #endif
    }

    #if canImport(SQACPHost)
    func promptContent() throws -> AcpPromptContentBlock {
        try JSONDecoder().decode(AcpPromptContentBlock.self, from: content)
    }
    #endif
}

#if canImport(SQACPHost)
extension ConversationControls {
    var promptCapabilities: AcpPromptCapabilities {
        .init(image: supportsImages, audio: supportsAudio, embeddedContext: supportsFileContents)
    }

    init(_ settings: AgentConversationSettings) {
        models = settings.models.map { .init(id: $0.id, title: $0.name) }
        selectedModelID = settings.selectedModelID
        configurations = settings.configOptions.map { option in
            .init(id: option.id, title: option.name, category: option.category,
                  selectedID: option.currentValue,
                  choices: option.choices.map { .init(id: $0.value, title: $0.name) })
        }
        supportsImages = settings.promptCapabilities.image
        supportsAudio = settings.promptCapabilities.audio
        supportsFileContents = settings.promptCapabilities.embeddedContext
        pendingChanges = !settings.pendingControlCommandIDs.isEmpty
        appliesToNextTurn = settings.controlsApplyToNextTurn
        slashCommands = settings.slashCommands.map { .init(name: $0.name, description: $0.description, hint: $0.hint) }
    }
}
#endif
