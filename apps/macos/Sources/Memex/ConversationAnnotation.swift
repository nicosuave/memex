import SwiftUI

struct ConversationAnnotation: Codable, Equatable, Sendable {
    let attachmentID: String
    let source: String
    var passage: String
    var comment: String

    var promptText: String {
        "Annotation on attachment \(attachmentID)\nLocation: \(source)\n\nSelected passage or region:\n\(passage)\n\nComment:\n\(comment)"
    }
}

extension ConversationAttachment {
    func annotated(passage: String, comment: String) throws -> Self {
        let annotation = ConversationAnnotation(attachmentID: self.annotation?.attachmentID ?? id,
            source: self.annotation?.source ?? path, passage: passage, comment: comment)
        let payload = try Self.text(title: "Comment: \(title)", text: annotation.promptText, source: annotation.source)
        return Self(id: self.annotation == nil ? payload.id : id,
            title: self.annotation == nil ? "Comment: \(title)" : title,
            path: path, content: payload.content, annotation: annotation)
    }
}

struct ConversationAnnotationEditor: View {
    let attachment: ConversationAttachment
    let capturedSource: ConversationAttachment?
    let save: (String, String) -> Bool
    @Environment(\.dismiss) private var dismiss
    @State private var passage: String
    @State private var comment: String
    @State private var failed = false

    init(attachment: ConversationAttachment, capturedSource: ConversationAttachment? = nil, save: @escaping (String, String) -> Bool) {
        self.attachment = attachment
        self.capturedSource = capturedSource
        self.save = save
        _passage = State(initialValue: attachment.annotation?.passage ?? "")
        _comment = State(initialValue: attachment.annotation?.comment ?? "")
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text(attachment.annotation == nil ? "Annotate captured context" : "Edit annotation").font(.title2)
            Text(attachment.annotation?.source ?? attachment.path).font(.caption).textSelection(.enabled)
            if let capturedSource {
                DisclosureGroup("View captured source") { ConversationCapturedSource(attachment: capturedSource) }
            }
            Text("Passage, line range, or image region").font(.caption)
            TextField("Describe the location within this captured attachment", text: $passage)
            TextEditor(text: $comment).frame(minHeight: 120)
            Text("This comment refers to the captured attachment. Its original contents are retained.").font(.caption).foregroundStyle(.secondary)
            if failed { Text("The comment could not be saved. Keep this editor open and resolve the attachment error.").foregroundStyle(.orange) }
            HStack {
                Spacer()
                Button("Cancel") { dismiss() }
                Button("Save comment") {
                    if save(passage, comment) { dismiss() } else { failed = true }
                }.disabled(comment.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }.padding(20).frame(width: 520)
    }
}

/// Navigation stays within the retained snapshot, including for remote sources.
/// Opening an annotation never rereads a source path that may now have changed.
private struct ConversationCapturedSource: View {
    let attachment: ConversationAttachment
    var body: some View {
        #if canImport(SQACPHost)
        if let block = try? attachment.promptContent() {
            switch block {
            case .text(let text):
                ScrollView { Text(text.text).font(.callout).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading) }
                    .frame(maxHeight: 180)
            case .image(let image):
                if let data = Data(base64Encoded: image.data), let image = NSImage(data: data) {
                    Image(nsImage: image).resizable().scaledToFit().frame(maxHeight: 220)
                }
            default:
                Text("Captured attachment: \(attachment.title) · \(attachment.content.count) retained bytes").font(.caption)
            }
        }
        #else
        Text("Captured attachment: \(attachment.title)").font(.caption)
        #endif
    }
}
