import AppKit

/// Structured presentation is separate from the original transcript used by find
/// and the raw-content disclosure. No source text is discarded or rewritten.
@MainActor enum ToolContentRenderer {
    /// Only explicitly typed images or image-view tool arguments become previews.
    /// Relative paths stay as transcript text because their host/workdir is unknown.
    static func imageSources(_ records: [TranscriptRecord]) -> [String] {
        var sources: [String] = []
        func add(_ source: String?) {
            guard let source, source.hasPrefix("/") || source.hasPrefix("https://") || source.hasPrefix("http://"), !sources.contains(source) else { return }
            sources.append(source)
        }
        func inspect(_ value: Any) {
            if let object = value as? [String: Any] {
                if object["type"] as? String == "image" || object["type"] as? String == "image_url" {
                    add(object["image_url"] as? String ?? object["url"] as? String ?? (object["image_url"] as? [String: Any])?["url"] as? String)
                }
                for key in ["content", "result"] { if let nested = object[key] { inspect(nested) } }
            } else if let array = value as? [Any] { for item in array { inspect(item) } }
        }
        for record in records {
            let message = record.record
            let name = message.toolName?.components(separatedBy: "__").last?.components(separatedBy: ".").last
            if ["view_image", "open_image"].contains(name ?? ""), let input = message.toolInput.flatMap(json) as? [String: Any] {
                add(input["path"] as? String ?? input["image_path"] as? String)
            }
            for part in [message.toolOutput, message.role == "tool_use" ? nil : message.text] {
                if let part, let value = json(part) { inspect(value) }
            }
        }
        return sources
    }

    /// Split the same rendered field walk into native cards; metadata and generic
    /// values keep their original attributed presentation and exact ordering.
    static func richBlocks(_ records: [TranscriptRecord], rendered: NSAttributedString? = nil) -> [RichContentBlock] {
        let rendered = rendered ?? render(records)
        var blocks: [RichContentBlock] = []
        rendered.enumerateAttribute(ToolPresentationSupport.codeLanguageAttribute, in: NSRange(location: 0, length: rendered.length)) { language, range, _ in
            let part = rendered.attributedSubstring(from: range)
            if let language = language as? String { blocks.append(.code(part.string, language: language)) }
            else { blocks.append(.attributed(part)) }
        }
        var seenImages = Set<Data>()
        func embeddedImages(_ value: Any) {
            if let object = value as? [String: Any] {
                if object["type"] as? String == "image", let encoded = object["data"] as? String,
                   let mime = object["mimeType"] as? String ?? object["mime_type"] as? String,
                   mime.hasPrefix("image/"), encoded.utf8.count <= 28_000_000,
                   let data = Data(base64Encoded: encoded), data.count <= 20_000_000, seenImages.insert(data).inserted {
                    blocks.append(.embeddedImage(label: "Image result", data: data, mimeType: mime))
                }
                for key in ["content", "result"] { if let nested = object[key] { embeddedImages(nested) } }
            } else if let array = value as? [Any] { for item in array { embeddedImages(item) } }
        }
        for record in records {
            let message = record.record
            for part in [message.toolOutput, message.role == "tool_use" ? nil : message.text] {
                if let part, let value = json(part) { embeddedImages(value) }
            }
        }
        for source in imageSources(records) { blocks.append(.attachment(label: "", source: source, image: true)) }
        return blocks
    }

    static func render(_ records: [TranscriptRecord], raw: Bool = false) -> NSAttributedString {
        let result = NSMutableAttributedString(string: "")
        for record in records {
            let message = record.record
            if records.count > 1 { label(message.role == "tool_use" ? "Input" : "Output", to: result) }
            var parts: [String] = []
            for value in [message.toolInput, message.toolOutput, message.text] {
                if let value, value.nilIfBlank != nil, !parts.contains(value) { parts.append(value) }
            }
            for (index, part) in parts.enumerated() {
                if index > 0 { append(raw ? "\n\n" : "\n", to: result) }
                if raw { append(part, to: result, code: true) }
                else { content(part, to: result, code: message.role == "tool_use", toolName: message.toolName ?? records.first?.record.toolName ?? "") }
            }
            if record.id != records.last?.id {
                if raw { append("\n\n", to: result) }
                else if !result.string.hasSuffix("\n") { append("\n", to: result) }
            }
        }
        return result
    }

    private static func content(_ text: String, to result: NSMutableAttributedString, code: Bool, toolName: String) {
        let text = text.trimmingCharacters(in: .newlines)
        if let value = json(text) {
            valueBlocks(value, to: result, toolName: toolName)
            return
        }
        // Execution wrappers prepend timing/status lines to a JSON result. Only
        // split at a line boundary when the entire remaining suffix parses.
        for boundary in text.indices where text[boundary] == "\n" {
            var next = text.index(after: boundary)
            while next < text.endIndex, text[next] == " " || text[next] == "\t" {
                next = text.index(after: next)
            }
            guard next < text.endIndex, text[next] == "{" || text[next] == "[" else { continue }
            let suffix = String(text[next...])
            if let value = json(suffix) {
                append(String(text[...boundary]), to: result, code: code)
                valueBlocks(value, to: result, toolName: toolName)
                return
            }
        }
        if isOpaque(text) { opaque(text, to: result) }
        else if let decorated = ToolPresentationSupport.decorate(text, path: "", toolName: toolName, code: code) { result.append(decorated) }
        else { append(text, to: result, code: code) }
    }

    private static func json(_ text: String) -> Any? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.hasPrefix("{") || trimmed.hasPrefix("[") else { return nil }
        return try? JSONSerialization.jsonObject(with: Data(trimmed.utf8))
    }

    private static func valueBlocks(_ value: Any, to result: NSMutableAttributedString, path: String = "", toolName: String) {
        if let object = value as? [String: Any], !object.isEmpty {
            for key in object.keys.sorted() {
                valueBlocks(object[key]!, to: result, path: path.isEmpty ? key : "\(path).\(key)", toolName: toolName)
            }
        } else if let array = value as? [Any], !array.isEmpty {
            for (index, entry) in array.enumerated() { valueBlocks(entry, to: result, path: "\(path)[\(index)]", toolName: toolName) }
        } else {
            if result.length > 0, !result.string.hasSuffix("\n") { append("\n", to: result) }
            if !path.isEmpty { label(path, to: result) }
            if let text = value as? String {
                if isOpaque(text) { opaque(text, to: result) }
                else if let nested = json(text) { valueBlocks(nested, to: result, toolName: toolName) }
                else if let decorated = ToolPresentationSupport.decorate(text, path: path, toolName: toolName, code: false) { result.append(decorated) }
                else if ["message", "description", "prompt", "instructions", "text"].contains(path.components(separatedBy: ".").last ?? "") {
                    result.append(RichTextRenderer.render(text, font: .systemFont(ofSize: 14)))
                } else { append(text, to: result, code: ["cmd", "command", "code", "script", "output"].contains(path.components(separatedBy: ".").last ?? "")) }
            } else if let data = try? JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed, .sortedKeys]),
                      let text = String(data: data, encoding: .utf8) {
                append(text, to: result)
            }
            append("\n", to: result)
        }
    }

    static func isOpaque(_ text: String) -> Bool {
        guard text.utf8.count >= 256 else { return false }
        let allowed = CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=_-")
        return text.unicodeScalars.allSatisfy { allowed.contains($0) }
    }

    private static func opaque(_ text: String, to result: NSMutableAttributedString) {
        append("Encoded payload · \(text.utf8.count) bytes · Show raw content to reveal", to: result)
    }

    private static func label(_ text: String, to result: NSMutableAttributedString) {
        let style = NSMutableParagraphStyle()
        style.paragraphSpacingBefore = result.length == 0 ? 0 : 4
        style.paragraphSpacing = 2
        result.append(NSAttributedString(string: text + "\n", attributes: [
            .font: NSFont.systemFont(ofSize: 12, weight: .semibold),
            .foregroundColor: NSColor.secondaryLabelColor, .paragraphStyle: style
        ]))
    }

    private static func append(_ text: String, to result: NSMutableAttributedString, code: Bool = false) {
        let style = NSMutableParagraphStyle()
        style.lineSpacing = 2
        style.paragraphSpacing = 0
        result.append(NSAttributedString(string: text, attributes: [
            .font: code ? NSFont.monospacedSystemFont(ofSize: 12, weight: .regular) : NSFont.systemFont(ofSize: 14),
            .foregroundColor: NSColor.labelColor, .paragraphStyle: style
        ]))
    }
}
