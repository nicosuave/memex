import AppKit

/// Structured presentation is separate from the original transcript used by find
/// and the raw-content disclosure. No source text is discarded or rewritten.
@MainActor enum ToolContentRenderer {
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
                if index > 0 { append("\n\n", to: result) }
                if raw { append(part, to: result, code: true) }
                else { content(part, to: result, code: message.role == "tool_use") }
            }
            if record.id != records.last?.id { append("\n\n", to: result) }
        }
        return result
    }

    private static func content(_ text: String, to result: NSMutableAttributedString, code: Bool) {
        if let value = json(text) {
            valueBlocks(value, to: result)
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
                valueBlocks(value, to: result)
                return
            }
        }
        if isOpaque(text) { opaque(text, to: result) }
        else { append(text, to: result, code: code) }
    }

    private static func json(_ text: String) -> Any? {
        let trimmed = text.trimmingCharacters(in: .whitespacesAndNewlines)
        guard trimmed.hasPrefix("{") || trimmed.hasPrefix("[") else { return nil }
        return try? JSONSerialization.jsonObject(with: Data(trimmed.utf8))
    }

    private static func valueBlocks(_ value: Any, to result: NSMutableAttributedString, path: String = "") {
        if let object = value as? [String: Any], !object.isEmpty {
            for key in object.keys.sorted() {
                valueBlocks(object[key]!, to: result, path: path.isEmpty ? key : "\(path).\(key)")
            }
        } else if let array = value as? [Any], !array.isEmpty {
            for (index, entry) in array.enumerated() { valueBlocks(entry, to: result, path: "\(path)[\(index)]") }
        } else {
            if result.length > 0, !result.string.hasSuffix("\n") { append("\n", to: result) }
            if !path.isEmpty { label(path, to: result) }
            if let text = value as? String {
                if isOpaque(text) { opaque(text, to: result) }
                else if let nested = json(text) { valueBlocks(nested, to: result) }
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
        style.paragraphSpacingBefore = 8
        style.paragraphSpacing = 4
        result.append(NSAttributedString(string: text + "\n", attributes: [
            .font: NSFont.systemFont(ofSize: 12, weight: .semibold),
            .foregroundColor: NSColor.secondaryLabelColor, .paragraphStyle: style
        ]))
    }

    private static func append(_ text: String, to result: NSMutableAttributedString, code: Bool = false) {
        let style = NSMutableParagraphStyle()
        style.lineSpacing = 3
        style.paragraphSpacing = 5
        result.append(NSAttributedString(string: text, attributes: [
            .font: code ? NSFont.monospacedSystemFont(ofSize: 12, weight: .regular) : NSFont.systemFont(ofSize: 14),
            .foregroundColor: NSColor.labelColor, .paragraphStyle: style
        ]))
    }
}
