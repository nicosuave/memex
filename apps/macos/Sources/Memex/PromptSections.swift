import AppKit

/// Prompt wrappers are often XML-like rather than valid XML. Recognize standalone
/// tags without interpreting entities, fetching resources, or changing fenced code.
@MainActor enum PromptSections {
    private final class Section {
        let name: String
        let attributes: String
        var parts: [Part] = []
        init(_ name: String = "", attributes: String = "") { self.name = name; self.attributes = attributes }
    }
    private enum Part { case text(String), section(Section) }
    private static let tag = try! NSRegularExpression(pattern: #"^\s*<(/?)([A-Za-z_][\w:.-]*)([^<>]*?)(/?)>\s*$"#)
    private static let inlinePair = try! NSRegularExpression(pattern: #"^\s*<([A-Za-z_][\w:.-]*)([^<>]*)>(.+)</\1>\s*$"#)

    static func hasOpeningSection(_ text: String) -> Bool {
        text.range(of: #"^\s*<[A-Za-z_][\w:.-]*(?:\s[^<>]*)?>"#, options: .regularExpression) != nil
    }

    static func render(_ text: String, font: NSFont) -> NSAttributedString? {
        let root = Section()
        var stack = [root]
        var buffer = ""
        var fence: Character?
        var found = false
        func flush() {
            if !buffer.isEmpty { stack.last!.parts.append(.text(buffer)); buffer = "" }
        }
        // Put boundary tags on their own lines, except inside literal code fences.
        var lines: [String] = []
        var literalFence: Character?
        for line in text.components(separatedBy: "\n") {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("```") || trimmed.hasPrefix("~~~") {
                if literalFence == nil { literalFence = trimmed.first }
                else if literalFence == trimmed.first { literalFence = nil }
                lines.append(line)
            } else if literalFence != nil { lines.append(line) }
            else {
                let separated: String
                if trimmed.hasPrefix("<") {
                    separated = line.replacingOccurrences(of: #"</?[A-Za-z_][\w:.-]*(?:\s[^<>]*?)?/?>"#,
                        with: "\n$0\n", options: .regularExpression)
                } else {
                    separated = line.replacingOccurrences(of: #"(?<=\S)(</[A-Za-z_][\w:.-]*>\s*)$"#,
                        with: "\n$1", options: .regularExpression)
                }
                lines += separated.components(separatedBy: "\n")
            }
        }
        for original in lines {
            let trimmed = original.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("```") || trimmed.hasPrefix("~~~") {
                if fence == nil { fence = trimmed.first }
                else if fence == trimmed.first { fence = nil }
                buffer += original + "\n"
                continue
            }
            if fence != nil { buffer += original + "\n"; continue }
            let ns = original as NSString
            let range = NSRange(location: 0, length: ns.length)
            if let match = inlinePair.firstMatch(in: original, range: range) {
                flush()
                let section = Section(ns.substring(with: match.range(at: 1)), attributes: ns.substring(with: match.range(at: 2)))
                section.parts.append(.text(ns.substring(with: match.range(at: 3))))
                stack.last!.parts.append(.section(section)); found = true
            } else if let match = tag.firstMatch(in: original, range: range) {
                let name = ns.substring(with: match.range(at: 2))
                if ns.substring(with: match.range(at: 1)) == "/" {
                    guard stack.count > 1, stack.last!.name == name else { buffer += original + "\n"; continue }
                    flush(); stack.removeLast()
                } else {
                    flush()
                    let section = Section(name, attributes: ns.substring(with: match.range(at: 3)))
                    stack.last!.parts.append(.section(section)); found = true
                    if ns.substring(with: match.range(at: 4)) != "/" { stack.append(section) }
                }
            } else { buffer += original + "\n" }
        }
        flush()
        guard found else { return nil }
        return render(root, font: font)
    }

    private static func render(_ section: Section, font: NSFont) -> NSAttributedString {
        let result = NSMutableAttributedString(string: "")
        if !section.name.isEmpty {
            let label = section.name.replacingOccurrences(of: "_", with: " ").replacingOccurrences(of: "-", with: " ").capitalized
            let style = NSMutableParagraphStyle()
            style.paragraphSpacing = 8
            result.append(NSAttributedString(string: label + "\n", attributes: [
                .font: NSFont.systemFont(ofSize: 12, weight: .semibold),
                .foregroundColor: NSColor.secondaryLabelColor, .paragraphStyle: style,
            ]))
            let metadata = section.attributes.trimmingCharacters(in: .whitespacesAndNewlines)
            if !metadata.isEmpty {
                result.append(NSAttributedString(string: metadata + "\n", attributes: [
                    .font: NSFont.systemFont(ofSize: 11), .foregroundColor: NSColor.secondaryLabelColor,
                ]))
            }
        }
        for part in section.parts {
            switch part {
            case .text(let text): result.append(RichTextRenderer.renderMarkdown(text, font: font))
            case .section(let child): result.append(render(child, font: font))
            }
        }
        if !section.name.isEmpty {
            let table = NSTextTable()
            table.numberOfColumns = 1
            let block = NSTextTableBlock(table: table, startingRow: 0, rowSpan: 1, startingColumn: 0, columnSpan: 1)
            block.backgroundColor = NSColor.controlAccentColor.withAlphaComponent(0.035)
            block.setWidth(10, type: .absoluteValueType, for: .padding)
            block.setWidth(6, type: .absoluteValueType, for: .margin)
            block.setWidth(2, type: .absoluteValueType, for: .border, edge: .minX)
            block.setBorderColor(NSColor.controlAccentColor.withAlphaComponent(0.25), for: .minX)
            let full = NSRange(location: 0, length: result.length)
            result.enumerateAttribute(.paragraphStyle, in: full) { value, range, _ in
                let style = (value as? NSParagraphStyle)?.mutableCopy() as? NSMutableParagraphStyle ?? NSMutableParagraphStyle()
                style.textBlocks.insert(block, at: 0)
                result.addAttribute(.paragraphStyle, value: style, range: range)
            }
        }
        return result
    }
}
