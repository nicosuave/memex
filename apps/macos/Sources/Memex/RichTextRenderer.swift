import AppKit
import Markdown

/// Render Markdown and prompt sections as selectable native text.
@MainActor
enum RichTextRenderer {
    static let sourceLocationAttribute = NSAttributedString.Key("MemexSourceLocation")
    static func render(_ text: String, font: NSFont) -> NSAttributedString {
        PromptSections.render(text, font: font) ?? renderMarkdown(text, font: font)
    }

    static func renderMarkdown(_ text: String, font: NSFont) -> NSAttributedString {
        let output = NSMutableAttributedString(string: "")
        let document = Document(parsing: text)
        for child in document.children { appendBlock(child, to: output, font: font, indent: 0) }
        return output
    }

    private static func paragraph(indent: CGFloat, spacing: CGFloat = 8) -> NSMutableParagraphStyle {
        let style = NSMutableParagraphStyle()
        style.paragraphSpacing = spacing
        style.lineSpacing = 3
        style.firstLineHeadIndent = indent
        style.headIndent = indent
        return style
    }

    private static func appendBlock(_ node: any Markup, to output: NSMutableAttributedString,
                                    font: NSFont, indent: CGFloat) {
        if let table = node as? Markdown.Table {
            appendTable(table, to: output, font: font, indent: indent)
        } else if let list = node as? OrderedList {
            appendList(list, orderedStart: Int(list.startIndex), to: output, font: font, indent: indent)
        } else if let list = node as? UnorderedList {
            appendList(list, orderedStart: nil, to: output, font: font, indent: indent)
        } else if node is BlockQuote {
            let start = output.length
            for child in node.children { appendBlock(child, to: output, font: font, indent: indent + 20) }
            output.addAttribute(.foregroundColor, value: NSColor.secondaryLabelColor,
                                range: NSRange(location: start, length: output.length - start))
        } else if let code = node as? CodeBlock {
            let value = CodeSyntax.render(code.code, language: code.language ?? "text", font: font)
            appendParagraph(value, to: output, style: paragraph(indent: indent + 10, spacing: 12))
        } else if let html = node as? HTMLBlock {
            appendParagraph(codeText(html.rawHTML, font: font), to: output,
                            style: paragraph(indent: indent, spacing: 10))
        } else if let heading = node as? Heading {
            let headingFont = NSFont.systemFont(ofSize: font.pointSize + CGFloat(max(1, 4 - heading.level)) * 1.5, weight: .semibold)
            let value = inlineChildren(node, attributes: [.font: headingFont, .foregroundColor: NSColor.labelColor])
            let style = paragraph(indent: indent, spacing: 9)
            style.paragraphSpacingBefore = 8
            appendParagraph(value, to: output, style: style)
        } else if node is ThematicBreak {
            appendParagraph(NSAttributedString(string: "────────", attributes: [.foregroundColor: NSColor.separatorColor, .font: font]),
                            to: output, style: paragraph(indent: indent))
        } else if node is Paragraph {
            appendParagraph(inlineChildren(node, attributes: [.font: font, .foregroundColor: NSColor.labelColor]),
                            to: output, style: paragraph(indent: indent))
        } else {
            for child in node.children { appendBlock(child, to: output, font: font, indent: indent) }
        }
    }

    private static func appendTable(_ node: Markdown.Table, to output: NSMutableAttributedString,
                                    font: NSFont, indent: CGFloat) {
        let table = NSTextTable()
        table.numberOfColumns = max(1, node.maxColumnCount)
        table.collapsesBorders = true
        let rows: [any Markup] = [node.head] + node.body.children.map { $0 }
        for (rowIndex, row) in rows.enumerated() {
            for (columnIndex, cell) in row.children.enumerated() {
                let block = NSTextTableBlock(table: table, startingRow: rowIndex, rowSpan: 1,
                                             startingColumn: columnIndex, columnSpan: 1)
                block.setWidth(6, type: .absoluteValueType, for: .padding)
                block.setWidth(0.5, type: .absoluteValueType, for: .border)
                block.setBorderColor(.separatorColor)
                if rowIndex == 0 { block.backgroundColor = NSColor.labelColor.withAlphaComponent(0.035) }
                let style = paragraph(indent: indent, spacing: 0)
                style.textBlocks = [block]
                if columnIndex < node.columnAlignments.count {
                    switch node.columnAlignments[columnIndex] {
                    case .center: style.alignment = .center
                    case .right: style.alignment = .right
                    default: break
                    }
                }
                let cellFont = rowIndex == 0 ? NSFontManager.shared.convert(font, toHaveTrait: .boldFontMask) : font
                appendParagraph(inlineChildren(cell, attributes: [.font: cellFont, .foregroundColor: NSColor.labelColor]),
                                to: output, style: style)
            }
        }
    }

    private static func appendList(_ node: any Markup, orderedStart: Int?, to output: NSMutableAttributedString,
                                   font: NSFont, indent: CGFloat) {
        for (index, item) in node.children.enumerated() {
            var first = true
            for child in item.children {
                if child is Paragraph {
                    let value = NSMutableAttributedString(string: first ? (orderedStart.map { "\($0 + index).\t" } ?? "•\t") : "",
                                                          attributes: [.font: font, .foregroundColor: NSColor.labelColor])
                    value.append(inlineChildren(child, attributes: [.font: font, .foregroundColor: NSColor.labelColor]))
                    let style = paragraph(indent: indent + 20, spacing: 5)
                    style.firstLineHeadIndent = first ? indent : indent + 20
                    style.tabStops = [NSTextTab(textAlignment: .left, location: indent + 20, options: [:])]
                    appendParagraph(value, to: output, style: style)
                    first = false
                } else {
                    appendBlock(child, to: output, font: font, indent: indent + 20)
                }
            }
        }
    }

    private static func appendParagraph(_ value: NSAttributedString, to output: NSMutableAttributedString,
                                        style: NSParagraphStyle) {
        let start = output.length
        output.append(value)
        if !value.string.hasSuffix("\n") {
            output.append(NSAttributedString(string: "\n", attributes: value.length > 0
                ? value.attributes(at: value.length - 1, effectiveRange: nil) : [.font: NSFont.systemFont(ofSize: 14)]))
        }
        output.addAttribute(.paragraphStyle, value: style, range: NSRange(location: start, length: output.length - start))
    }

    private static func inlineChildren(_ node: any Markup, attributes: [NSAttributedString.Key: Any]) -> NSAttributedString {
        let output = NSMutableAttributedString(string: "")
        for child in node.children { output.append(inline(child, attributes: attributes)) }
        return output
    }

    private static func inline(_ node: any Markup, attributes: [NSAttributedString.Key: Any]) -> NSAttributedString {
        var attributes = attributes
        if let text = node as? Markdown.Text { return NSAttributedString(string: text.string, attributes: attributes) }
        if node is SoftBreak { return NSAttributedString(string: " ", attributes: attributes) }
        if node is LineBreak { return NSAttributedString(string: "\n", attributes: attributes) }
        let font = attributes[.font] as? NSFont ?? .systemFont(ofSize: 14)
        if node is Strong { attributes[.font] = NSFontManager.shared.convert(font, toHaveTrait: .boldFontMask) }
        if node is Emphasis { attributes[.font] = NSFontManager.shared.convert(font, toHaveTrait: .italicFontMask) }
        if node is Strikethrough { attributes[.strikethroughStyle] = NSUnderlineStyle.single.rawValue }
        if let code = node as? InlineCode {
            attributes[.font] = NSFont.monospacedSystemFont(ofSize: font.pointSize, weight: .regular)
            attributes[.backgroundColor] = NSColor.quaternaryLabelColor.withAlphaComponent(0.12)
            return NSAttributedString(string: code.code, attributes: attributes)
        }
        if let html = node as? InlineHTML { return codeText(html.rawHTML, font: font) }
        if let link = node as? Markdown.Link, let destination = link.destination,
           let url = URL(string: destination), ["https", "http", "mailto"].contains(url.scheme?.lowercased() ?? "") {
            attributes[.link] = url
            attributes[.foregroundColor] = NSColor.linkColor
            attributes[.underlineStyle] = NSUnderlineStyle.single.rawValue
        }
        if let link = node as? Markdown.Link, let destination = link.destination,
           ContentLocation.parse(destination)?.url.isFileURL == true {
            attributes[sourceLocationAttribute] = destination
        }
        if let image = node as? Markdown.Image {
            let value = NSMutableAttributedString(string: "[Image: ", attributes: attributes)
            value.append(inlineChildren(image, attributes: attributes))
            value.append(NSAttributedString(string: image.source.map { " — \($0)]" } ?? "]", attributes: attributes))
            return value
        }
        return inlineChildren(node, attributes: attributes)
    }

    private static func codeText(_ text: String, font: NSFont) -> NSAttributedString {
        let result = NSMutableAttributedString(string: text, attributes: [
            .font: NSFont.monospacedSystemFont(ofSize: font.pointSize, weight: .regular),
            .foregroundColor: NSColor.labelColor,
            .backgroundColor: NSColor.quaternaryLabelColor.withAlphaComponent(0.10),
        ])
        return result
    }
}
