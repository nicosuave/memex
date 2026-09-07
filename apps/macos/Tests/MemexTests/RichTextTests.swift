import AppKit
import Testing
@testable import Memex

@MainActor @Test func markdownStylesHeadingsEmphasisAndLinks() throws {
    let text = RichTextRenderer.render("# Heading\n\n**Bold** and *italic* with [site](https://example.com).", font: .systemFont(ofSize: 14))
    let string = text.string as NSString
    let heading = try #require(text.attribute(.font, at: string.range(of: "Heading").location, effectiveRange: nil) as? NSFont)
    let bold = try #require(text.attribute(.font, at: string.range(of: "Bold").location, effectiveRange: nil) as? NSFont)
    let italic = try #require(text.attribute(.font, at: string.range(of: "italic").location, effectiveRange: nil) as? NSFont)
    #expect(heading.pointSize > 14)
    #expect(NSFontManager.shared.traits(of: bold).contains(.boldFontMask))
    #expect(NSFontManager.shared.traits(of: italic).contains(.italicFontMask))
    #expect((text.attribute(.link, at: string.range(of: "site").location, effectiveRange: nil) as? URL)?.absoluteString == "https://example.com")
}

@MainActor @Test func markdownPreservesNestedListAndQuoteStructure() throws {
    let text = RichTextRenderer.render("3. First\n   - Nested\n4. Second\n\n> Quoted", font: .systemFont(ofSize: 14))
    let string = text.string as NSString
    #expect(text.string.contains("3.\tFirst"))
    #expect(text.string.contains("4.\tSecond"))
    #expect(text.string.contains("•\tNested"))
    let first = try #require(text.attribute(.paragraphStyle, at: string.range(of: "First").location, effectiveRange: nil) as? NSParagraphStyle)
    let nested = try #require(text.attribute(.paragraphStyle, at: string.range(of: "Nested").location, effectiveRange: nil) as? NSParagraphStyle)
    let quote = try #require(text.attribute(.paragraphStyle, at: string.range(of: "Quoted").location, effectiveRange: nil) as? NSParagraphStyle)
    #expect(nested.headIndent > first.headIndent)
    #expect(quote.headIndent > 0)
}

@MainActor @Test func xmlStaysLiteralWithoutLoadingContent() throws {
    for source in ["```xml\n<item name=\"hello\">world</item>\n```"] {
        let text = RichTextRenderer.render(source, font: .systemFont(ofSize: 14))
        #expect(text.string.contains("name=\"hello\""))
        #expect(text.string.contains("world</"))
        #expect(text.attribute(.attachment, at: 0, effectiveRange: nil) == nil)
    }
}

@MainActor @Test func fencedCodeKeepsLiteralMarkdownAndUnsafeLinksAreInactive() {
    let text = RichTextRenderer.render("```swift\nlet value = \"**literal**\"\n```\n\n[click](javascript:alert)", font: .systemFont(ofSize: 14))
    #expect(text.string.contains("**literal**"))
    let index = (text.string as NSString).range(of: "click").location
    #expect(text.attribute(.link, at: index, effectiveRange: nil) == nil)
}

@MainActor @Test func markdownTableUsesNativeTextBlocks() throws {
    let text = RichTextRenderer.render("| Name | Value |\n| --- | ---: |\n| Test | 42 |", font: .systemFont(ofSize: 14))
    #expect(text.string.contains("Name"))
    #expect(text.string.contains("42"))
    let index = (text.string as NSString).range(of: "42").location
    let style = try #require(text.attribute(.paragraphStyle, at: index, effectiveRange: nil) as? NSParagraphStyle)
    #expect(style.textBlocks.first is NSTextTableBlock)
    #expect(style.alignment == .right)
}

@MainActor @Test func promptWrappersBecomeNestedSectionsWithMarkdown() throws {
    let source = "<system_prompt>\nFollow **these rules**.\n<context>\n- First item\n- Second item\n</context>\n</system_prompt>"
    let text = RichTextRenderer.render(source, font: .systemFont(ofSize: 14))
    #expect(text.string.contains("System Prompt"))
    #expect(text.string.contains("Context"))
    #expect(!text.string.contains("<context>"))
    #expect(text.string.contains("•\tFirst item"))
    let boldIndex = (text.string as NSString).range(of: "these rules").location
    let font = try #require(text.attribute(.font, at: boldIndex, effectiveRange: nil) as? NSFont)
    #expect(NSFontManager.shared.traits(of: font).contains(.boldFontMask))
    let nestedIndex = (text.string as NSString).range(of: "First item").location
    let style = try #require(text.attribute(.paragraphStyle, at: nestedIndex, effectiveRange: nil) as? NSParagraphStyle)
    #expect(style.textBlocks.count == 2)
}

@MainActor @Test func partialPromptSectionsKeepContentAndFencesStayLiteral() {
    let text = RichTextRenderer.render("<instructions>\nKeep this partial preview.\n```xml\n<example>literal</example>\n```", font: .systemFont(ofSize: 14))
    #expect(text.string.contains("Instructions"))
    #expect(text.string.contains("Keep this partial preview."))
    #expect(text.string.contains("<example>literal</example>"))
}

@MainActor @Test func promptSectionTitleUsesOnlyTheCardInset() throws {
    let text = RichTextRenderer.render("<context_window_reminder>\nBody text.\n</context_window_reminder>", font: .systemFont(ofSize: 14))
    let storage = NSTextStorage(attributedString: text)
    let layout = NSLayoutManager()
    let container = NSTextContainer(size: NSSize(width: 600, height: 1000))
    container.lineFragmentPadding = 0
    layout.addTextContainer(container)
    storage.addLayoutManager(layout)
    layout.ensureLayout(for: container)
    let title = layout.lineFragmentRect(forGlyphAt: 0, effectiveRange: nil)
    #expect(title.minY <= 16) // Six-point outer margin plus ten-point card padding.
    #expect(text.string.contains("Context Window Reminder\nBody text."))
}
