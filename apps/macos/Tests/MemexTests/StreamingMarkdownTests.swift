import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct StreamingMarkdownTests {
    static let font = NSFont.systemFont(ofSize: 14)

    static let fixtures: [String] = [
        """
        # Title

        Some **bold** text with *emphasis*, `code`, and a [link](https://example.com).

        ## Section two
        Text right after a heading, then a list:
        - first item
        - second item with **bold**
          - nested item
          - nested two
        - third

        1. one
        2. two
        3. three

        """,
        """
        | Name | Value |
        |:-----|------:|
        | a | 1 |
        | **b** | `2` |

        After the table.
        | not | a table |

        """,
        """
        Intro paragraph.

        ```swift
        let x = 1

        print(x)
        ```

        ```
        no language
        ```
        ~~~
        tilde fence
        ~~~
        Closing words.

        """,
        """
        Text before HTML.
         <!-- an HTML comment -->

        > quoted **text**
        > continues
        lazy line

        Inline <span>html</span> and &amp; entity, 2 < 3 > 1.

        ---

        Final *line*.

        """,
        """
        ## 标题 🚀

        这是一个段落，包含**粗体**和*斜体*。👨‍👩‍👧 family emoji.

        - 项目一
        - 项目二 ✅

        """,
        """
        Setext heading
        ===

        Paragraph with
        soft break and lazy
        continuation.
        Second setext
        ---

        * star list
        + plus list

        2. starts at two
        3. three

        """,
        """
        ~~struck~~ text and ![alt](https://example.com/a.png) image.

        [file](/tmp/file.swift:12)

        Done with snake_case and 2*3*4.

        """,
        """
        See [docs][1] and [more][ref].

        Body text.

        [1]: https://example.com
        [ref]: https://example.org

        """,
    ]

    @Test func incrementalRenderingEqualsFullRenderingAtEverySplitPoint() {
        for (index, fixture) in Self.fixtures.enumerated() {
            let renderer = StreamingMarkdownRenderer(font: Self.font, heals: false)
            let characters = Array(fixture)
            // One character at a time for the first fixture, every few elsewhere.
            let step = index == 0 ? 1 : 3
            var ends = Array(stride(from: 1, through: characters.count, by: step))
            ends += characters.indices.filter { characters[$0] == "\n" }.map { $0 + 1 }
            for end in Set(ends + [characters.count]).sorted() {
                let prefix = String(characters[..<end])
                let output = renderer.render(prefix)
                let full = RichTextRenderer.renderMarkdown(prefix, font: Self.font)
                #expect(equivalent(output.text, full), "fixture \(index) at \(end): \(prefix.debugDescription)")
                if !equivalent(output.text, full) { break }
            }
        }
    }

    @Test func healedStreamingKeepsSettledBlocksAndEndsOnTheFullRender() {
        for fixture in Self.fixtures {
            let renderer = StreamingMarkdownRenderer(font: Self.font)
            var settled = NSAttributedString()
            var generation = renderer.generation
            for end in 1...fixture.count {
                let output = renderer.render(String(fixture.prefix(end)))
                if output.generation == generation {
                    // Healing never rewrites what settled earlier.
                    #expect(output.text.attributedSubstring(from: NSRange(location: 0, length: settled.length)).string == settled.string)
                }
                generation = output.generation
                settled = output.text.attributedSubstring(from: NSRange(location: 0, length: output.settledLength))
            }
            // Complete source: nothing to heal, so the streamed render is the full one.
            #expect(equivalent(renderer.render(fixture).text, RichTextRenderer.renderMarkdown(fixture, font: Self.font)))
        }
    }

    @Test func settledBlocksAreNotRenderedAgain() {
        let paragraphs = (0..<40).map { "Paragraph \($0) with **bold** text and `code`." }
        let source = paragraphs.joined(separator: "\n\n") + "\n\n"
        let renderer = StreamingMarkdownRenderer(font: Self.font)
        var updates = 0
        for end in stride(from: 1, through: source.count, by: 5) {
            _ = renderer.render(String(source.prefix(end)))
            updates += 1
        }
        _ = renderer.render(source)
        updates += 1
        // Each update renders at most the open tail (two blocks) plus blocks
        // that settle; every block settles exactly once.
        #expect(renderer.renderedBlockCount <= paragraphs.count + 2 * updates)
        #expect(renderer.settledBlockCount >= paragraphs.count - 1)
        let before = renderer.renderedBlockCount
        _ = renderer.render(source + "One more")
        #expect(renderer.renderedBlockCount - before <= 2)
        let settledBefore = renderer.settledBlockCount
        _ = renderer.render(source + "One more line")
        #expect(renderer.settledBlockCount == settledBefore)
    }

    @Test func settledCodeCardsKeepTheirLayoutObjects() throws {
        let renderer = StreamingMarkdownRenderer(font: Self.font)
        let context = RichContentContext()
        // The fence settles once the first line after it is complete.
        let first = "Intro.\n\n```swift\nlet a = 1\n```\n\nAfter the code.\nMore"
        let output = renderer.render(first)
        #expect(output.isRich)
        let items = renderer.richContent(context: context).items
        _ = renderer.render(first + " keeps growing")
        let later = renderer.richContent(context: context)
        guard case .code(let code) = items[1], case .code(let laterCode) = later.items[1] else {
            Issue.record("Expected a code card")
            return
        }
        #expect(code === laterCode)
        #expect(later.blocks == RichContentDocument(first + " keeps growing").blocks)
        // A materialized view keeps the settled item views when the layout is replaced.
        let layout = RichContentLayout(items: items, context: context)
        let view = layout.view()
        let codeView = try #require(view.subviews.compactMap { $0 as? CodeContentView }.first)
        let next = RichContentLayout(items: later.items, context: context)
        next.adoptMaterializedView(from: layout)
        #expect(next.view() === view)
        #expect(view.subviews.compactMap { $0 as? CodeContentView }.first === codeView)
    }

    @Test func promptSectionsAndCarriageReturnsUseTheOrdinaryRenderer() {
        let ordinary = ["<context>\nvalue\n</context>", "Intro\n\n<environment_context>\n  <cwd>/tmp</cwd>", "one\r\ntwo", "<https://example.com>"]
        for text in ordinary {
            #expect(StreamingMarkdownRenderer(font: Self.font).render(text).requiresFullRender, "\(text.debugDescription)")
        }
        for text in ["a < b and 3 > 2", "Inline <kbd>Ctrl</kbd> keys", "```html\n<div>code</div>\n```", "<3 hearts"] {
            #expect(!StreamingMarkdownRenderer(font: Self.font).render(text).requiresFullRender, "\(text.debugDescription)")
            #expect(PromptSections.render(text, font: Self.font) == nil)
        }
    }

    // MARK: Healing

    @Test(arguments: [
        ("**bold", "**bold**"),
        ("Some *it", "Some *it*"),
        ("_it", "_it_"),
        ("`code", "`code`"),
        ("~~del", "~~del~~"),
        ("**bold*", "**bold**"),
        ("``code with ` inside", "``code with ` inside``"),
        ("**bold `code", "**bold `code`**"),
        ("**bold text ", "**bold text**"),
        ("Hello **", "Hello "),
        ("Hello `", "Hello "),
        ("[text](http://par", "text"),
        ("See [docs](https://exa", "See docs"),
        ("**See [docs](https://exa", "**See docs**"),
        ("![alt](http://img", ""),
        ("Look ![al", "Look "),
        ("| a | **b", "| a | **b**"),
        ("- item **bo", "- item **bo**"),
        ("**🚀 Launch", "**🚀 Launch**"),
        ("**粗体", "**粗体**"),
    ])
    func healingClosesTheUnfinishedTail(_ input: String, _ expected: String) {
        #expect(StreamingMarkdownHealer.heal(input) == expected)
    }

    @Test(arguments: [
        "snake_case_name and foo_bar",
        "2*x + 3*y",
        "Use *.swift and **/*.ts globs",
        "a * b * c",
        "the ` key",
        "a ~ b",
        "x_",
        "Price is 5*",
        "**bold** and `code` and [link](https://example.com)",
        "| **a | b",
        "    indented **code",
        "```swift\nlet a = `x **y",
        "~~~\nstill *code",
        "line **one\n",
        "<div class=\"a **b",
        "https://example.com/a_b_c",
    ])
    func healingLeavesLiteralCharactersAlone(_ input: String) {
        #expect(StreamingMarkdownHealer.heal(input) == input)
    }

    @Test(arguments: [
        ("Hello\n-", "Hello\n"),
        ("Text\n#", "Text\n"),
        ("1.", ""),
        ("Item\n> ", "Item\n"),
        ("```sw", ""),
        ("Intro\n~~~", "Intro\n"),
        ("```swift\nlet a = 1\n``", "```swift\nlet a = 1\n"),
        ("Para\n\n---", "Para\n\n"),
    ])
    func unfinishedBlockMarkersAreWithheld(_ input: String, _ expected: String) {
        #expect(StreamingMarkdownHealer.holdBackIncompleteLine(input) == expected)
    }

    @Test(arguments: ["- item", "~~strike", "`code` and", "# Heading", "```swift\nlet a = 1\n```"])
    func meaningfulLinesAreNotWithheld(_ input: String) {
        #expect(StreamingMarkdownHealer.holdBackIncompleteLine(input) == input)
    }

    @Test func healingAppliesOnlyToTheLastBlock() {
        let renderer = StreamingMarkdownRenderer(font: Self.font)
        let output = renderer.render("A literal **marker.\n\nNow **bold")
        #expect(output.text.string.contains("A literal **marker."))
        #expect(output.text.string.hasSuffix("Now bold\n"))
        let full = renderer.render("A literal **marker.\n\nNow **bold** done.\n")
        #expect(equivalent(full.text, RichTextRenderer.renderMarkdown("A literal **marker.\n\nNow **bold** done.\n", font: Self.font)))
    }
}

/// Compares rendered Markdown, treating text tables structurally: every render
/// creates new NSTextTable objects, which compare by identity.
@MainActor func equivalent(_ lhs: NSAttributedString, _ rhs: NSAttributedString) -> Bool {
    guard lhs.string == rhs.string else { return false }
    var location = 0
    while location < lhs.length {
        var left = NSRange(), right = NSRange()
        let a = lhs.attributes(at: location, effectiveRange: &left)
        let b = rhs.attributes(at: location, effectiveRange: &right)
        guard Set(a.keys) == Set(b.keys) else { return false }
        for (key, value) in a {
            if let style = value as? NSParagraphStyle, let other = b[key] as? NSParagraphStyle {
                guard normalized(style) == normalized(other) else { return false }
            } else if !(value as AnyObject).isEqual(b[key]) {
                return false
            }
        }
        location = min(NSMaxRange(left), NSMaxRange(right))
    }
    return true
}

@MainActor private func normalized(_ style: NSParagraphStyle) -> String {
    let copy = style.mutableCopy() as! NSMutableParagraphStyle
    let blocks = copy.textBlocks.map { block -> String in
        guard let cell = block as? NSTextTableBlock else { return "block" }
        return "\(cell.startingRow),\(cell.rowSpan),\(cell.startingColumn),\(cell.columnSpan),\(cell.table.numberOfColumns)"
    }
    copy.textBlocks = []
    return "\(copy.description)|\(blocks)"
}
