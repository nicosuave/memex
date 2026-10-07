import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct LazyRichContentTests {
    @Test func measuringOffscreenRichRowsDoesNotMaterializeTheirViewTrees() throws {
        let reader = TranscriptController()
        reader.view.frame = NSRect(x: 0, y: 0, width: 1_000, height: 720)
        let records = (0..<200).map { index in
            TranscriptRecord(recordID: "rich-\(index)", record: Message(role: "assistant",
                text: "Message \(index).\n\n- First item\n- Second item\n\n```swift\nlet result = \(index)\n```",
                toolName: nil, toolInput: nil, toolOutput: nil))
        }
        reader.update(sessionID: "lazy", records: records, provider: "codex")
        let layouts = try reader.rows.indices.map { try #require(reader.measurement(at: $0).richLayout) }
        #expect(layouts.count == records.count)
        #expect(layouts.filter { $0.materializedView != nil }.count < 10)
        #expect(layouts[150].materializedView == nil)
        let height = reader.measurement(at: 150).height
        let measured = reader.measurement(at: 150).fullTextHeight
        let cell = try #require(reader.tableView(reader.table, viewFor: reader.table.tableColumns.first, row: 150))
        cell.frame = NSRect(x: 0, y: 0, width: 1_000, height: height)
        cell.layoutSubtreeIfNeeded()
        let view = try #require(layouts[150].materializedView)
        #expect(!view.subviews.compactMap { $0 as? CodeContentView }.isEmpty)
        #expect(view.height(for: reader.measurement(at: 150).contentWidth) == measured)
        #expect(reader.measurement(at: 150).height == height)
        #expect(layouts[149].materializedView == nil)
        #expect(layouts[151].materializedView == nil)
    }

    @Test func exactTextKitGeometrySurvivesMaterializationAndResize() throws {
        let bitmap = try #require(NSBitmapImageRep(bitmapDataPlanes: nil, pixelsWide: 2, pixelsHigh: 2,
            bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true, isPlanar: false,
            colorSpaceName: .deviceRGB, bytesPerRow: 8, bitsPerPixel: 32))
        let png = try #require(bitmap.representation(using: .png, properties: [:]))
        let blocks: [RichContentBlock] = [
            .markdown("A **formatted** paragraph with [local source](/tmp/source.swift:12).\n\n- First item\n- Second item"),
            .markdown("| Key | Value |\n| --- | --- |\n| Unicode 🙂 | A long table value that wraps in a narrow column |"),
            .code("let text = \"" + String(repeating: "wide ", count: 60) + "\"\nlet next = 42\n", language: "swift"),
            .embeddedImage(label: "Image", data: png, mimeType: "image/png"),
            .attachment(label: "Remote image", source: "https://example.com/image.png", image: true),
            .attachmentNotice(label: "Document", detail: "Provider attachment"),
        ]
        let plan = RichContentLayout(blocks: blocks, font: .systemFont(ofSize: 14), context: RichContentContext(isLocalHost: true))
        let widths: [CGFloat] = [280, 800]
        let expected = widths.map { plan.height(for: $0) }
        #expect(plan.materializedView == nil)
        let view = plan.view()
        for (index, width) in widths.enumerated() {
            view.frame = NSRect(x: 0, y: 0, width: width, height: expected[index])
            view.layoutSubtreeIfNeeded()
            #expect(view.height(for: width) == expected[index])
            #expect(view.subviews.last?.frame.maxY == expected[index])
            for text in view.subviews.compactMap({ $0 as? NSTextView }) {
                let manager = try #require(text.layoutManager)
                let container = try #require(text.textContainer)
                manager.ensureLayout(for: container)
                #expect(ceil(manager.usedRect(for: container).height) == text.frame.height)
            }
            let code = try #require(view.subviews.compactMap { $0 as? CodeContentView }.first)
            code.layoutSubtreeIfNeeded()
            #expect(code.scrollView.hasHorizontalScroller)
            #expect(code.textView.frame.height <= code.scrollView.contentSize.height)
        }
        #expect(view.subviews.compactMap { $0 as? AttachmentContentView }.map(\.contentHeight) == [260, 70, 70])
        let text = try #require(view.subviews.compactMap { $0 as? NSTextView }.first)
        let location = (text.string as NSString).range(of: "local source").location
        #expect(text.attributedString().attribute(.link, at: location, effectiveRange: nil) as? String == "/tmp/source.swift:12")
        var opened: ContentLocation?
        view.onOpenLocation = { opened = $0 }
        #expect(view.textView(text, clickedOnLink: "/tmp/source.swift:12", at: location))
        #expect(opened?.line == 12)
    }

    @Test func cachedLayoutDoesNotRetainDiscardedControlsAndCanRematerialize() throws {
        let plan = RichContentLayout(blocks: [.markdown("Prose"), .code("let value = 42\n", language: "swift")],
            font: .systemFont(ofSize: 14), context: RichContentContext())
        let height = plan.height(for: 400)
        weak var discarded: RichContentView?
        autoreleasepool {
            let view = plan.view()
            discarded = view
            #expect(plan.view() === view)
            #expect(view.height(for: 400) == height)
        }
        #expect(discarded == nil)
        #expect(plan.materializedView == nil)
        let recreated = plan.view()
        #expect(recreated.height(for: 400) == height)
        #expect(recreated.subviews.compactMap { $0 as? CodeContentView }.first?.code == "let value = 42\n")
        #expect(recreated.subviews.compactMap { $0 as? NSTextView }.first?.string == "Prose\n")
    }

    @Test func codeHorizontalPositionSurvivesControlRecycling() throws {
        let plan = RichContentLayout(blocks: [.code(String(repeating: "long ", count: 200), language: "text")],
            font: .systemFont(ofSize: 14), context: RichContentContext())
        autoreleasepool {
            let view = plan.view()
            view.frame = NSRect(x: 0, y: 0, width: 300, height: plan.height(for: 300))
            view.layoutSubtreeIfNeeded()
            let code = view.subviews.compactMap { $0 as? CodeContentView }[0]
            code.layoutSubtreeIfNeeded()
            code.scrollView.contentView.scroll(to: NSPoint(x: 100, y: 0))
            code.scrollView.reflectScrolledClipView(code.scrollView.contentView)
        }
        #expect(plan.materializedView == nil)
        let recreated = plan.view()
        recreated.frame = NSRect(x: 0, y: 0, width: 300, height: plan.height(for: 300))
        recreated.layoutSubtreeIfNeeded()
        let code = try #require(recreated.subviews.compactMap { $0 as? CodeContentView }.first)
        code.layoutSubtreeIfNeeded()
        #expect(code.scrollView.contentView.bounds.minX == 100)
    }
}
