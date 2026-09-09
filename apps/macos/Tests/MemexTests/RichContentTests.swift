import AppKit
import Testing
@testable import Memex

@Test func richDocumentPreservesCodeAndSurroundingProse() {
    let source = "Before.\n\n```swift\nlet x = \"**literal**\"\n```\n\nAfter."
    let document = RichContentDocument(source)
    #expect(document.hasRichBlocks)
    #expect(document.blocks.count == 3)
    #expect(document.blocks[1] == .code("let x = \"**literal**\"\n", language: "swift"))
    if case .markdown(let text) = document.blocks[2] { #expect(text == "After.") }
    else { Issue.record("Missing trailing prose") }
}

@Test func attachmentsRetainSourceAndHostBoundary() throws {
    let document = RichContentDocument("Before ![Screenshot](/tmp/screenshot.png) after.\n\n[Source](/tmp/source.swift:42)")
    #expect(document.blocks.contains(.attachment(label: "Screenshot", source: "/tmp/screenshot.png", image: true)))
    #expect(document.blocks.contains(.attachment(label: "Source", source: "/tmp/source.swift:42", image: false)))
    let source = try #require(ContentLocation.parse("/tmp/source file.swift:42"))
    #expect(source.url.path == "/tmp/source file.swift")
    #expect(source.line == 42)
    #expect(!source.canOpen(in: RichContentContext()))
    #expect(source.canOpen(in: RichContentContext(isLocalHost: true)))
    #expect(ContentLocation.parse("javascript:alert(1)") == nil)
    #expect(ContentLocation.parse("file://another-host/tmp/image.png") == nil)
    #expect(ContentLocation.parse("relative/path.png") == nil)
}

@MainActor @Test func syntaxColorsTokensWithoutChangingCopyText() throws {
    let source = "let x = \"return 42\" // let y = 2\n"
    let rendered = CodeSyntax.render(source, language: "swift", font: .systemFont(ofSize: 14))
    #expect(rendered.string == source)
    let ns = source as NSString
    #expect(rendered.attribute(.foregroundColor, at: ns.range(of: "let").location, effectiveRange: nil) as? NSColor == .systemPurple)
    #expect(rendered.attribute(.foregroundColor, at: ns.range(of: "return").location, effectiveRange: nil) as? NSColor == .systemRed)
    #expect(rendered.attribute(.foregroundColor, at: ns.range(of: "let y").location, effectiveRange: nil) as? NSColor == .secondaryLabelColor)
    #expect(CodeSyntax.render(source, language: "unknown", font: .systemFont(ofSize: 14)).string == source)
}

@MainActor @Test func codeCardKeepsLongLinesHorizontallyScrollable() {
    let code = "let value = \"" + String(repeating: "long ", count: 100) + "\"\n"
    let card = CodeContentView(code: code, language: "swift", font: .systemFont(ofSize: 14))
    card.frame = NSRect(x: 0, y: 0, width: 300, height: card.height(for: 300))
    card.layoutSubtreeIfNeeded()
    #expect(card.scrollView.hasHorizontalScroller)
    #expect(card.textView.frame.width > 300)
    #expect(card.textView.string == String(code.dropLast()))
    #expect(card.height(for: 200) == card.height(for: 600))
}

@MainActor @Test func remoteImageDoesNotLoadLocalFileAndKeepsStableHeight() {
    let card = AttachmentContentView(label: "Image", source: "/System/Library/CoreServices/DefaultDesktop.heic", isImage: true, context: RichContentContext())
    #expect(card.contentHeight == 70)
    let view = RichContentView()
    view.configure(text: "Text\n\n```json\n{\"value\": 42}\n```\n\n![Image](https://example.com/image.png)", font: .systemFont(ofSize: 14), context: RichContentContext())
    let first = view.height(for: 400)
    #expect(first > 70)
    #expect(view.height(for: 400) == first)
}

@MainActor @Test func localLinksRetainLineMetadataWithoutOpeningByDefault() throws {
    let rendered = RichTextRenderer.renderMarkdown("See [file](/tmp/source.swift:24).", font: .systemFont(ofSize: 14))
    let index = (rendered.string as NSString).range(of: "file").location
    #expect(rendered.attribute(RichTextRenderer.sourceLocationAttribute, at: index, effectiveRange: nil) as? String == "/tmp/source.swift:24")
    #expect(rendered.attribute(.link, at: index, effectiveRange: nil) == nil)
}

@MainActor @Test func sourceLineNavigationHandlesUnicodeAndRejectsRemoteOpen() throws {
    let source = "First\nUnicode 🙂 here\nThird\n"
    let range = try #require(SourceFilePreview.range(ofLine: 2, in: source))
    #expect((source as NSString).substring(with: range) == "Unicode 🙂 here\n")
    #expect(SourceFilePreview.range(ofLine: 9, in: source) == nil)
    #expect(SourceFilePreview.range(ofLine: 2, in: "Only line") == nil)
    let location = try #require(ContentLocation.parse("file:///tmp/source.swift#L24"))
    #expect(location.line == 24)
    #expect(location.url.fragment == nil)
    #expect(!location.open(in: RichContentContext()))
}

@MainActor @Test func richContentSupportsAttributedFieldsAndEmbeddedImages() throws {
    let bitmap = try #require(NSBitmapImageRep(bitmapDataPlanes: nil, pixelsWide: 2, pixelsHigh: 2, bitsPerSample: 8, samplesPerPixel: 4, hasAlpha: true, isPlanar: false, colorSpaceName: .deviceRGB, bytesPerRow: 8, bitsPerPixel: 32))
    let data = try #require(bitmap.representation(using: .png, properties: [:]))
    let card = AttachmentContentView(label: "Result", source: "Embedded image", isImage: true, context: RichContentContext(), embeddedData: data)
    #expect(card.contentHeight == 260)
    let view = RichContentView()
    view.configure(blocks: [.attributed(NSAttributedString(string: "Retained metadata")), .embeddedImage(label: "Result", data: data, mimeType: "image/png")], font: .systemFont(ofSize: 14), context: RichContentContext())
    #expect(view.height(for: 400) > 260)
    #expect(view.subviews.compactMap { $0 as? NSTextView }.contains { $0.string == "Retained metadata" })
}

@Test func ordinaryRepliesStayPlainAndInlineFilesKeepProse() {
    let plain = RichContentDocument("Short reply.")
    #expect(!plain.hasRichBlocks)
    #expect(plain.blocks == [.markdown("Short reply.")])
    let inline = RichContentDocument("See [source](/tmp/source.swift:42) for details.")
    #expect(!inline.hasRichBlocks)
    #expect(inline.blocks.count == 1)
}

@MainActor private final class WheelRecordingScrollView: NSScrollView {
    var receivedEvents: [NSEvent] = []
    override func scrollWheel(with event: NSEvent) { receivedEvents.append(event) }
}

@MainActor @Test func codeWheelRoutesVerticalGesturesToTranscriptAndKeepsHorizontalPanning() async throws {
    _ = NSApplication.shared
    let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 400, height: 400), styleMask: [.titled], backing: .buffered, defer: false)
    window.isReleasedWhenClosed = false
    defer { window.close() }
    let outer = WheelRecordingScrollView(frame: NSRect(x: 0, y: 0, width: 400, height: 400))
    window.contentView = outer
    let document = NSView(frame: NSRect(x: 0, y: 0, width: 400, height: 1000))
    outer.documentView = document
    let code = CodeContentView(code: String(repeating: "long ", count: 100) + "\nsecond line\n", language: "text", font: .systemFont(ofSize: 14))
    code.frame = NSRect(x: 0, y: 0, width: 300, height: code.height(for: 300))
    document.addSubview(code)
    code.scrollView.scrollerStyle = .legacy
    window.orderFront(nil)
    window.contentView?.layoutSubtreeIfNeeded()
    code.layoutSubtreeIfNeeded()
    let verticalCG = try #require(CGEvent(scrollWheelEvent2Source: nil, units: .pixel, wheelCount: 2, wheel1: -100, wheel2: -4, wheel3: 0))
    let vertical = try #require(NSEvent(cgEvent: verticalCG))
    code.scrollView.scrollWheel(with: vertical)
    #expect(outer.receivedEvents.count == 1)
    #expect(outer.receivedEvents.first === vertical)
    #expect(code.scrollView.contentView.bounds.origin.y == 0)
    #expect(code.textView.frame.height <= code.scrollView.contentSize.height)
    let horizontalCG = try #require(CGEvent(scrollWheelEvent2Source: nil, units: .pixel, wheelCount: 2, wheel1: 0, wheel2: -80, wheel3: 0))
    let horizontal = try #require(NSEvent(cgEvent: horizontalCG))
    #expect(horizontal.scrollingDeltaX < 0)
    #expect(code.textView.frame.width > code.scrollView.contentSize.width)
    code.scrollView.scrollWheel(with: horizontal)
    // AppKit can animate wheel scrolling; exercise a window-backed scroll view
    // and allow the animation to advance before inspecting its final bounds.
    for _ in 0..<25 where code.scrollView.contentView.bounds.origin.x == 0 {
        try await Task.sleep(for: .milliseconds(20))
    }
    #expect(outer.receivedEvents.count == 1)
    #expect(code.scrollView.contentView.bounds.origin.x > 0)
    #expect(code.scrollView.contentView.bounds.origin.y == 0)
}

@MainActor @Test func shortCodeCardHasTightGeometryAndCopiesExactOriginal() throws {
    _ = NSApplication.shared
    let original = "const answer = {\n  value: 42\n}\n"
    let card = CodeContentView(code: original, language: "ts", font: .systemFont(ofSize: 14))
    card.scrollView.scrollerStyle = .legacy
    card.frame = NSRect(x: 0, y: 0, width: 400, height: card.height(for: 400))
    let window = NSWindow(contentRect: card.frame, styleMask: [.titled], backing: .buffered, defer: false)
    window.isReleasedWhenClosed = false
    window.contentView = card
    defer { window.close() }
    card.layoutSubtreeIfNeeded()
    #expect(card.textView.string == "const answer = {\n  value: 42\n}")
    #expect(!card.scrollView.hasHorizontalScroller)
    #expect(abs(card.scrollView.contentSize.height - card.textView.frame.height) <= 1)
    #expect(card.bounds.height - card.scrollView.frame.maxY == 8)
    let button = try #require(card.subviews.compactMap { $0 as? NSButton }.first)
    #expect(button.title.isEmpty)
    #expect(button.image != nil)
    #expect(button.alphaValue == 0)
    let buttonFrame = button.frame
    let hover = try #require(NSEvent.enterExitEvent(with: .mouseEntered, location: .zero, modifierFlags: [], timestamp: 0, windowNumber: window.windowNumber, context: nil, eventNumber: 0, trackingNumber: 0, userData: nil))
    card.mouseEntered(with: hover)
    #expect(button.alphaValue == 1)
    #expect(button.frame == buttonFrame)
    card.mouseExited(with: hover)
    #expect(button.alphaValue == 0)
    #expect(window.makeFirstResponder(button))
    #expect(button.alphaValue == 1)
    #expect(button.frame == buttonFrame)
    _ = window.makeFirstResponder(nil)
    #expect(button.alphaValue == 0)
    let pasteboard = NSPasteboard.general
    let previous = (pasteboard.pasteboardItems ?? []).map { item in
        item.types.compactMap { type in item.data(forType: type).map { (type, $0) } }
    }
    defer {
        pasteboard.clearContents()
        let items = previous.map { representations in
            let item = NSPasteboardItem()
            for (type, data) in representations { item.setData(data, forType: type) }
            return item
        }
        pasteboard.writeObjects(items)
    }
    let copyAction = try #require(button.action)
    #expect(NSApplication.shared.sendAction(copyAction, to: button.target, from: button))
    #expect(pasteboard.string(forType: .string) == original)
}

@MainActor @Test func codeDisplayRemovesOnlyOneTerminalNewline() {
    let card = CodeContentView(code: "value\n\n", language: "text", font: .systemFont(ofSize: 14))
    #expect(card.textView.string == "value\n")
    #expect(card.code == "value\n\n")
    let windows = CodeContentView(code: "value\r\n", language: "text", font: .systemFont(ofSize: 14))
    #expect(windows.textView.string == "value")
    #expect(windows.code == "value\r\n")
}
