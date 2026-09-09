import AppKit
import ImageIO
import Markdown

/// Local paths are meaningful only when the session's exact host is this host.
struct RichContentContext: Equatable {
    var isLocalHost = false
}

struct ContentLocation: Equatable {
    let url: URL
    let line: Int?

    static func parse(_ source: String) -> ContentLocation? {
        if source.hasPrefix("/") {
            var path = source
            var line: Int?
            if let colon = path.lastIndex(of: ":"), let number = Int(path[path.index(after: colon)...]), number > 0 {
                line = number
                path = String(path[..<colon])
            }
            return ContentLocation(url: URL(fileURLWithPath: path), line: line)
        }
        guard let url = URL(string: source), let scheme = url.scheme?.lowercased(),
              ["https", "http", "mailto", "file"].contains(scheme) else { return nil }
        if scheme == "file", let host = url.host, !host.isEmpty && host != "localhost" { return nil }
        if scheme == "file" {
            var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
            let fragment = components?.fragment ?? ""
            let line = Int(fragment.hasPrefix("L") ? String(fragment.dropFirst()) : fragment)
            components?.fragment = nil
            return ContentLocation(url: components?.url ?? url, line: line.flatMap { $0 > 0 ? $0 : nil })
        }
        return ContentLocation(url: url, line: nil)
    }

    func canOpen(in context: RichContentContext) -> Bool { !url.isFileURL || context.isLocalHost }

    @MainActor @discardableResult func open(in context: RichContentContext) -> Bool {
        guard canOpen(in: context) else { return false }
        if url.isFileURL, let line { return SourceFilePreview.show(url: url, line: line) }
        return NSWorkspace.shared.open(url)
    }
}

enum RichContentBlock: Equatable {
    case attributed(NSAttributedString)
    case markdown(String)
    case code(String, language: String)
    case attachment(label: String, source: String, image: Bool)
    case attachmentNotice(label: String, detail: String)
    case embeddedImage(label: String, data: Data, mimeType: String)
}

struct RichContentDocument {
    let blocks: [RichContentBlock]
    var hasRichBlocks: Bool { blocks.contains { if case .markdown = $0 { false } else { true } } }

    init(_ source: String) {
        var result: [RichContentBlock] = []
        for node in Document(parsing: source).children {
            if let code = node as? CodeBlock {
                result.append(.code(code.code, language: code.language ?? "text"))
            } else if let paragraph = node as? Paragraph,
                      paragraph.children.contains(where: { $0 is Markdown.Image }) {
                var prose = ""
                for child in paragraph.children {
                    if let image = child as? Markdown.Image, let source = image.source {
                        if !prose.isEmpty { result.append(.markdown(prose)); prose = "" }
                        result.append(.attachment(label: image.plainText, source: source, image: true))
                    } else { prose += child.detachedFromParent.format() }
                }
                if !prose.isEmpty { result.append(.markdown(prose)) }
            } else if let paragraph = node as? Paragraph, paragraph.childCount == 1,
                      let link = paragraph.child(at: 0) as? Markdown.Link,
                      let source = link.destination, ContentLocation.parse(source)?.url.isFileURL == true {
                result.append(.attachment(label: link.plainText, source: source, image: false))
            } else {
                result.append(.markdown(node.detachedFromParent.format()))
            }
        }
        blocks = result
    }
}

/// Measured with the same native layout used for display; no estimated row heights.
@MainActor final class RichContentView: NSView, NSTextViewDelegate {
    var onOpenLocation: ((ContentLocation) -> Void)?
    private var context = RichContentContext()
    private var items: [NSView] = []
    override var isFlipped: Bool { true }

    func configure(text: String, font: NSFont, context: RichContentContext) {
        configure(blocks: RichContentDocument(text).blocks, font: font, context: context)
    }

    func configure(blocks: [RichContentBlock], font: NSFont, context: RichContentContext) {
        self.context = context
        items.forEach { $0.removeFromSuperview() }
        items = blocks.map { block in
            switch block {
            case .markdown, .attributed:
                let view = Self.textView()
                let rendered: NSAttributedString
                if case .markdown(let source) = block { rendered = RichTextRenderer.renderMarkdown(source, font: font) }
                else if case .attributed(let content) = block { rendered = content }
                else { rendered = NSAttributedString(string: "") }
                let content = NSMutableAttributedString(attributedString: rendered)
                content.enumerateAttribute(RichTextRenderer.sourceLocationAttribute, in: NSRange(location: 0, length: content.length)) { value, range, _ in
                    guard let source = value as? String, let location = ContentLocation.parse(source), location.canOpen(in: context) else { return }
                    content.addAttribute(.link, value: source, range: range)
                    content.addAttribute(.foregroundColor, value: NSColor.linkColor, range: range)
                }
                view.textStorage?.setAttributedString(content)
                view.delegate = self
                return view
            case .code(let source, let language): return CodeContentView(code: source, language: language, font: font)
            case .embeddedImage(let label, let data, let mimeType):
                return AttachmentContentView(label: label, source: "Embedded image · \(mimeType)", isImage: true, context: context, embeddedData: data)
            case .attachmentNotice(let label, let detail):
                return AttachmentContentView(label: label, source: "", isImage: false, context: context, unavailableDetail: detail)
            case .attachment(let label, let source, let image):
                let card = AttachmentContentView(label: label, source: source, isImage: image, context: context)
                card.onOpenLocation = { [weak self] location in self?.open(location) }
                return card
            }
        }
        items.forEach { addSubview($0) }
        needsLayout = true
    }

    func textView(_ textView: NSTextView, clickedOnLink link: Any, at charIndex: Int) -> Bool {
        let source = (link as? String) ?? (link as? URL)?.absoluteString
        guard let source, let location = ContentLocation.parse(source) else { return true }
        open(location)
        return true
    }

    private func open(_ location: ContentLocation) {
        guard location.canOpen(in: context) else { return }
        if let onOpenLocation { onOpenLocation(location) }
        else { location.open(in: context) }
    }

    @discardableResult func height(for width: CGFloat) -> CGFloat {
        var y: CGFloat = 0
        for item in items {
            let height: CGFloat
            if let text = item as? NSTextView { height = Self.textHeight(text, width: width) }
            else if let code = item as? CodeContentView { height = code.height(for: width) }
            else if let attachment = item as? AttachmentContentView { height = attachment.contentHeight }
            else { height = 0 }
            item.frame = NSRect(x: 0, y: y, width: max(1, width), height: height)
            y += height + 8
        }
        return max(0, y - 8)
    }

    override func layout() { super.layout(); height(for: bounds.width) }

    static func textView() -> NSTextView {
        let view = NSTextView()
        view.isEditable = false
        view.isSelectable = true
        view.drawsBackground = false
        view.textContainerInset = .zero
        view.textContainer?.lineFragmentPadding = 0
        view.textContainer?.widthTracksTextView = false
        return view
    }

    static func textHeight(_ view: NSTextView, width: CGFloat) -> CGFloat {
        guard let container = view.textContainer, let manager = view.layoutManager else { return 0 }
        container.containerSize = NSSize(width: max(1, width), height: .greatestFiniteMagnitude)
        manager.ensureLayout(for: container)
        return ceil(manager.usedRect(for: container).height)
    }
}

/// A code card owns horizontal panning; vertical gestures belong to the transcript.
@MainActor final class CodeHorizontalScrollView: NSScrollView {
    override func scrollWheel(with event: NSEvent) {
        if abs(event.scrollingDeltaY) >= abs(event.scrollingDeltaX), event.scrollingDeltaY != 0 {
            var ancestor = superview
            while let view = ancestor {
                if let scroll = view as? NSScrollView {
                    scroll.scrollWheel(with: event)
                    return
                }
                ancestor = view.superview
            }
            return
        }
        super.scrollWheel(with: event)
    }
}

@MainActor private final class CodeCopyButton: NSButton {
    var isCardHovered = false { didSet { refreshVisibility() } }
    override var acceptsFirstResponder: Bool { true }
    override func becomeFirstResponder() -> Bool {
        let accepted = super.becomeFirstResponder()
        if accepted { alphaValue = 1 }
        return accepted
    }
    override func resignFirstResponder() -> Bool {
        let accepted = super.resignFirstResponder()
        if accepted { alphaValue = isCardHovered ? 1 : 0 }
        return accepted
    }
    func refreshVisibility() { alphaValue = isCardHovered || window?.firstResponder === self ? 1 : 0 }
}

@MainActor final class CodeContentView: NSView {
    let code: String
    let language: String
    private let label: NSTextField
    private let copyButton = CodeCopyButton(title: "", target: nil, action: nil)
    private var hoverTracking: NSTrackingArea?
    let scrollView = CodeHorizontalScrollView()
    let textView = RichContentView.textView()
    override var isFlipped: Bool { true }

    init(code: String, language: String, font: NSFont) {
        self.code = code
        self.language = language
        label = NSTextField(labelWithString: language)
        super.init(frame: .zero)
        wantsLayer = true
        layer?.cornerRadius = 8
        label.font = .systemFont(ofSize: 11, weight: .medium)
        label.textColor = .secondaryLabelColor
        copyButton.isBordered = false
        copyButton.image = NSImage(systemSymbolName: "doc.on.doc", accessibilityDescription: "Copy code")
        copyButton.imagePosition = .imageOnly
        copyButton.toolTip = "Copy code"
        copyButton.setAccessibilityLabel("Copy code")
        copyButton.alphaValue = 0
        copyButton.target = self
        copyButton.action = #selector(copyCode)
        // A fence terminator contributes one final newline. Do not draw its
        // empty line, but retain the exact original code for the Copy action.
        let displayCode = code.hasSuffix("\r\n") || code.hasSuffix("\n") ? String(code.dropLast()) : code
        textView.textStorage?.setAttributedString(CodeSyntax.render(displayCode, language: language, font: font))
        textView.isHorizontallyResizable = true
        textView.isVerticallyResizable = true
        textView.autoresizingMask = []
        textView.minSize = .zero
        textView.maxSize = NSSize(width: 1_000_000, height: 1_000_000)
        textView.textContainer?.widthTracksTextView = false
        textView.textContainer?.heightTracksTextView = false
        scrollView.drawsBackground = false
        scrollView.hasHorizontalScroller = true
        scrollView.hasVerticalScroller = false
        scrollView.verticalScrollElasticity = .none
        scrollView.autohidesScrollers = true
        scrollView.documentView = textView
        addSubview(label); addSubview(copyButton); addSubview(scrollView)
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    private func metrics() -> (height: CGFloat, width: CGFloat) {
        let height = RichContentView.textHeight(textView, width: 1_000_000)
        let usedWidth = textView.layoutManager.flatMap { manager in textView.textContainer.map { manager.usedRect(for: $0).width } } ?? 0
        return (height, ceil(usedWidth) + 4)
    }

    func height(for width: CGFloat) -> CGFloat {
        let size = metrics()
        let overflow = size.width > max(1, width - 24)
        // Only overflowing code needs a scrollbar. Reserve legacy thickness
        // there so either macOS scrollbar preference leaves every line visible.
        let scrollbar = overflow ? NSScroller.scrollerWidth(for: .regular, scrollerStyle: .legacy) : 0
        return size.height + 40 + scrollbar
    }

    override func updateTrackingAreas() {
        super.updateTrackingAreas()
        if let hoverTracking { removeTrackingArea(hoverTracking) }
        let tracking = NSTrackingArea(rect: .zero, options: [.mouseEnteredAndExited, .activeInKeyWindow, .inVisibleRect], owner: self)
        addTrackingArea(tracking)
        hoverTracking = tracking
    }
    override func mouseEntered(with event: NSEvent) { copyButton.isCardHovered = true }
    override func mouseExited(with event: NSEvent) { copyButton.isCardHovered = false }

    override func layout() {
        super.layout()
        effectiveAppearance.performAsCurrentDrawingAppearance {
            layer?.backgroundColor = NSColor.labelColor.withAlphaComponent(0.035).cgColor
        }
        label.frame = NSRect(x: 12, y: 8, width: max(0, bounds.width - 90), height: 18)
        copyButton.frame = NSRect(x: max(0, bounds.width - 36), y: 5, width: 24, height: 24)
        let size = metrics()
        scrollView.hasHorizontalScroller = size.width > max(1, bounds.width - 24)
        scrollView.frame = NSRect(x: 12, y: 32, width: max(1, bounds.width - 24), height: max(1, bounds.height - 40))
        textView.frame = NSRect(x: 0, y: 0, width: max(scrollView.contentSize.width, size.width), height: size.height)
    }
    @objc private func copyCode() { NSPasteboard.general.clearContents(); NSPasteboard.general.setString(code, forType: .string) }
}

@MainActor final class AttachmentContentView: NSView {
    var onOpenLocation: ((ContentLocation) -> Void)?
    private let title: NSTextField
    private let detail: NSTextField
    private let preview = NSImageView()
    private let openButton = NSButton(title: "Open", target: nil, action: nil)
    private let location: ContentLocation?
    private let context: RichContentContext
    private var imageWindow: NSWindow?
    private let embeddedData: Data?
    let contentHeight: CGFloat
    override var isFlipped: Bool { true }

    init(label: String, source: String, isImage: Bool, context: RichContentContext, embeddedData: Data? = nil, unavailableDetail: String? = nil) {
        let location = ContentLocation.parse(source)
        self.location = location
        self.context = context
        let embeddedData = embeddedData.flatMap { $0.count <= 20_000_000 ? $0 : nil }
        self.embeddedData = embeddedData
        title = NSTextField(labelWithString: label.isEmpty ? (location?.url.lastPathComponent ?? "Attachment") : label)
        detail = NSTextField(wrappingLabelWithString: unavailableDetail ?? (source + ((location?.url.isFileURL == true && !context.isLocalHost) ? " · Recorded on another host" : "")))
        // Never interpret another machine's absolute path on this machine, and
        // never fetch remote media as a side effect of reading a transcript.
        var thumbnail: NSImage?
        var imageSource: CGImageSource?
        if isImage, let embeddedData { imageSource = CGImageSourceCreateWithData(embeddedData as CFData, nil) }
        else if isImage, context.isLocalHost, let url = location?.url, url.isFileURL {
            imageSource = CGImageSourceCreateWithURL(url as CFURL, nil)
        }
        if let imageSource, let cgImage = CGImageSourceCreateThumbnailAtIndex(imageSource, 0, [
                kCGImageSourceCreateThumbnailFromImageAlways: true,
                kCGImageSourceThumbnailMaxPixelSize: 600,
                kCGImageSourceCreateThumbnailWithTransform: true,
           ] as CFDictionary) { thumbnail = NSImage(cgImage: cgImage, size: .zero) }
        contentHeight = thumbnail == nil ? 70 : 260
        super.init(frame: .zero)
        wantsLayer = true
        layer?.cornerRadius = 8
        title.font = .systemFont(ofSize: 13, weight: .medium)
        title.isSelectable = true
        detail.font = .systemFont(ofSize: 11)
        detail.textColor = .secondaryLabelColor
        detail.isSelectable = true
        preview.image = thumbnail
        preview.imageScaling = .scaleProportionallyUpOrDown
        openButton.title = thumbnail == nil ? "Open" : "Enlarge"
        openButton.isHidden = unavailableDetail != nil
        openButton.isEnabled = thumbnail != nil || location?.canOpen(in: context) == true
        openButton.target = self
        openButton.action = #selector(openAttachment)
        addSubview(title); addSubview(detail); addSubview(preview); addSubview(openButton)
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }
    override func layout() {
        super.layout()
        effectiveAppearance.performAsCurrentDrawingAppearance {
            layer?.backgroundColor = NSColor.labelColor.withAlphaComponent(0.035).cgColor
        }
        title.frame = NSRect(x: 12, y: 8, width: max(1, bounds.width - (openButton.isHidden ? 24 : 100)), height: 20)
        openButton.frame = NSRect(x: max(0, bounds.width - 80), y: 6, width: 70, height: 24)
        detail.frame = NSRect(x: 12, y: 31, width: max(1, bounds.width - 24), height: 32)
        preview.frame = NSRect(x: 12, y: 68, width: max(1, bounds.width - 24), height: max(0, bounds.height - 80))
    }
    @objc private func openAttachment() {
        if preview.image != nil {
            let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 900, height: 650), styleMask: [.titled, .closable, .resizable], backing: .buffered, defer: false)
            let view = NSImageView()
            if let embeddedData { view.image = NSImage(data: embeddedData) ?? preview.image }
            else if let location, location.canOpen(in: context) { view.image = NSImage(contentsOf: location.url) ?? preview.image }
            else { view.image = preview.image }
            view.imageScaling = .scaleProportionallyUpOrDown
            window.contentView = view
            window.title = title.stringValue
            window.isReleasedWhenClosed = false
            window.center(); window.makeKeyAndOrderFront(nil)
            imageWindow = window
        } else if let location, location.canOpen(in: context) {
            if let onOpenLocation { onOpenLocation(location) }
            else { location.open(in: context) }
        }
    }
}
