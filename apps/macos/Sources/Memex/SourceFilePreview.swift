import AppKit

/// Explicit local-file navigation, with a visible and selectable target line.
/// Text is never interpreted as markup or executed.
@MainActor final class SourceFilePreview: NSWindowController, NSWindowDelegate {
    private static var openPreviews: [UUID: SourceFilePreview] = [:]
    private let id = UUID()
    private let url: URL

    static func show(url: URL, line: Int) -> Bool {
        let controller = SourceFilePreview(url: url, line: line)
        openPreviews[controller.id] = controller
        controller.window?.center()
        controller.showWindow(nil)
        return true
    }

    private init(url: URL, line: Int) {
        self.url = url
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 900, height: 650),
                              styleMask: [.titled, .closable, .resizable], backing: .buffered, defer: false)
        super.init(window: window)
        window.title = "\(url.lastPathComponent):\(line)"
        window.isReleasedWhenClosed = false
        window.delegate = self
        let root = NSView()
        window.contentView = root
        let controls = NSStackView()
        controls.orientation = .horizontal
        let path = NSTextField(labelWithString: "\(url.path):\(line)")
        path.isSelectable = true
        path.lineBreakMode = .byTruncatingMiddle
        let open = NSButton(title: "Open file", target: self, action: #selector(openFile))
        let reveal = NSButton(title: "Reveal in Finder", target: self, action: #selector(revealFile))
        controls.addArrangedSubview(path); controls.addArrangedSubview(open); controls.addArrangedSubview(reveal)
        let scroll = NSScrollView()
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = true
        scroll.autohidesScrollers = true
        let text = RichContentView.textView()
        text.font = .monospacedSystemFont(ofSize: 13, weight: .regular)
        text.textContainerInset = NSSize(width: 16, height: 16)
        text.isHorizontallyResizable = true
        text.isVerticallyResizable = true
        scroll.documentView = text
        for view in [controls, scroll] { view.translatesAutoresizingMaskIntoConstraints = false; root.addSubview(view) }
        NSLayoutConstraint.activate([
            controls.topAnchor.constraint(equalTo: root.topAnchor, constant: 10),
            controls.leadingAnchor.constraint(equalTo: root.leadingAnchor, constant: 12),
            controls.trailingAnchor.constraint(equalTo: root.trailingAnchor, constant: -12),
            scroll.topAnchor.constraint(equalTo: controls.bottomAnchor, constant: 10),
            scroll.leadingAnchor.constraint(equalTo: root.leadingAnchor), scroll.trailingAnchor.constraint(equalTo: root.trailingAnchor),
            scroll.bottomAnchor.constraint(equalTo: root.bottomAnchor),
        ])
        let size = (try? url.resourceValues(forKeys: [.fileSizeKey]))?.fileSize ?? Int.max
        if size <= 5_000_000, let source = try? String(contentsOf: url, encoding: .utf8) {
            text.string = source
            let height = RichContentView.textHeight(text, width: 1_000_000)
            let width = text.layoutManager.flatMap { manager in text.textContainer.map { manager.usedRect(for: $0).width } } ?? 900
            text.frame = NSRect(x: 0, y: 0, width: max(900, width + 32), height: max(650, height + 32))
            if let range = Self.range(ofLine: line, in: source) {
                text.setSelectedRange(range)
                text.textStorage?.addAttribute(.backgroundColor, value: NSColor.findHighlightColor.withAlphaComponent(0.35), range: range)
                root.layoutSubtreeIfNeeded()
                text.scrollRangeToVisible(range)
            } else { window.subtitle = "Line \(line) is beyond the end of this file" }
        } else {
            text.string = "Cannot preview line \(line). The file is unavailable, not UTF-8 text, or larger than 5 MB. Use Open file to view it in its default application."
            text.textContainer?.containerSize = NSSize(width: 850, height: CGFloat.greatestFiniteMagnitude)
            text.frame = NSRect(x: 0, y: 0, width: 900, height: 650)
        }
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    static func range(ofLine line: Int, in source: String) -> NSRange? {
        guard line > 0 else { return nil }
        let text = source as NSString
        var current = 1
        var offset = 0
        while offset < text.length {
            let range = text.lineRange(for: NSRange(location: offset, length: 0))
            if current == line { return range }
            offset = NSMaxRange(range)
            current += 1
        }
        return current == line && (source.isEmpty || source.last?.isNewline == true)
            ? NSRange(location: text.length, length: 0) : nil
    }

    @objc private func openFile() { NSWorkspace.shared.open(url) }
    @objc private func revealFile() { NSWorkspace.shared.activateFileViewerSelecting([url]) }
    func windowWillClose(_ notification: Notification) { Self.openPreviews[id] = nil }
}
