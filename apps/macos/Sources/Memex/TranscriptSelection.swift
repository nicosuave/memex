import AppKit
import CryptoKit

struct TranscriptSelection: Codable, Equatable, Sendable {
    let text: String
    let sourceIDs: [String]
    var sessionID: String? = nil
    var location: Location? = nil

    struct Location: Codable, Equatable, Sendable {
        let viewIndex: Int
        let range: NSRange
        let renderedRowDigest: String
    }

    @MainActor static func digest(_ views: [NSTextView]) -> String {
        var hash = SHA256()
        for view in views {
            let bytes = Data(view.string.utf8)
            hash.update(data: Data("\(bytes.count):".utf8))
            hash.update(data: bytes)
        }
        return hash.finalize().map { String(format: "%02x", $0) }.joined()
    }

    /// Only use an offset against the identical rendered row. Older captures or
    /// changed messages must have one exact match, never an arbitrary first hit.
    @MainActor func matches(in views: [NSTextView]) -> [(NSTextView, NSRange)] {
        if let location, views.indices.contains(location.viewIndex),
           Self.digest(views) == location.renderedRowDigest {
            let view = views[location.viewIndex]
            let body = view.string as NSString
            let range = location.range
            if range.location >= 0, range.length > 0, range.location <= body.length,
               range.length <= body.length - range.location, body.substring(with: range) == text {
                return [(view, range)]
            }
        }
        guard !text.isEmpty else { return [] }
        var matches: [(NSTextView, NSRange)] = []
        for view in views {
            let body = view.string as NSString
            var start = 0
            while start < body.length {
                let range = body.range(of: text, options: .literal,
                                       range: NSRange(location: start, length: body.length - start))
                guard range.location != NSNotFound else { break }
                matches.append((view, range))
                if matches.count > 1 { return matches }
                start = range.location + 1
            }
        }
        return matches
    }
}

struct TranscriptSelectionReveal: Equatable {
    let id = UUID()
    let selection: TranscriptSelection
    let transcriptKey: String
}

@MainActor protocol TranscriptSelectionTarget: AnyObject {
    func dismissSelectionActions()
    func showSelectionActions(in textView: NSTextView)
}

/// Present actions only after a user finishes selecting. Programmatic selections
/// from Find and row restoration must not open a popover.
@MainActor final class TranscriptSelectionTextView: NSTextView {
    private var selectionTarget: (any TranscriptSelectionTarget)? {
        var ancestor = superview
        while let view = ancestor {
            if let target = view as? any TranscriptSelectionTarget { return target }
            ancestor = view.superview
        }
        return nil
    }

    override func mouseDown(with event: NSEvent) {
        selectionTarget?.dismissSelectionActions()
        // NSTextView tracks the drag through mouse-up inside mouseDown.
        super.mouseDown(with: event)
        selectionTarget?.showSelectionActions(in: self)
    }

    override func keyDown(with event: NSEvent) {
        let previous = selectedRanges
        super.keyDown(with: event)
        if previous != selectedRanges {
            selectionTarget?.dismissSelectionActions()
            selectionTarget?.showSelectionActions(in: self)
        }
    }
}

@MainActor final class TranscriptSelectionActions: NSObject, NSPopoverDelegate {
    private(set) var popover: NSPopover?
    private var addSelection: (() -> String?)?
    private var errorLabel: NSTextField?

    static func selectedText(in textView: NSTextView) -> String? {
        let range = textView.selectedRange()
        let text = textView.string as NSString
        guard range.location != NSNotFound, range.length > 0,
              range.location <= text.length, range.length <= text.length - range.location else { return nil }
        let selected = text.substring(with: range)
        return selected.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty ? nil : selected
    }

    func show(in textView: NSTextView, sourceIDs: [String], location: TranscriptSelection.Location? = nil,
              isCurrent: @escaping () -> Bool, add: @escaping (TranscriptSelection) -> String?) {
        dismiss()
        guard let selected = Self.selectedText(in: textView), let anchorView = textView.window?.contentView,
              !sourceIDs.isEmpty, let manager = textView.layoutManager, let container = textView.textContainer else { return }
        let range = textView.selectedRange()
        manager.ensureLayout(for: container)
        let glyphs = manager.glyphRange(forCharacterRange: range, actualCharacterRange: nil)
        let selectionRect = manager.boundingRect(forGlyphRange: glyphs, in: container)
            .offsetBy(dx: textView.textContainerOrigin.x, dy: textView.textContainerOrigin.y)
            .intersection(textView.visibleRect)
        guard !selectionRect.isEmpty else { return }
        let selection = TranscriptSelection(text: selected, sourceIDs: sourceIDs, location: location)
        addSelection = { [weak textView] in
            guard let textView, textView.window != nil, isCurrent(),
                  textView.selectedRange() == range, Self.selectedText(in: textView) == selected else {
                return "The selection changed. Select the text again."
            }
            return add(selection)
        }
        let controller = NSViewController()
        controller.view = NSView(frame: NSRect(x: 0, y: 0, width: 122, height: 38))
        let button = NSButton(title: "Add to chat", target: self, action: #selector(addToChat))
        button.isBordered = false
        button.frame = NSRect(x: 8, y: 6, width: 106, height: 26)
        button.setAccessibilityLabel("Add selected text to chat")
        controller.view.addSubview(button)
        let error = NSTextField(wrappingLabelWithString: "")
        error.font = .systemFont(ofSize: 12)
        error.textColor = .secondaryLabelColor
        error.isHidden = true
        controller.view.addSubview(error)
        errorLabel = error
        let popover = NSPopover()
        popover.behavior = .transient
        popover.animates = false
        popover.delegate = self
        popover.contentViewController = controller
        self.popover = popover
        // Anchor in the window's content view so the gap is not clipped to a
        // one-line NSTextView. The upper edge depends on that view's coordinates.
        let anchorRect = anchorView.convert(selectionRect, from: textView).insetBy(dx: 0, dy: -6)
        popover.show(relativeTo: anchorRect, of: anchorView,
                     preferredEdge: anchorView.isFlipped ? .minY : .maxY)
    }

    func dismiss() {
        popover?.close()
        popover = nil
        addSelection = nil
        errorLabel = nil
    }

    func popoverDidClose(_ notification: Notification) {
        guard let closed = notification.object as? NSPopover, closed === popover else { return }
        popover = nil
        addSelection = nil
        errorLabel = nil
    }

    @objc private func addToChat() {
        guard let addSelection else { return }
        if let error = addSelection() {
            errorLabel?.stringValue = error
            errorLabel?.isHidden = false
            errorLabel?.frame = NSRect(x: 12, y: 10, width: 306, height: 50)
            popover?.contentSize = NSSize(width: 330, height: 96)
            popover?.contentViewController?.view.subviews.compactMap { $0 as? NSButton }.first?.frame.origin.y = 64
        } else {
            dismiss()
        }
    }
}
