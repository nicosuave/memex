import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor struct TranscriptSelectionTests {
    private func record(_ text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: "selected-record", record: Message(role: "assistant", text: text,
            toolName: nil, toolInput: nil, toolOutput: nil))
    }

    private func textViews(in view: NSView) -> [NSTextView] {
        guard !view.isHidden else { return [] }
        return (view as? NSTextView).map { [$0] } ?? view.subviews.flatMap { textViews(in: $0) }
    }

    private func window(_ controller: TranscriptController) -> NSWindow {
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 500),
                              styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 500)
        window.contentViewController = controller
        window.orderFront(nil)
        window.contentView?.layoutSubtreeIfNeeded()
        return window
    }

    private func showActions(in text: NSTextView) throws {
        var ancestor = text.superview
        while let view = ancestor {
            if let target = view as? any TranscriptSelectionTarget {
                target.showSelectionActions(in: text)
                return
            }
            ancestor = view.superview
        }
        Issue.record("No transcript selection target")
    }

    private func addButton(_ actions: TranscriptSelectionActions) throws -> NSButton {
        try #require(actions.popover?.contentViewController?.view.subviews.compactMap { $0 as? NSButton }.first)
    }

    private func activate(_ button: NSButton) throws {
        // performClick runs AppKit's press animation in a nested event loop.
        // Its delayed CFRunLoopStop can stop Swift Testing's async-main loop.
        #expect(button.isEnabled)
        #expect(NSApplication.shared.sendAction(try #require(button.action), to: button.target, from: button))
    }

    @Test(arguments: ["Before **selected café 👋** after.", "Before\n\n```swift\nlet selected = 42\n```\n\nAfter"])
    func selectedTextIsCapturedFromProseAndCode(_ markdown: String) throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { controller.selectionActions.dismiss(); window.close() }
        let source = record(markdown)
        controller.update(sessionID: "one", records: [source], provider: "codex")
        var received: TranscriptSelection?
        controller.onAddSelection = { received = $0; return nil }
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        cell.layoutSubtreeIfNeeded()
        let text = try #require(textViews(in: cell).first { !$0.isHidden && $0.string.contains("selected") })
        let expected = markdown.contains("café") ? "selected café 👋" : "selected = 42"
        text.setSelectedRange((text.string as NSString).range(of: expected))
        #expect(controller.selectionActions.popover == nil) // Find also sets a range programmatically.
        try showActions(in: text)
        #expect(controller.selectionActions.popover?.isShown == true)
        #expect(text is TranscriptSelectionTextView)
        try expectActionsAboveSelection(controller.selectionActions, text: text)
        try activate(addButton(controller.selectionActions))
        #expect(received?.text == expected)
        #expect(received?.sourceIDs == [source.sourceID])
        #expect(received?.location?.range == text.selectedRange())
        #expect(controller.selectionActions.popover == nil)
        let selection = try #require(received)
        text.setSelectedRange(NSRange(location: 0, length: 0))
        controller.revealSelection(.init(selection: selection, transcriptKey: "one")) { #expect($0 == nil) }
        let revealedCell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        #expect(textViews(in: revealedCell).contains { TranscriptSelectionActions.selectedText(in: $0) == expected })
        #expect(controller.selectionActions.popover == nil)
    }

    private func expectActionsAboveSelection(_ actions: TranscriptSelectionActions, text: NSTextView) throws {
        let window = try #require(text.window)
        let manager = try #require(text.layoutManager)
        let container = try #require(text.textContainer)
        let glyphs = manager.glyphRange(forCharacterRange: text.selectedRange(), actualCharacterRange: nil)
        let rect = manager.boundingRect(forGlyphRange: glyphs, in: container)
            .offsetBy(dx: text.textContainerOrigin.x, dy: text.textContainerOrigin.y)
        let selectedOnScreen = window.convertToScreen(text.convert(rect, to: nil))
        let popup = try #require(actions.popover?.contentViewController?.view.window)
        #expect(popup.frame.minY >= selectedOnScreen.maxY + 5)
        #expect(!popup.frame.intersects(selectedOnScreen))
    }

    @Test func wrappedSelectionKeepsActionsAboveAllHighlightedLines() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { controller.selectionActions.dismiss(); window.close() }
        controller.update(sessionID: "wrapped", records: [record(String(repeating: "selected words ", count: 30))], provider: "codex")
        controller.onAddSelection = { _ in nil }
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        cell.layoutSubtreeIfNeeded()
        let text = try #require(textViews(in: cell).first { !$0.isHidden })
        text.setSelectedRange(NSRange(location: 10, length: 180))
        try showActions(in: text)
        try expectActionsAboveSelection(controller.selectionActions, text: text)
    }

    @Test func emptyAndWhitespaceSelectionsHaveNoAction() {
        let text = TranscriptSelectionTextView()
        text.string = "one   two"
        text.setSelectedRange(NSRange(location: 0, length: 0))
        #expect(TranscriptSelectionActions.selectedText(in: text) == nil)
        text.setSelectedRange(NSRange(location: 3, length: 3))
        #expect(TranscriptSelectionActions.selectedText(in: text) == nil)
        text.setSelectedRange(NSRange(location: 0, length: 6))
        #expect(TranscriptSelectionActions.selectedText(in: text) == "one   ")
    }

    @Test func keyboardSelectionOpensActionsButFindDoesNot() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { controller.selectionActions.dismiss(); window.close() }
        let records = [record("Choose this text")]
        controller.update(sessionID: "one", records: records, provider: "codex")
        controller.onAddSelection = { _ in nil }
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        cell.layoutSubtreeIfNeeded()
        let text = try #require(textViews(in: cell).first { !$0.isHidden })
        window.makeFirstResponder(text)
        text.setSelectedRange(NSRange(location: 0, length: 0))
        let key = try #require(NSEvent.keyEvent(with: .keyDown, location: .zero, modifierFlags: .shift,
            timestamp: 0, windowNumber: window.windowNumber, context: nil, characters: "\u{f703}",
            charactersIgnoringModifiers: "\u{f703}", isARepeat: false, keyCode: 124))
        text.keyDown(with: key)
        #expect(TranscriptSelectionActions.selectedText(in: text) == "C")
        #expect(controller.selectionActions.popover?.isShown == true)
        let hit = try #require(ConversationMatcher.matches(records, query: "text").first)
        controller.update(sessionID: "one", records: records, provider: "codex", findQuery: "text", findHit: hit, findGeneration: 1)
        #expect(controller.selectionActions.popover == nil)
        #expect(controller.selectedFindRange != nil)
    }

    @Test func sessionSwitchDismissesActionsAndCannotCaptureIntoAnotherChat() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { controller.selectionActions.dismiss(); window.close() }
        controller.update(sessionID: "one", records: [record("First chat")], provider: "codex")
        var count = 0
        controller.onAddSelection = { _ in count += 1; return nil }
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        cell.layoutSubtreeIfNeeded()
        let text = try #require(textViews(in: cell).first { !$0.isHidden })
        text.setSelectedRange(NSRange(location: 0, length: 5))
        try showActions(in: text)
        let oldButton = try addButton(controller.selectionActions)
        controller.update(sessionID: "two", records: [record("Second chat")], provider: "codex")
        #expect(controller.selectionActions.popover == nil)
        try activate(oldButton)
        #expect(count == 0)
    }

    @Test func changedSelectionCannotAttachOldTextAndShowsReason() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { controller.selectionActions.dismiss(); window.close() }
        controller.update(sessionID: "one", records: [record("First second")], provider: "codex")
        var count = 0
        controller.onAddSelection = { _ in count += 1; return nil }
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        cell.layoutSubtreeIfNeeded()
        let text = try #require(textViews(in: cell).first { !$0.isHidden })
        text.setSelectedRange(NSRange(location: 0, length: 5))
        try showActions(in: text)
        text.setSelectedRange(NSRange(location: 6, length: 6))
        try activate(addButton(controller.selectionActions))
        #expect(count == 0)
        let error = controller.selectionActions.popover?.contentViewController?.view.subviews.compactMap { $0 as? NSTextField }.first
        #expect(error?.stringValue == "The selection changed. Select the text again.")
    }

    @Test(arguments: [false, true])
    func persistedSelectionRevealsExactRepeatedOccurrence(_ code: Bool) throws {
        let phrase = "selected café 👋"
        let source = code
            ? "```text\n\(phrase)\n```\n\nBetween\n\n```text\n\(phrase)\n```"
            : "\(phrase) then \(phrase)"
        let controller = TranscriptController()
        let window = window(controller)
        defer { controller.selectionActions.dismiss(); window.close() }
        controller.update(sessionID: "capture", records: [record(source)], provider: "codex")
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        cell.layoutSubtreeIfNeeded()
        let candidates = textViews(in: cell).filter { $0.string.contains(phrase) }
        let text = try #require(candidates.last)
        let range = (text.string as NSString).range(of: phrase, options: .backwards)
        text.setSelectedRange(range)
        var captured: TranscriptSelection?
        controller.onAddSelection = { captured = $0; return nil }
        try showActions(in: text)
        try activate(addButton(controller.selectionActions))
        let saved = try JSONEncoder().encode(try #require(captured))
        let restored = try JSONDecoder().decode(TranscriptSelection.self, from: saved)
        // Recreate the rendered row, as happens after navigation or relaunch.
        controller.update(sessionID: "reopened", records: [record(source)], provider: "codex")
        controller.revealSelection(.init(selection: restored, transcriptKey: "reopened")) { #expect($0 == nil) }
        let reopened = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        let views = textViews(in: reopened).filter { $0.string.contains(phrase) }
        #expect(views.last?.selectedRange() == range)
        #expect(views.dropLast().allSatisfy { $0.selectedRange().length == 0 })
        #expect(controller.selectionActions.popover == nil)
    }

    @Test func revealScrollsToSourceAndRepeatedClicksWorkWithoutChangingFind() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { window.close() }
        let records = (0..<50).map { index in
            TranscriptRecord(recordID: "row-\(index)", record: Message(role: "assistant",
                text: "Repeated selected passage in message \(index)", toolName: nil, toolInput: nil, toolOutput: nil))
        }
        let findHit = try #require(ConversationMatcher.matches(records, query: "message 1").first)
        controller.update(sessionID: "scroll", records: records, provider: "codex",
                          findQuery: "message 1", findHit: findHit, findGeneration: 1, bottomInset: 120)
        let selection = TranscriptSelection(text: "selected passage", sourceIDs: [records[40].sourceID])
        for _ in 0..<2 {
            controller.scrollView.contentView.scroll(to: .zero)
            controller.revealSelection(.init(selection: selection, transcriptKey: "scroll")) { #expect($0 == nil) }
            let cell = try #require(controller.table.view(atColumn: 0, row: 40, makeIfNecessary: true))
            let text = try #require(textViews(in: cell).first)
            #expect(TranscriptSelectionActions.selectedText(in: text) == "selected passage")
            #expect(window.firstResponder === text)
            #expect(controller.scrollView.contentView.bounds.minY > 0)
            #expect(text.convert(text.bounds, to: controller.table).maxY <= controller.scrollView.contentView.bounds.maxY - 120)
            #expect(controller.selectedFindRange != nil)
        }
        let next = TranscriptSelection(text: "message 45", sourceIDs: [records[45].sourceID])
        controller.revealSelection(.init(selection: next, transcriptKey: "scroll")) { #expect($0 == nil) }
        let oldCell = try #require(controller.table.view(atColumn: 0, row: 40, makeIfNecessary: true))
        #expect(textViews(in: oldCell).allSatisfy { $0.selectedRange().length == 0 })
    }

    @Test func missingAmbiguousAndOtherSessionSelectionsNeverChooseAnUnrelatedPassage() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { window.close() }
        controller.update(sessionID: "current", records: [record("same same")], provider: "codex")
        for selection in [TranscriptSelection(text: "same", sourceIDs: ["missing"]),
                          TranscriptSelection(text: "same", sourceIDs: ["selected-record"]),
                          TranscriptSelection(text: "changed text", sourceIDs: ["selected-record"])] {
            var failure: String?
            controller.revealSelection(.init(selection: selection, transcriptKey: "current")) { failure = $0 }
            #expect(failure != nil)
            let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
            #expect(textViews(in: cell).allSatisfy { $0.selectedRange().length == 0 })
        }
        controller.revealSelection(.init(selection: .init(text: "same same", sourceIDs: ["selected-record"]),
                                         transcriptKey: "previous")) { _ in Issue.record("Stale session request was applied") }
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        #expect(textViews(in: cell).allSatisfy { $0.selectedRange().length == 0 })
    }

    @Test func revealOpensCollapsedToolSourceWithoutOpeningSelectionActions() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { window.close() }
        let source = TranscriptRecord(recordID: "tool", record: Message(role: "tool_use", text: "",
            toolName: "exec_command", toolInput: #"{"cmd":"echo selected-tool-passage"}"#, toolOutput: nil))
        controller.update(sessionID: "tool", records: [source], provider: "codex")
        #expect(!controller.measurement(at: 0).hasBody)
        controller.revealSelection(.init(selection: .init(text: "selected-tool-passage", sourceIDs: [source.sourceID]),
                                         transcriptKey: "tool")) { #expect($0 == nil) }
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        #expect(textViews(in: cell).contains { TranscriptSelectionActions.selectedText(in: $0) == "selected-tool-passage" })
        #expect(controller.selectionActions.popover == nil)
    }

    #if canImport(SQACPHost)
    @Test func addToChatPreservesDraftAndAttachmentsWithoutSending() throws {
        let session = Session(source: "codex", sessionID: "selection", sourcePath: "/tmp/selection.jsonl",
                              project: "selection", cwd: "/tmp", machine: "local")
        let live = LiveConversation(session: session, checkOwnership: { _ in false })
        live.draft = "Existing unsent question"
        #expect(live.appendContext(title: "Earlier", text: "existing bytes", source: "earlier"))
        let first = try #require(live.attachments.first)
        #expect(live.appendTranscriptSelection(.init(text: "  exact café 👋\n", sourceIDs: ["record-42"])) == nil)
        #expect(live.draft == "Existing unsent question")
        #expect(live.attachments.first == first)
        let captured = try #require(live.attachments.last)
        let restored = try JSONDecoder().decode(ConversationAttachment.self, from: JSONEncoder().encode(captured))
        #expect(restored.selectedTranscriptText(in: session.id)?.text == "  exact café 👋\n")
        #expect(restored.selectedTranscriptText(in: "another chat") == nil)
        let content = try #require(JSONSerialization.jsonObject(with: captured.content) as? [String: Any])
        #expect(content["text"] as? String == "Attached context: Selected text\nSource: \(session.id)#record-42\n\n  exact café 👋\n")
        #expect(!live.connectionAttempted)
        #expect(live.pendingPrompt == nil)
    }

    @Test func legacySelectedTextChipsRecoverCapturedBytesAndSource() throws {
        let sessionID = "local\u{1f}codex\u{1f}session\u{1f}/tmp/source, with-comma.jsonl"
        let source = "\(sessionID)#record-1, \(sessionID)#record-2"
        let text = "  café 👋\n**literal captured text**\n"
        let original = try ConversationAttachment.text(title: "Selected text", text: text, source: source)
        var json = try #require(JSONSerialization.jsonObject(with: JSONEncoder().encode(original)) as? [String: Any])
        json.removeValue(forKey: "transcriptSelection")
        let restored = try JSONDecoder().decode(ConversationAttachment.self, from: JSONSerialization.data(withJSONObject: json))
        let selection = try #require(restored.selectedTranscriptText(in: sessionID))
        #expect(selection.text == text)
        #expect(selection.sourceIDs == ["record-1", "record-2"])
        #expect(selection.location == nil)
        #expect(restored.content == original.content)
        #expect(restored.selectedTranscriptText(in: "another session") == nil)
    }

    @Test func unavailableConversationReportsOwnershipAndPreservesDraft() {
        let session = Session(source: "codex", sessionID: "selection", sourcePath: "/tmp/selection.jsonl",
                              project: "selection", cwd: "/tmp", machine: "local")
        let live = LiveConversation(session: session, checkOwnership: { _ in true })
        live.draft = "Keep me"
        let error = live.appendTranscriptSelection(.init(text: "selected", sourceIDs: ["record"]))
        #expect(error == "This conversation is open in another app. Close it there before adding context here.")
        #expect(live.draft == "Keep me")
        #expect(live.attachments.isEmpty)
    }
    #endif
}
