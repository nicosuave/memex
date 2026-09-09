import AppKit
import SwiftUI
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct RenderingTests {
    @Test func messagesUseUnlabelledContentSizedBubblesAndKeepAccessibleSpeakers() throws {
        let controller = TranscriptController()
        let window = readerWindow(controller)
        defer { window.close() }
        let records = ["user", "assistant"].enumerated().map { index, role in
            TranscriptRecord(recordID: "clean-\(index)", record: Message(role: role, text: "Short reply",
                toolName: nil, toolInput: nil, toolOutput: nil))
        }
        controller.update(sessionID: "clean", records: records, provider: "codex")
        pump(window)
        #expect(controller.measurement(at: 0).contentWidth < 150)
        for row in 0..<2 {
            let cell = try #require(controller.table.view(atColumn: 0, row: row, makeIfNecessary: true))
            cell.layoutSubtreeIfNeeded()
            #expect(descendants(of: cell, as: NSTextField.self).allSatisfy {
                $0.isHiddenOrHasHiddenAncestor || !["You", "codex"].contains($0.stringValue)
            })
            let text = try #require(descendants(of: cell, as: NSTextView.self).first)
            #expect(text.accessibilityLabel() == (row == 0 ? "You" : "codex"))
            #expect(text.isSelectable)
            #expect(text.frame.maxY <= controller.measurement(at: row).height)
        }
    }

    @Test func userBubblesWrapWithinReaderAtNarrowAndWideSizes() throws {
        let controller = TranscriptController()
        let window = readerWindow(controller)
        defer { window.close() }
        let record = TranscriptRecord(recordID: "wrapped", record: Message(role: "user",
            text: String(repeating: "A longer message that needs room to wrap. ", count: 30),
            toolName: nil, toolInput: nil, toolOutput: nil))
        controller.update(sessionID: "wrapped", records: [record], provider: "codex")
        for width in [350.0, 700.0, 1200.0] {
            window.setContentSize(NSSize(width: width, height: 600))
            pump(window)
            let readerWidth = controller.table.bounds.width
            let value = controller.measurement(at: 0)
            #expect(value.contentWidth <= min(800, readerWidth - 60) * 0.77)
            #expect(abs(value.contentX + value.contentWidth - (readerWidth - 30)) < 1)
            let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
            cell.layoutSubtreeIfNeeded()
            let text = try #require(descendants(of: cell, as: NSTextView.self).first)
            let manager = try #require(text.layoutManager)
            let container = try #require(text.textContainer)
            manager.ensureLayout(for: container)
            #expect(manager.usedRect(for: container).height <= value.fullTextHeight + 1)
        }
    }

    @Test func wideReaderKeepsAssistantAndToolsAtLeftGutter() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 1600, height: 700)
        let records = [
            TranscriptRecord(recordID: "assistant", record: Message(role: "assistant", text: "Response", toolName: nil, toolInput: nil, toolOutput: nil)),
            TranscriptRecord(recordID: "tool", record: Message(role: "tool_use", text: "", toolName: "exec", toolInput: nil, toolOutput: nil)),
            TranscriptRecord(recordID: "user", record: Message(role: "user", text: "Reply", toolName: nil, toolInput: nil, toolOutput: nil))
        ]
        controller.update(sessionID: "wide", records: records, provider: "codex")
        #expect(controller.measurement(at: 0).contentX == 30)
        #expect(controller.measurement(at: 1).contentX == 30)
        let user = controller.measurement(at: 2)
        #expect(abs(user.contentX + user.contentWidth - (controller.table.bounds.width - 30)) < 1)
    }

    @Test func bubbleEdgesDoNotMeasureBlankLinesAndKeepInteriorFormatting() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let texts = ["\n\ngreat start\n\n", "First paragraph\n\nSecond paragraph", "```swift\n    indented()\n\n    next()\n```"]
        let records = texts.enumerated().map { index, text in
            TranscriptRecord(recordID: "trim-\(index)", record: Message(role: "user", text: text,
                toolName: nil, toolInput: nil, toolOutput: nil))
        }
        controller.update(sessionID: "trim", records: records, provider: "codex")
        let short = controller.measurement(at: 0)
        #expect(short.attributedBody.string == "great start")
        #expect(short.textHeight < 30)
        #expect(controller.measurement(at: 1).attributedBody.string == "First paragraph\nSecond paragraph")
        #expect(controller.measurement(at: 2).attributedBody.string == "    indented()\n\n    next()")
        #expect(records[0].record.text == texts[0])
    }

    @Test func richTextCellsPreserveFormattingAndMeasuredHeightAcrossResizes() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let source = "# Heading\n\nA **bold** sentence with [a link](https://example.com).\n\n" +
            String(repeating: "- A list item with enough text to wrap on narrower windows.\n", count: 8) +
            "\n<context>\n## Nested heading\n\n**Structured** prompt content.\n</context>\n\n| Name | Value |\n| --- | --- |\n| Example | 42 |\n\n```xml\n<item name=\"example\">value</item>\n```"
        let record = TranscriptRecord(recordID: "rich", record: Message(role: "assistant", text: source,
            toolName: nil, toolInput: nil, toolOutput: nil))
        controller.update(sessionID: "rich", records: [record], provider: "codex")
        let original = controller.measurement(at: 0).attributedBody
        #expect(!original.string.contains("**bold**"))
        for width in [350.0, 800.0, 500.0] {
            controller.table.setFrameSize(NSSize(width: width, height: controller.table.frame.height))
            let measured = controller.measurement(at: 0)
            #expect(measured.attributedBody === original)
            let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
            cell.layoutSubtreeIfNeeded()
            let text = try #require(descendants(of: cell, as: NSTextView.self).first)
            #expect(text.attributedString().isEqual(to: original))
            let manager = try #require(text.layoutManager)
            let container = try #require(text.textContainer)
            manager.ensureLayout(for: container)
            if let rich = measured.richContent {
                #expect(rich.height(for: measured.contentWidth) == measured.fullTextHeight)
            } else {
                #expect(manager.usedRect(for: container).height <= measured.fullTextHeight + 1)
            }
            if width == 500, let path = ProcessInfo.processInfo.environment["MEMEX_RENDER_SNAPSHOT"],
               let bitmap = cell.bitmapImageRepForCachingDisplay(in: cell.bounds) {
                cell.wantsLayer = true
                cell.layer?.backgroundColor = NSColor.textBackgroundColor.cgColor
                cell.cacheDisplay(in: cell.bounds, to: bitmap)
                try bitmap.representation(using: .png, properties: [:])?.write(to: URL(fileURLWithPath: path))
            }
        }
    }

    @Test func readerScrollsAndResizesWhileMainRunLoopKeepsRunning() throws {
        let profiler = Process()
        if let profilePath = ProcessInfo.processInfo.environment["MEMEX_RENDER_PROFILE"] {
            profiler.executableURL = URL(fileURLWithPath: "/usr/bin/sample")
            profiler.arguments = [String(ProcessInfo.processInfo.processIdentifier), "2", "-file", profilePath]
            profiler.standardOutput = FileHandle.nullDevice
            profiler.standardError = FileHandle.nullDevice
            try profiler.run()
        }
        defer { if profiler.isRunning { profiler.waitUntilExit() } }
        _ = NSApplication.shared
        let store = Store()
        let session = Session(source: "codex", sessionID: "render-test", sourcePath: "/fixture", project: "memex", label: "Rendering fixture")
        store.sessions = [session]
        store.selectedID = session.id
        store.records = fixtureRecords(count: 180)
        store.loadedReaderKey = store.readerPositionKey
        let host = NSHostingView(rootView: ReaderView(store: store))
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 900, height: 700),
                              styleMask: [.titled, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = host
        defer { window.close() }
        // Never order the window onscreen. Unlike a fittingSize-only test, a real
        // window plus the run loop exercises deferred SwiftUI graph transactions.
        let heartbeat = Heartbeat()
        let timer = Timer.scheduledTimer(withTimeInterval: 0.01, repeats: true) { _ in
            MainActor.assumeIsolated { heartbeat.count += 1 }
        }
        defer { timer.invalidate() }
        pump(window)
        let table = try #require(descendants(of: host, as: NSTableView.self).first)
        let scroll = try #require(table.enclosingScrollView)
        #expect(table.numberOfRows > 0)
        for width in [500.0, 1100.0, 650.0, 900.0] {
            window.setContentSize(NSSize(width: width, height: 700))
            pump(window)
            table.scrollRowToVisible(table.numberOfRows - 1)
            pump(window)
            #expect(scroll.contentView.bounds.minY > 0)
            #expect(table.frame.height.isFinite && table.frame.height > 700)
            table.scrollRowToVisible(0)
            pump(window)
        }
        #expect(heartbeat.count >= 20)
        #expect(!window.isVisible)
        store.records = [TranscriptRecord(recordID: "replacement", record: Message(role: "assistant",
            text: "Replacement session text", toolName: nil, toolInput: nil, toolOutput: nil))]
        pump(window)
        #expect(table.numberOfRows == 1)
        let cell = try #require(table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        #expect(descendants(of: cell, as: NSTextView.self).contains { $0.string.trimmingCharacters(in: .whitespacesAndNewlines) == "Replacement session text" && $0.isSelectable })
    }

    @Test func singleToolPairAndInstructionNeedOnlyOneDisclosure() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let records = fixtureRecords(count: 4)
        controller.update(sessionID: "first", records: records, provider: "codex")
        controller.view.layoutSubtreeIfNeeded()
        #expect(controller.table.numberOfRows == 3)
        #expect(controller.rows[2].records.count == 2)
        #expect(controller.measurement(at: 2).body.isEmpty)
        #expect(controller.measurement(at: 2).contentX == 30)
        let cell = try #require(controller.table.view(atColumn: 0, row: 2, makeIfNecessary: true))
        let button = try #require(descendants(of: cell, as: NSButton.self).first { !$0.isHidden && $0.title.contains("Fixture Tool") })
        button.performClick(nil)
        #expect(controller.table.numberOfRows == 3)
        #expect(controller.measurement(at: 2).body.contains(records[2].record.text))
        controller.toggle(controller.rows[2].id)
        #expect(controller.measurement(at: 2).body.isEmpty)
        let instruction = TranscriptRecord(recordID: "instruction", record: Message(role: "developer", text: "Complete instructions", toolName: nil, toolInput: nil, toolOutput: nil))
        controller.update(sessionID: "instructions", records: [instruction], provider: "codex")
        #expect(controller.table.numberOfRows == 1)
        #expect(controller.measurement(at: 0).title == "Developer instructions")
        controller.toggle(controller.rows[0].id)
        #expect(controller.measurement(at: 0).body == "Complete instructions")
    }

    @Test func longMessagesRetainFullSelectableTextBehindShowAll() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let text = String(repeating: "Complete message content. ", count: 400) + "END OF MESSAGE"
        let records = ["user", "assistant"].enumerated().map { index, role in
            TranscriptRecord(recordID: "full-\(index)", record: Message(role: role, text: text, toolName: nil, toolInput: nil, toolOutput: nil))
        }
        controller.update(sessionID: "full", records: records, provider: "codex")
        for row in 0..<2 {
            #expect(controller.measurement(at: row).body == text)
            let cell = try #require(controller.table.view(atColumn: 0, row: row, makeIfNecessary: true))
            #expect(descendants(of: cell, as: NSTextView.self).contains { $0.string.hasSuffix("END OF MESSAGE") })
            #expect(descendants(of: cell, as: NSButton.self).contains { !$0.isHidden && $0.title == "Show all" })
            #expect(controller.measurement(at: row).textHeight < controller.measurement(at: row).fullTextHeight)
            controller.toggleFullBody(controller.rows[row].id)
            #expect(controller.measurement(at: row).textHeight == controller.measurement(at: row).fullTextHeight)
        }
    }

    @Test func anOpenToolStaysOpenWhenItsResultAndAnotherOperationArrive() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let call = TranscriptRecord(recordID: "call", record: Message(role: "tool_use", text: "", toolName: "exec", toolInput: "echo hello", toolOutput: nil))
        let result = TranscriptRecord(recordID: "result", record: Message(role: "tool_result", text: "hello", toolName: "exec", toolInput: nil, toolOutput: nil))
        let next = TranscriptRecord(recordID: "next", record: Message(role: "tool_use", text: "", toolName: "write_file", toolInput: "file body", toolOutput: nil))
        controller.update(sessionID: "pages", records: [call], provider: "codex")
        controller.toggle(controller.rows[0].id)
        controller.update(sessionID: "pages", records: [call, result, next], provider: "codex")
        #expect(controller.table.numberOfRows == 3)
        #expect(controller.rows[1].id == "activity:call")
        #expect(controller.measurement(at: 1).body.contains("echo hello"))
        #expect(controller.measurement(at: 1).body.contains("hello"))
        #expect(controller.rows[1].records.map(\.id) == ["call", "result"])
        #expect(controller.measurement(at: 1).contentX == 50)
    }

    @Test func prependingCallKeepsItsAlreadyOpenResultVisible() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let call = TranscriptRecord(recordID: "call", record: Message(role: "tool_use", text: "", toolName: "exec", toolInput: "echo result", toolOutput: nil))
        let result = TranscriptRecord(recordID: "result", record: Message(role: "tool_result", text: "result content", toolName: "exec", toolInput: nil, toolOutput: nil))
        controller.update(sessionID: "prepend", records: [result], provider: "codex", anchorID: "result")
        #expect(controller.measurement(at: 0).body.contains("result content"))
        controller.update(sessionID: "prepend", records: [call, result], provider: "codex", anchorID: "result")
        #expect(controller.table.numberOfRows == 1)
        #expect(controller.rows[0].id == "activity:call")
        #expect(controller.measurement(at: 0).body.contains("result content"))
        #expect(controller.measurement(at: 0).body.contains("echo result"))
    }

    @Test func appendingPagesAndMeasuringAtDifferentWidthsPreservesRows() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let records = fixtureRecords(count: 120)
        controller.update(sessionID: "first", records: Array(records.prefix(60)), provider: "codex")
        let firstIDs = controller.rows.map(\.id)
        controller.update(sessionID: "first", records: records, provider: "codex")
        #expect(Array(controller.rows.prefix(firstIDs.count).map(\.id)) == firstIDs)
        for width in [400.0, 800.0, 1200.0] {
            controller.table.setFrameSize(NSSize(width: width, height: controller.table.frame.height))
            for row in controller.rows.indices {
                let value = controller.measurement(at: row)
                #expect(value.height.isFinite && value.height > 0)
                #expect(value.contentWidth > 0 && value.contentX >= 0)
            }
        }
    }

    @Test func scrollingLoadsOnePageAndWaitsForAnotherScrollAfterAppend() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let records = fixtureRecords(count: 60)
        var requests = 0
        let load = { requests += 1 }
        controller.update(sessionID: "first", records: records, provider: "codex", hasMore: true, onLoadMore: load)
        controller.view.layoutSubtreeIfNeeded()
        #expect(requests == 0)
        controller.table.scrollRowToVisible(controller.table.numberOfRows - 1)
        controller.loadNextPageIfNeeded()
        controller.loadNextPageIfNeeded()
        #expect(requests == 1)
        controller.update(sessionID: "first", records: records, provider: "codex", hasMore: true,
                          isLoading: true, onLoadMore: load)
        controller.loadNextPageIfNeeded()
        #expect(requests == 1)
        controller.update(sessionID: "first", records: fixtureRecords(count: 120), provider: "codex",
                          hasMore: true, onLoadMore: load)
        #expect(requests == 1)
        controller.table.scrollRowToVisible(controller.table.numberOfRows - 1)
        controller.loadNextPageIfNeeded()
        #expect(requests == 2)
        controller.update(sessionID: "first", records: fixtureRecords(count: 120), provider: "codex",
                          hasMore: false, onLoadMore: load)
        controller.loadNextPageIfNeeded()
        #expect(requests == 2)
    }

    @Test func shortCollapsedPageCanLoadOnWheelAndDoesNotCascadeOnAppend() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let records = (0..<60).map { index in
            TranscriptRecord(recordID: "\(index)", record: Message(role: "tool_use", text: "", toolName: "Read", toolInput: nil, toolOutput: nil))
        }
        var requests = 0
        let load = { requests += 1 }
        controller.update(sessionID: "first", records: records, provider: "codex", hasMore: true, onLoadMore: load)
        controller.view.layoutSubtreeIfNeeded()
        #expect(requests == 0)
        controller.scrollView.onScroll?()
        #expect(requests == 1)
        controller.update(sessionID: "first", records: records, provider: "codex", hasMore: true, isLoading: true, onLoadMore: load)
        // Failed page: allow a fresh wheel gesture to retry, with no automatic loop.
        controller.update(sessionID: "first", records: records, provider: "codex", hasMore: true, onLoadMore: load)
        #expect(requests == 1)
        controller.scrollView.onScroll?()
        #expect(requests == 2)
    }

    @Test func browsingStartsAtEndAfterAsyncLoadAndReturningRestoresPosition() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 450)
        let records = simpleRecords(0..<120)
        controller.update(sessionID: "browse-a", records: [], provider: "codex", isLoading: true, startsAtEnd: true)
        controller.update(sessionID: "browse-a", records: records, provider: "codex", startsAtEnd: true)
        controller.view.layoutSubtreeIfNeeded()
        #expect(controller.scrollView.documentVisibleRect.maxY >= controller.table.rect(ofRow: 119).maxY - 1)
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: controller.table.rect(ofRow: 35).minY + 12))
        let savedY = controller.scrollView.contentView.bounds.minY
        controller.update(sessionID: "browse-b", records: simpleRecords(200..<280), provider: "codex", startsAtEnd: true)
        controller.update(sessionID: "browse-a", records: [], provider: "codex", isLoading: true, startsAtEnd: true)
        controller.update(sessionID: "browse-a", records: records, provider: "codex", startsAtEnd: true)
        #expect(abs(controller.scrollView.contentView.bounds.minY - savedY) < 1)
    }

    @Test func prependingEarlierMessagesKeepsVisibleMessageAtSameOffset() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 450)
        var requests = 0
        controller.update(sessionID: "browse", records: simpleRecords(60..<120), provider: "codex",
                          hasEarlier: true, startsAtEnd: true, onLoadEarlier: { requests += 1 })
        #expect(requests == 0)
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: 12))
        controller.loadNextPageIfNeeded()
        #expect(requests == 1)
        controller.update(sessionID: "browse", records: simpleRecords(0..<120), provider: "codex", startsAtEnd: true)
        let expected = controller.table.rect(ofRow: 60).minY + 12
        #expect(abs(controller.scrollView.contentView.bounds.minY - expected) < 1)
        #expect(requests == 1)
    }

    @Test func searchOpensAtMatchedMessageAndExpandsMatchedTool() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 450)
        let records = simpleRecords(0..<100)
        controller.update(sessionID: "search-query", records: records, provider: "codex", anchorID: "45")
        #expect(abs(controller.scrollView.contentView.bounds.minY - controller.table.rect(ofRow: 45).minY) < 1)
        var withTool = records
        withTool[45] = TranscriptRecord(recordID: "tool", record: Message(role: "tool_use", text: "matching tool content", toolName: "Read", toolInput: nil, toolOutput: nil))
        controller.update(sessionID: "search-tool", records: withTool, provider: "codex", anchorID: "tool")
        let index = try #require(controller.rows.firstIndex { $0.id == "activity:tool" })
        #expect(controller.measurement(at: index).body == "matching tool content")
        #expect(abs(controller.scrollView.contentView.bounds.minY - controller.table.rect(ofRow: index).minY) < 1)
    }

    private func readerWindow(_ controller: TranscriptController) -> NSWindow {
        _ = NSApplication.shared
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 600),
                              styleMask: [.titled, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = controller
        return window
    }

    private func simpleRecords(_ range: Range<Int>) -> [TranscriptRecord] {
        range.map { TranscriptRecord(recordID: "\($0)", record: Message(role: "assistant", text: "Message \($0)", toolName: nil, toolInput: nil, toolOutput: nil)) }
    }

    private func fixtureRecords(count: Int) -> [TranscriptRecord] {
        (0..<count).map { index in
            let roles = ["user", "assistant", "tool_use", "tool_result"]
            return TranscriptRecord(recordID: "\(index)", record: Message(
                role: roles[index % 4],
                text: String(repeating: "## Heading\n\nA **long** transcript with `code` and [links](https://example.com).\n\n- Words to wrap in a list.\n\n", count: 20),
                toolName: index % 4 == 2 ? "fixture_tool" : nil, toolInput: nil, toolOutput: nil))
        }
    }

    private func pump(_ window: NSWindow) {
        window.contentView?.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        let deadline = Date().addingTimeInterval(0.08)
        while Date() < deadline {
            RunLoop.main.run(mode: .default, before: deadline)
        }
    }
}

@MainActor private final class Heartbeat { var count = 0 }

@MainActor private func descendants<T: NSView>(of view: NSView, as type: T.Type) -> [T] {
    ((view as? T).map { [$0] } ?? []) + view.subviews.flatMap { descendants(of: $0, as: type) }
}
