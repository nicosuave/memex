import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor struct ReaderEnhancementTests {
    @Test func expandedToolPanelHasEqualInsetsAroundItsContent() throws {
        let tool = TranscriptRecord(recordID: "tool", record: Message(role: "tool_use", text: "", toolName: "exec_command", toolInput: #"{"cmd":"printf hello"}"#, toolOutput: nil))
        let reader = controller([tool])
        reader.toggle(reader.rows[0].id)
        let value = reader.measurement(at: 0)
        let cell = try #require(reader.tableView(reader.table, viewFor: reader.table.tableColumns.first, row: 0))
        cell.frame = NSRect(x: 0, y: 0, width: 700, height: value.height)
        cell.layoutSubtreeIfNeeded()
        let clip = try #require(value.richContent?.superview)
        let panel = try #require(cell.subviews.first { !$0.isHidden && $0.layer?.cornerRadius == 8 })
        #expect(clip.frame.minX - panel.frame.minX == 12)
        #expect(panel.frame.maxX - clip.frame.maxX == 12)
        #expect(clip.frame.minY - panel.frame.minY == 12)
        #expect(panel.frame.maxY - clip.frame.maxY == 12)
    }

    @Test func showingActionsDoesNotMoveDisclosureTitleOrChevron() throws {
        let reader = controller([record("tool", "tool_use", String(repeating: "Long command ", count: 20))])
        let value = reader.measurement(at: 0)
        let cell = try #require(reader.tableView(reader.table, viewFor: reader.table.tableColumns.first, row: 0))
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: value.height), styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        defer { window.close() }
        window.contentView = cell
        cell.layoutSubtreeIfNeeded()
        let buttons = cell.subviews.compactMap { $0 as? NSButton }
        let disclosure = try #require(buttons.first { $0.title == value.title })
        let copy = try #require(buttons.first { $0.accessibilityLabel() == "Copy message" })
        let frame = disclosure.frame
        let titleRect = disclosure.cell?.titleRect(forBounds: disclosure.bounds)
        let imageRect = disclosure.cell?.imageRect(forBounds: disclosure.bounds)
        #expect(window.makeFirstResponder(copy))
        cell.updateTrackingAreas()
        #expect(copy.alphaValue == 1)
        #expect(copy.title.isEmpty)
        #expect(copy.image != nil)
        #expect(!buttons.contains { ["Raw", "Source"].contains($0.title) })
        #expect(disclosure.frame == frame)
        #expect(disclosure.cell?.titleRect(forBounds: disclosure.bounds) == titleRect)
        #expect(disclosure.cell?.imageRect(forBounds: disclosure.bounds) == imageRect)
        #expect(disclosure.frame.maxX <= copy.frame.minX)
        window.makeFirstResponder(nil)
        cell.updateTrackingAreas()
        #expect(disclosure.frame == frame)
    }

    @Test func toolReopenReusesRenderedContentAndKeepsCollapsedBodyEmpty() throws {
        let tool = TranscriptRecord(recordID: "tool", record: Message(role: "tool_use", text: "", toolName: "exec_command", toolInput: #"{"cmd":"printf hello"}"#, toolOutput: nil))
        let reader = controller([tool])
        let id = reader.rows[0].id
        reader.toggle(id)
        let opened = reader.measurement(at: 0)
        let rich = try #require(opened.richContent)
        reader.toggle(id)
        #expect(!reader.measurement(at: 0).hasBody)
        reader.toggle(id)
        #expect(reader.measurement(at: 0).richContent === rich)
        #expect(reader.measurement(at: 0).attributedBody === opened.attributedBody)
        #expect(reader.measurement(at: 0).height == opened.height)
        reader.toggleRaw(id)
        #expect(reader.measurement(at: 0).richContent == nil)
        reader.toggleRaw(id)
        #expect(reader.measurement(at: 0).richContent === rich)
    }

    @Test func longRichBodyStartsAtTopAndFooterDoesNotOverlap() throws {
        let reader = controller([record("code", "assistant", "```swift\n" + String(repeating: "let value = 42\n", count: 100) + "```")])
        let value = reader.measurement(at: 0)
        let cell = try #require(reader.tableView(reader.table, viewFor: reader.table.tableColumns.first, row: 0))
        cell.frame = NSRect(x: 0, y: 0, width: 700, height: value.height)
        cell.layoutSubtreeIfNeeded()
        let show = try #require(cell.subviews.compactMap { $0 as? NSButton }.first { $0.title == "Show all" })
        let copy = try #require(cell.subviews.compactMap { $0 as? NSButton }.first { $0.accessibilityLabel() == "Copy message" })
        let rich = try #require(value.richContent)
        let clip = try #require(rich.superview)
        #expect(clip.isFlipped)
        #expect(rich.frame.minY == 0)
        #expect(show.frame.minY >= clip.frame.maxY)
        #expect(copy.frame.minY >= show.frame.maxY)
        #expect(copy.frame.maxY <= cell.bounds.maxY)
        #expect(copy.alphaValue == 0)
    }

    @Test func longMessagesExpandWithoutDiscardingCopyText() {
        let source = String(repeating: "A complete line of source content.\n\n", count: 120)
        let records = [record("long", "user", source)]
        let reader = controller(records)
        let collapsed = reader.measurement(at: 0)
        #expect(collapsed.isLong)
        #expect(collapsed.textHeight == 360)
        #expect(collapsed.body == source)
        #expect(collapsed.attributedBody.string.contains("source content."))
        reader.toggleFullBody(reader.rows[0].id)
        let expanded = reader.measurement(at: 0)
        #expect(expanded.showsFullBody)
        #expect(expanded.textHeight == expanded.fullTextHeight)
        #expect(expanded.height > collapsed.height)
        reader.toggleFullBody(reader.rows[0].id)
        #expect(reader.measurement(at: 0).textHeight == collapsed.textHeight)
    }

    @Test func findShowsExactOccurrenceInsideSuppressedMixedContext() throws {
        let source = "<recommended_plugins>\nHidden needle\n</recommended_plugins>\nActual request"
        let records = [record("mixed", "user", source)]
        let reader = controller(records)
        #expect(reader.rows.count == 2)
        #expect(reader.measurement(at: 1).body.trimmingCharacters(in: .whitespacesAndNewlines) == "Actual request")
        let hit = try #require(ConversationMatcher.matches(records, query: "needle").first)
        reader.update(sessionID: "reader", records: records, provider: "codex", findQuery: "needle", findHit: hit, findGeneration: 1)
        #expect(reader.rows.count == 1)
        let selected = try #require(reader.selectedFindRange)
        #expect((reader.measurement(at: 0).attributedBody.string as NSString).substring(with: selected) == "needle")
        reader.update(sessionID: "reader", records: records, provider: "codex")
        #expect(reader.rows.count == 2)
    }

    @Test func rawTranscriptContainsUnknownFieldsAndUnpairedRecords() throws {
        let data = Data(#"{"record_id":"raw","unknown_envelope":42,"record":{"role":"user","text":"<environment_context>hidden</environment_context>","new_provider_field":{"original":true}}}"#.utf8)
        let source = try JSONDecoder().decode(TranscriptRecord.self, from: data)
        let reader = controller([source])
        reader.update(sessionID: "reader", records: [source], provider: "codex", rawTranscript: true)
        let value = reader.measurement(at: 0)
        #expect(!value.isDisclosure)
        #expect(value.body.contains("unknown_envelope"))
        #expect(value.body.contains("new_provider_field"))
        #expect(value.body.contains("hidden"))
        #expect(value.attributedBody.string == source.rawTranscriptBody)
    }

    @Test func promptOutlineOmitsInjectedContextAndRetainsSourceOffsets() {
        let records = [record("context", "user", "<environment_context>\nprivate setup\n</environment_context>"),
                       record("a", "user", "<recommended_plugins>\ncatalog\n</recommended_plugins>\nFix the reader"),
                       record("b", "assistant", "Working"), record("c", "user", "Then test it")]
        let entries = ConversationOutline.entries(records, offset: 20)
        #expect(entries.map(\.id) == ["a", "c"])
        #expect(entries.map(\.offset) == [21, 23])
        #expect(entries.first?.preview == "Fix the reader")
    }

    @Test func sourceOnlyMarkdownMatchIsSelectedPrecisely() throws {
        let records = [record("link", "assistant", "See [the result](https://example.com/hidden-target).")]
        let reader = controller(records)
        let hit = try #require(ConversationMatcher.matches(records, query: "hidden-target").first)
        reader.update(sessionID: "reader", records: records, provider: "codex", findQuery: "hidden-target", findHit: hit, findGeneration: 1)
        let selected = try #require(reader.selectedFindRange)
        #expect((reader.measurement(at: 0).attributedBody.string as NSString).substring(with: selected) == "hidden-target")
    }

    @Test func richCodeUsesNativeContentAndFindReturnsToExactText() throws {
        let records = [record("code", "assistant", "```swift\nlet target = 42\n```\n")]
        let reader = controller(records)
        #expect(reader.measurement(at: 0).richContent != nil)
        let hit = try #require(ConversationMatcher.matches(records, query: "target").first)
        reader.update(sessionID: "reader", records: records, provider: "codex", findQuery: "target", findHit: hit, findGeneration: 1)
        #expect(reader.measurement(at: 0).richContent == nil)
        #expect(reader.selectedFindRange != nil)
    }

    @Test func routineLifecycleRowsAreHiddenButInterruptionsAndRawEventsRemain() throws {
        var completed = record("complete", "lifecycle", "Turn completed").record
        completed.lifecycleEvent = "task_complete"
        var aborted = record("abort", "lifecycle", "Turn interrupted").record
        aborted.lifecycleEvent = "turn_aborted"
        let records = [TranscriptRecord(recordID: "complete", record: completed), TranscriptRecord(recordID: "abort", record: aborted)]
        let reader = controller(records)
        #expect(reader.rows.count == 1)
        #expect(reader.measurement(at: 0).title == "Interrupted")
        #expect(reader.measurement(at: 0).hasFailure)
        let hit = try #require(ConversationMatcher.matches(records, query: "Turn completed").first)
        reader.update(sessionID: "reader", records: records, provider: "codex", findQuery: "Turn completed", findHit: hit, findGeneration: 1)
        #expect(reader.rows.count == 2)
        #expect(reader.selectedFindRange != nil)
        reader.update(sessionID: "reader", records: records, provider: "codex", rawTranscript: true)
        #expect(reader.rows.count == 2)
        #expect(reader.measurement(at: 0).body.contains("task_complete"))
    }

    @Test func returningToLongExpandedRawMessageRestoresDisplayStateAndOffset() {
        let navigation = TranscriptNavigationState()
        let records = [record("long", "assistant", String(repeating: "A line of retained content.\n", count: 180))]
        let reader = controller(records)
        reader.update(sessionID: "reader", records: records, provider: "codex", navigation: navigation)
        reader.toggleRaw("message:long")
        reader.toggleFullBody("message:long")
        reader.scrollView.contentView.scroll(to: NSPoint(x: 0, y: 500))
        reader.update(sessionID: "other", records: [record("other", "user", "Other")], provider: "codex", navigation: navigation)
        reader.update(sessionID: "reader", records: records, provider: "codex", navigation: navigation)
        #expect(reader.measurement(at: 0).showsFullBody)
        #expect(reader.measurement(at: 0).showsRaw)
        #expect(abs(reader.scrollView.contentView.bounds.minY - 500) < 1)
    }

    @Test func pagedToolResultsRefreshTheNativeRichView() {
        let call = TranscriptRecord(recordID: "call", record: Message(role: "tool_use", text: "", toolName: "bash", toolInput: #"{"command":"echo result"}"#, toolOutput: nil, eventID: "call"))
        let result = TranscriptRecord(recordID: "output", record: Message(role: "tool_result", text: "fresh output", toolName: "bash", toolInput: nil, toolOutput: nil, parentToolUseID: "call"))
        let reader = controller([call])
        reader.toggle("activity:call")
        let before = reader.measurement(at: 0).richContent
        #expect(before != nil)
        reader.update(sessionID: "reader", records: [call, result], provider: "codex")
        #expect(reader.measurement(at: 0).richContent !== before)
        #expect(reader.measurement(at: 0).body.contains("fresh output"))
    }

    @Test func groupReopenReusesToolViewButPagingWhileHiddenRefreshesIt() throws {
        let call = TranscriptRecord(recordID: "call", record: Message(role: "tool_use", text: "", toolName: "bash", toolInput: #"{"command":"echo result"}"#, toolOutput: nil, eventID: "call"))
        let other = TranscriptRecord(recordID: "other", record: Message(role: "tool_use", text: "", toolName: "bash", toolInput: #"{"command":"pwd"}"#, toolOutput: nil, eventID: "other"))
        let result = TranscriptRecord(recordID: "output", record: Message(role: "tool_result", text: "fresh output", toolName: "bash", toolInput: nil, toolOutput: nil, parentToolUseID: "call"))
        let reader = controller([call, other])
        let group = reader.rows[0].id
        reader.toggle(group)
        reader.toggle("activity:call")
        let before = try #require(reader.measurement(at: 1).richContent)
        reader.toggle(group)
        reader.toggle(group)
        #expect(reader.measurement(at: 1).richContent === before)
        reader.toggle(group)
        reader.update(sessionID: "reader", records: [call, other, result], provider: "codex")
        reader.toggle(group)
        #expect(reader.measurement(at: 1).richContent !== before)
        #expect(reader.measurement(at: 1).body.contains("fresh output"))
    }

    private func controller(_ records: [TranscriptRecord]) -> TranscriptController {
        let reader = TranscriptController()
        reader.view.frame = NSRect(x: 0, y: 0, width: 700, height: 500)
        reader.update(sessionID: "reader", records: records, provider: "codex")
        return reader
    }
    private func record(_ id: String, _ role: String, _ text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: role, text: text, toolName: nil, toolInput: nil, toolOutput: nil))
    }
}
