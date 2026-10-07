import AppKit
import Testing
@testable import Memex

@MainActor @Suite(.serialized) struct ConversationListTests {
    @Test func narrowSearchExcerptKeepsTheMatchNearItsStartWithoutChangingSource() {
        let prefix = String(repeating: "context ", count: 10)
        let snippet = prefix + "MEMEX_UI_TOOL_OK in the original result"
        let excerpt = ConversationExcerpt.text(snippet, query: "\"MEMEX_UI_TOOL_OK\"")
        #expect(excerpt.hasPrefix("…"))
        #expect(excerpt.prefix(40).contains("MEMEX_UI_TOOL_OK"))
        #expect(excerpt.hasSuffix("in the original result"))
        #expect(ConversationExcerpt.text(snippet, query: "absent") == snippet)
        #expect(ConversationExcerpt.text("🧪 Café café context needle", query: "needle").contains("needle"))
    }

    @Test func retainedStateRefreshesWithoutChangingSelectionAndSearchPreviewWraps() throws {
        let controller = ConversationListController()
        let session = Session(source: "codex", sessionID: "s", sourcePath: "/s", project: "memex", label: "The conversation title",
                              snippet: "The matching passage remains visible across two lines", searchRecordID: "record")
        controller.update(sessions: [session], selectedID: session.id, select: { _ in }, loadMore: { _ in })
        let initialHeight = controller.tableView(controller.table, heightOfRow: 0)
        let state = ConversationListState(activity: .approval, hasDraft: true)
        controller.update(sessions: [session], selectedID: session.id, states: [session.id: state], select: { _ in }, loadMore: { _ in })
        #expect(controller.rows[0].state == state)
        #expect(controller.table.selectedRow == 0)
        let height = controller.tableView(controller.table, heightOfRow: 0)
        #expect(height == initialHeight)
        let cell = ConversationCell()
        cell.frame = NSRect(x: 0, y: 0, width: 280, height: height)
        cell.configure(controller.rows[0])
        cell.layoutSubtreeIfNeeded()
        let fields = cell.subviews.compactMap { $0 as? NSTextField }.filter { !$0.isHidden }
        let title = try #require(fields.first { $0.stringValue == session.title })
        let metadata = try #require(fields.first { $0.stringValue == "memex · codex" })
        let preview = try #require(fields.first { $0.stringValue == session.snippet })
        #expect(title.maximumNumberOfLines == 2)
        #expect(title.cell?.wraps == true)
        #expect(title.frame.maxY <= metadata.frame.minY)
        #expect(preview.maximumNumberOfLines == 2)
        #expect(fields.allSatisfy { $0.frame.maxY <= height })
        #expect(cell.accessibilityLabel()?.contains("Approval needed") == true)
        #expect(cell.accessibilityLabel()?.contains("Draft") == false)
        let icons = cell.subviews.compactMap { $0 as? NSImageView }.filter { !$0.isHidden }
        #expect(icons.count == 1)
        #expect(icons.contains { $0.toolTip == "Approval needed" })
    }

    @Test func nativeCellsShowSubagentsAndKeepMetadataInsideTheRow() throws {
        let controller = ConversationListController()
        let cell = ConversationCell()
        for (machine, kind, expected) in [
            ("local", "subagent", "Subagent"),
            ("nicbook-atm", "subagent", "nicbook-atm · Subagent"),
            ("nicbook-atm", "main", "nicbook-atm"),
            ("local", "main", "")
        ] {
            let session = Session(source: "codex", sessionID: "s", sourcePath: "/s", project: "memex",
                                  machine: machine, conversationKind: kind)
            controller.update(sessions: [session], selectedID: nil, select: { _ in }, loadMore: { _ in })
            let height = controller.tableView(controller.table, heightOfRow: 0)
            cell.frame = NSRect(x: 0, y: 0, width: 300, height: height)
            cell.configure(controller.rows[0])
            cell.layoutSubtreeIfNeeded()
            let visibleLabels = cell.subviews.compactMap { $0 as? NSTextField }.filter { !$0.isHidden }
            #expect(visibleLabels.allSatisfy { $0.frame.maxY <= height })
            let metadata = "memex · codex" + (expected.isEmpty ? "" : " · \(expected)")
            #expect(visibleLabels.contains { $0.stringValue == metadata && $0.toolTip == metadata })
            #expect(cell.accessibilityLabel()?.contains("Subagent") == (kind == "subagent"))
        }
    }

    @Test func activityAndDraftChangesKeepRowGeometryAndHelp() throws {
        let session = sessions(1)[0]
        let cell = ConversationCell()
        let baseline = ConversationListController.Row(session)
        cell.frame = NSRect(x: 0, y: 0, width: 260, height: baseline.height)
        cell.configure(baseline)
        cell.layoutSubtreeIfNeeded()
        let initialFrames = cell.subviews.map(\.frame)
        let activities: [ConversationActivity?] = [nil, .starting, .working, .stopping, .approval, .question,
                                                  .failed, .completed, .stopped, .openElsewhere]
        for activity in activities {
            for hasDraft in [false, true] {
                let state = ConversationListState(activity: activity, hasDraft: hasDraft)
                let row = ConversationListController.Row(session, state: state)
                cell.configure(row)
                cell.layoutSubtreeIfNeeded()
                #expect(row.height == baseline.height)
                #expect(cell.subviews.map(\.frame) == initialFrames)
                #expect(cell.toolTip == activity?.label)
                #expect(state.label == activity?.label)
                #expect(cell.accessibilityLabel()?.contains("Draft") == false)
                let icons = cell.subviews.compactMap { $0 as? NSImageView }.filter { !$0.isHidden }
                #expect(icons.count == (activity == nil ? 0 : 1))
                if let label = state.label { #expect(cell.accessibilityLabel()?.contains(label) == true) }
            }
        }
        // A provider belongs in metadata; it should not consume a preview line.
        var withoutSnippet = session
        withoutSnippet.snippet = nil
        let compact = ConversationListController.Row(withoutSnippet)
        #expect(compact.preview.isEmpty)
        #expect(compact.height < baseline.height)
    }

    func sessions(_ count: Int) -> [Session] {
        (0..<count).map { Session(source: "codex", sessionID: "s\($0)", sourcePath: "/s\($0)", project: "memex", label: "Conversation \($0) with enough text to wrap onto another line", lastAt: "2026-09-07T12:00:00Z", snippet: "A two-line preview of this conversation.") }
    }
    @Test func largeListKeepsVisibleCellsAndScrollPositionWhenAppending() async throws {
        let controller = ConversationListController()
        let window = NSWindow(contentRect: NSRect(x: -10000, y: -10000, width: 300, height: 600), styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.alphaValue = 0
        window.contentViewController = controller
        window.setContentSize(NSSize(width: 300, height: 600))
        defer { window.close() }
        let initial = sessions(1000)
        controller.update(sessions: initial, selectedID: initial[5].id, select: { _ in }, loadMore: { _ in })
        window.orderBack(nil)
        await Task.yield()
        window.contentView?.layoutSubtreeIfNeeded()
        controller.table.scrollRowToVisible(950)
        controller.table.layoutSubtreeIfNeeded()
        let y = controller.scrollView.contentView.bounds.minY
        let visible = controller.table.rows(in: controller.table.visibleRect)
        #expect(visible.length > 0 && visible.length < 16)
        let row = visible.location
        let cell = try #require(controller.table.view(atColumn: 0, row: row, makeIfNecessary: true))
        controller.update(sessions: sessions(2000), selectedID: initial[5].id, select: { _ in }, loadMore: { _ in })
        controller.table.layoutSubtreeIfNeeded()
        #expect(controller.table.numberOfRows == 2000)
        #expect(controller.table.selectedRow == 5)
        #expect(abs(controller.scrollView.contentView.bounds.minY - y) < 1)
        #expect(controller.table.view(atColumn: 0, row: row, makeIfNecessary: false) === cell)
        let realized = (0..<2000).filter { controller.table.view(atColumn: 0, row: $0, makeIfNecessary: false) != nil }
        #expect(realized.count < 40)
        controller.update(sessions: sessions(2000), selectedID: initial[5].id,
                          states: [initial[0].id: .init(activity: .working),
                                   initial[row].id: .init(activity: .approval, hasDraft: true)],
                          select: { _ in }, loadMore: { _ in })
        controller.table.layoutSubtreeIfNeeded()
        #expect(abs(controller.scrollView.contentView.bounds.minY - y) < 1)
        #expect(controller.table.selectedRow == 5)
    }
    @Test func selectionPagingAndReplacementKeepExactIdentity() async throws {
        let controller = ConversationListController()
        controller.loadViewIfNeeded()
        controller.view.frame = NSRect(x: 0, y: 0, width: 300, height: 600)
        var selected: String?
        var requested: [String] = []
        var rows = sessions(1000)
        controller.update(sessions: rows, selectedID: rows[2].id, select: { selected = $0 }, loadMore: { requested.append($0) })
        controller.view.layoutSubtreeIfNeeded()
        #expect(selected == nil)
        controller.table.selectRowIndexes(IndexSet(integer: 500), byExtendingSelection: false)
        #expect(selected == rows[500].id)
        controller.table.scrollRowToVisible(999)
        NotificationCenter.default.post(name: NSView.boundsDidChangeNotification, object: controller.scrollView.contentView)
        #expect(requested.last == rows[999].id)
        rows[500].label = "Hydrated title"
        controller.update(sessions: rows, selectedID: rows[500].id, select: { selected = $0 }, loadMore: { _ in })
        #expect(controller.rows[500].title == "Hydrated title")
        #expect(controller.table.selectedRow == 500)
        controller.update(sessions: [rows[700], rows[500]], selectedID: rows[500].id, select: { selected = $0 }, loadMore: { _ in })
        #expect(controller.table.numberOfRows == 2)
        #expect(controller.table.selectedRow == 1)
        controller.update(sessions: [], selectedID: nil, select: { selected = $0 }, loadMore: { _ in })
        #expect(controller.table.numberOfRows == 0)
        #expect(controller.table.selectedRow == -1)
    }
}
