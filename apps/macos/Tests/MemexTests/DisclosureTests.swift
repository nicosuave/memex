import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct DisclosureTests {
    @Test(arguments: ["tool_use", "developer", "reasoning", "system"])
    func clickingDisclosureKeepsHeaderAndTrailingChevronStable(role: String) throws {
        let (controller, window) = makeReader()
        defer { window.close() }
        let target = record("target", role: role, text: String(repeating: "Long disclosure content.\n", count: 100))
        controller.update(sessionID: role, records: messages("before") + [target] + messages("after"), provider: "codex")
        settle(window)
        let id = try #require(controller.rows.first { $0.records.contains { $0.id == "target" } }?.id)
        try positionHeader(id, controller: controller, window: window)
        let original = try header(id, controller: controller)
        let titleRect = try #require(original.cell).titleRect(forBounds: original.bounds)
        let imageRect = try #require(original.cell).imageRect(forBounds: original.bounds)
        let originalY = original.convert(original.bounds, to: controller.scrollView).minY
        #expect(original.image != nil)
        #expect(!original.title.contains("›") && !original.title.contains("⌄"))
        #expect(imageRect.minX == titleRect.maxX + 6)
        #expect(imageRect.width == 10 && imageRect.height == 10)
        for _ in 0..<4 {
            try header(id, controller: controller).performClick(nil)
            settle(window)
            let button = try header(id, controller: controller)
            #expect(abs(button.convert(button.bounds, to: controller.scrollView).minY - originalY) < 1)
            #expect(button.cell?.titleRect(forBounds: button.bounds) == titleRect)
            #expect(button.cell?.imageRect(forBounds: button.bounds) == imageRect)
        }
    }

    @Test func groupAndNestedToolKeepTheirOwnHeaderPositions() throws {
        let (controller, window) = makeReader()
        defer { window.close() }
        let tools = [record("first", role: "tool_use", text: "echo first"),
                     record("second", role: "tool_use", text: String(repeating: "output\n", count: 100))]
        controller.update(sessionID: "group", records: messages("before") + tools + messages("after"), provider: "codex")
        settle(window)
        for id in ["group:first", "activity:first", "activity:second", "group:first"] {
            try positionHeader(id, controller: controller, window: window)
            let button = try header(id, controller: controller)
            let y = button.convert(button.bounds, to: controller.scrollView).minY
            button.performClick(nil)
            settle(window)
            let updated = try header(id, controller: controller)
            #expect(abs(updated.convert(updated.bounds, to: controller.scrollView).minY - y) < 1)
        }
    }

    @Test func rawSwitchKeepsHeaderAndDoesNotRequestPagesDuringReload() throws {
        let (controller, window) = makeReader()
        defer { window.close() }
        let tool = record("target", role: "tool_use", text: "{\"command\":\"echo hello\",\"timeout_ms\":1000}")
        var requests = 0
        controller.update(sessionID: "raw", records: messages("before") + [tool] + messages("after"),
                          provider: "codex", hasMore: true, onLoadMore: { requests += 1 })
        settle(window)
        controller.toggle("activity:target")
        try positionHeader("activity:target", controller: controller, window: window)
        let initial = try header("activity:target", controller: controller)
        let y = initial.convert(initial.bounds, to: controller.scrollView).minY
        for _ in 0..<4 {
            controller.toggleRaw("activity:target")
            settle(window)
            let button = try header("activity:target", controller: controller)
            #expect(abs(button.convert(button.bounds, to: controller.scrollView).minY - y) < 1)
        }
        #expect(requests == 0)
    }

    @Test func collapsingFinalToolRetainsHeaderThenReleasesTrailingSpaceOnScroll() throws {
        let (controller, window) = makeReader()
        defer { window.close() }
        let tool = record("final", role: "tool_use", text: String(repeating: "Last operation output.\n", count: 100))
        controller.update(sessionID: "end", records: messages("before") + [tool], provider: "codex")
        settle(window)
        controller.toggle("activity:final")
        try positionHeader("activity:final", controller: controller, window: window)
        let button = try header("activity:final", controller: controller)
        let y = button.convert(button.bounds, to: controller.scrollView).minY
        #expect(controller.scrollView.contentView.bounds.height >= 400)
        #expect(abs(y - 78) < 1)
        button.performClick(nil)
        settle(window)
        let collapsed = try header("activity:final", controller: controller)
        #expect(abs(collapsed.convert(collapsed.bounds, to: controller.scrollView).minY - y) < 1)
        let lastRow = controller.table.rect(ofRow: controller.rows.count - 1)
        #expect(controller.table.frame.height > lastRow.maxY)
        controller.scrollView.contentView.scroll(to: .zero)
        settle(window)
        #expect(abs(controller.table.frame.height - lastRow.maxY) < 1)
        controller.update(sessionID: "replacement", records: messages("replacement"), provider: "codex")
        settle(window)
        #expect(controller.table.minimumDocumentHeight == 0)
    }

    private func makeReader() -> (TranscriptController, NSWindow) {
        _ = NSApplication.shared
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 450)
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 450),
                              styleMask: [.titled, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = controller
        return (controller, window)
    }

    private func positionHeader(_ id: String, controller: TranscriptController, window: NSWindow) throws {
        let index = try #require(controller.rows.firstIndex { $0.id == id })
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: controller.table.rect(ofRow: index).minY - 70))
        settle(window)
    }

    private func header(_ id: String, controller: TranscriptController) throws -> NSButton {
        let index = try #require(controller.rows.firstIndex { $0.id == id })
        let cell = try #require(controller.table.view(atColumn: 0, row: index, makeIfNecessary: true))
        cell.layoutSubtreeIfNeeded()
        return try #require(cell.subviews.compactMap { $0 as? NSButton }.first { !$0.isHidden && $0.image != nil })
    }

    private func record(_ id: String, role: String = "assistant", text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: role, text: text,
            toolName: role == "tool_use" ? "exec" : nil, toolInput: nil, toolOutput: nil))
    }

    private func messages(_ prefix: String) -> [TranscriptRecord] {
        (0..<30).map { record("\(prefix)-\($0)", text: "Message \($0)") }
    }

    private func settle(_ window: NSWindow) {
        window.contentView?.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        let deadline = Date().addingTimeInterval(0.03)
        while Date() < deadline { RunLoop.main.run(mode: .default, before: deadline) }
    }
}
