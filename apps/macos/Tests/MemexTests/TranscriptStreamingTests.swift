import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct TranscriptStreamingTests {
    private func reply(_ id: String, _ text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: "assistant", text: text, toolName: nil, toolInput: nil, toolOutput: nil))
    }

    private func user(_ id: String, _ text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: "user", text: text, toolName: nil, toolInput: nil, toolOutput: nil))
    }

    private func window(_ controller: TranscriptController) -> NSWindow {
        _ = NSApplication.shared
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 600),
                              styleMask: [.titled, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = controller
        // Ordered offscreen so rows keep cells; never key, never visible to the reader.
        window.setFrameOrigin(NSPoint(x: -10_000, y: -10_000))
        window.orderBack(nil)
        return window
    }

    private func update(_ controller: TranscriptController, _ records: [TranscriptRecord], streaming: Bool) {
        controller.update(sessionID: "live", records: records, provider: "codex", startsAtEnd: true,
                          followLatest: true, isStreaming: streaming)
    }

    private var reducesMotion: Bool {
        NSWorkspace.shared.accessibilityDisplayShouldReduceMotion || AppPreferences.shared.reduceMotion
    }

    @Test func streamedRowMatchesTheFullRenderAndIsKeptWhenTheTurnEnds() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { window.close() }
        let full = "Start of the answer.\n\n- one\n- two\n\n```swift\nlet a = 1\n```\n\nThe **end** of `it`."
        var records = [user("u", "Question"), reply("a", "S")]
        update(controller, records, streaming: true)
        for end in Array(stride(from: 2, through: full.count, by: 3)) + [full.count] {
            records[1] = reply("a", String(full.prefix(end)))
            update(controller, records, streaming: true)
        }
        controller.view.layoutSubtreeIfNeeded()
        // Without a visible window nothing is paced: everything received is shown.
        #expect(controller.streamingRevealedText == full)
        let streamed = controller.measurement(at: 1)
        #expect(streamed.textRevision != nil)
        let reference = TranscriptController()
        reference.view.frame = NSRect(x: 0, y: 0, width: 700, height: 600)
        reference.update(sessionID: "reference", records: records, provider: "codex")
        let expected = reference.measurement(at: 1)
        #expect(equivalent(streamed.attributedBody, expected.attributedBody))
        #expect(streamed.height == expected.height)
        let streamedRich = try #require(streamed.richLayout)
        #expect(streamedRich.items.count == expected.richLayout?.items.count)

        update(controller, records, streaming: false)
        let finished = controller.measurement(at: 1)
        #expect(finished.textRevision == nil)
        // Identical content is not replaced: the same text and views stay up.
        #expect(finished.attributedBody === streamed.attributedBody)
        #expect(finished.richLayout === streamedRich)
        #expect(finished.height == expected.height)
    }

    @Test func revealIsPacedAndFlushedWhenTheNextRowArrives() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { window.close() }
        controller.pacesRevealOffscreen = true
        var records = [user("u", "Question"), reply("a", "Hello")]
        update(controller, records, streaming: true)
        // Text present when the row first appears is shown at once.
        #expect(controller.streamingRevealedText == "Hello")
        let full = "Hello there, this streamed reply grows in one large burst of words."
        records[1] = reply("a", full)
        update(controller, records, streaming: true)
        if reducesMotion {
            #expect(controller.streamingRevealedText == full)
            return
        }
        #expect(controller.streamingRevealedText == "Hello")
        #expect(controller.measurement(at: 1).attributedBody.string == "Hello")
        controller.advanceStreamingReveal(at: CACurrentMediaTime() + 0.1)
        let partial = try #require(controller.streamingRevealedText)
        #expect(partial.count > 5 && partial.count < full.count && full.hasPrefix(partial))
        #expect(partial.last == " ")
        #expect(controller.measurement(at: 1).attributedBody.string == partial.trimmingCharacters(in: .whitespaces))
        controller.advanceStreamingReveal(at: CACurrentMediaTime() + 0.5)
        #expect(controller.streamingRevealedText == full)

        records[1] = reply("a", full + " Then more text arrives just before a new row.")
        update(controller, records, streaming: true)
        #expect(controller.streamingRevealedText == full)
        records.append(user("u2", "Next question"))
        update(controller, records, streaming: true)
        #expect(controller.streamingRevealedText == nil)
        #expect(controller.measurement(at: 1).attributedBody.string.hasSuffix("just before a new row."))
    }

    @Test func selectingInTheStreamingRowShowsAllOfIt() throws {
        let controller = TranscriptController()
        let window = window(controller)
        defer { controller.selectionActions.dismiss(); window.close() }
        controller.pacesRevealOffscreen = true
        var records = [reply("a", "Hello")]
        update(controller, records, streaming: true)
        let full = "Hello there, the rest of this reply is still being revealed."
        records[0] = reply("a", full)
        update(controller, records, streaming: true)
        controller.view.layoutSubtreeIfNeeded()
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true) as? TranscriptSelectionTarget)
        cell.dismissSelectionActions()
        #expect(controller.streamingRevealedText == full)
        records[0] = reply("a", full + " More arrives after the selection began.")
        update(controller, records, streaming: true)
        #expect(controller.streamingRevealedText == full + " More arrives after the selection began.")
    }

    @Test func growthNeverDetachesButTheReaderDoes() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 300)
        controller.view.layoutSubtreeIfNeeded()
        var changes: [Bool] = []
        controller.onFollowStateChange = { changes.append($0) }
        var records = (0..<20).map { reply("\($0)", "Message \($0)") }
        update(controller, records, streaming: true)
        func atEnd() -> Bool {
            abs(controller.table.rect(ofRow: controller.rows.count - 1).maxY - controller.scrollView.contentView.bounds.maxY) < 1
        }
        #expect(controller.isFollowingLatest && atEnd())
        for count in 1...4 {
            records[19] = reply("19", "Message 19" + String(repeating: "\n\nMore of the reply.", count: count))
            update(controller, records, streaming: true)
            #expect(controller.isFollowingLatest && atEnd())
        }
        // The reader scrolls up a little, beyond the tolerance.
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: controller.scrollView.contentView.bounds.minY - 30))
        #expect(!controller.isFollowingLatest)
        let reading = controller.scrollView.contentView.bounds.minY
        records[19] = reply("19", records[19].record.text + "\n\nEven more while the reader reads.")
        update(controller, records, streaming: true)
        #expect(abs(controller.scrollView.contentView.bounds.minY - reading) < 1)
        #expect(!controller.isFollowingLatest)
        // Returning to the end resumes following.
        let end = controller.table.rect(ofRow: controller.rows.count - 1).maxY - controller.scrollView.contentView.bounds.height
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: end))
        #expect(controller.isFollowingLatest)
        records[19] = reply("19", records[19].record.text + "\n\nAnd the end again.")
        update(controller, records, streaming: true)
        #expect(atEnd())
        #expect(changes == [false, true])
    }

    @Test func aGestureInProgressIsNotFoughtAndJumpToLatestReattaches() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 300)
        controller.view.layoutSubtreeIfNeeded()
        var records = (0..<20).map { reply("\($0)", "Message \($0)") }
        update(controller, records, streaming: true)
        NotificationCenter.default.post(name: NSScrollView.willStartLiveScrollNotification, object: controller.scrollView)
        let before = controller.scrollView.contentView.bounds.minY
        records[19] = reply("19", "Message 19\n\nGrowing during the gesture.\n\nAnd more.")
        update(controller, records, streaming: true)
        #expect(abs(controller.scrollView.contentView.bounds.minY - before) < 1)
        NotificationCenter.default.post(name: NSScrollView.didEndLiveScrollNotification, object: controller.scrollView)
        #expect(!controller.isFollowingLatest)
        controller.jumpToLatest()
        #expect(controller.isFollowingLatest)
        let end = controller.table.rect(ofRow: controller.rows.count - 1).maxY
        #expect(abs(end - controller.scrollView.contentView.bounds.maxY) < 1)
    }

    @Test func nonStreamingUpdatesDoNotCreateAStream() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 300)
        var records = [reply("a", "Hello")]
        update(controller, records, streaming: false)
        records[0] = reply("a", "Hello and more")
        update(controller, records, streaming: false)
        #expect(controller.streamingRevealedText == nil)
        #expect(controller.measurement(at: 0).textRevision == nil)
    }
}
