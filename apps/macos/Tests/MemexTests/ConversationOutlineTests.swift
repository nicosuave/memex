import Testing
@testable import Memex

@Suite @MainActor struct ConversationOutlineTests {
    @Test func omitsInjectedContextAndRetainsSourceOffsets() {
        let records = [record("context", "user", "<environment_context>private setup</environment_context>"),
                       record("a", "user", "<recommended_plugins>catalog</recommended_plugins>\nFix the reader"),
                       record("b", "assistant", "Working"), record("c", "user", "Then test it")]
        let entries = ConversationOutline.entries(records, offset: 20)
        #expect(entries.map(\.id) == ["a", "c"])
        #expect(entries.map(\.offset) == [21, 23])
        #expect(entries.first?.preview == "Fix the reader")
    }

    @Test func liveRefreshPreservesSelectionAndFollowsInterveningWork() {
        let records = [record("one", "user", "First request"), record("work", "assistant", "Working"),
                       record("two", "user", "Second request")]
        let outline = ConversationOutline()
        outline.load(records: records)
        outline.selectedID = "two"
        outline.load(records: records)
        #expect(outline.selectedID == "two")
        outline.follow("work")
        #expect(outline.selectedID == "one")
        outline.follow("two")
        #expect(outline.selectedID == "two")
        #expect(outline.prompts.map(\.offset) == [0, 2])
    }

    @Test func followsSourceIdentityAndBoundsLongPreview() {
        let source = record("source", "user", String(repeating: "long request ", count: 100))
        let projected = TranscriptRecord(recordID: "display", record: source.record, sourceRecordID: "source")
        let outline = ConversationOutline()
        outline.load(records: [projected])
        outline.follow("source")
        #expect(outline.selectedID == "display")
        #expect(outline.prompts.first?.preview.count == 180)
        #expect(outline.prompts.first?.id == "display")
    }

    @Test func repeatedRecordsKeepFirstIdentityAndOriginalPageOffsets() {
        let first = record("first", "user", "First")
        let second = record("second", "user", "Second")
        let initial = ConversationOutline.entries([first, first, second], offset: 10)
        #expect(initial.map(\.id) == ["first", "second"])
        #expect(initial.map(\.offset) == [10, 12])
        let next = ConversationOutline.entries([second, record("third", "user", "Third")],
                                               offset: 13, excludingIDs: Set(initial.map(\.id)))
        #expect(next.map(\.id) == ["third"])
        #expect(next.map(\.offset) == [14])
        let outline = ConversationOutline()
        outline.load(records: [first, second, first])
        outline.follow("first")
        #expect(outline.selectedID == "first")
        #expect(outline.prompts.count == 2)
    }

    @Test func railFitsTheExistingNarrowReadingMargin() {
        for width in [200.0, 400.0, 700.0, 1200.0] {
            #expect(4 + ConversationOutlineRail.railWidth <= ConversationReadingLane.origin(in: width))
        }
    }

    private func record(_ id: String, _ role: String, _ text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: role, text: text, toolName: nil, toolInput: nil, toolOutput: nil))
    }
}
