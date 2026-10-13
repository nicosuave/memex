import Foundation
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

    @Test func liveUpdatesReprojectOnlyTheChangedTail() {
        var records = [record("one", "user", "First request"), record("work", "assistant", "Working")]
        let outline = ConversationOutline()
        outline.load(records: records)
        outline.selectedID = "one"
        records[1] = record("work", "assistant", "Working harder")
        records.append(record("two", "user", "Second request"))
        outline.load(records: records)
        #expect(outline.prompts.map(\.id) == ["one", "two"])
        #expect(outline.selectedID == "one")
        records[2] = record("two", "user", "Second request, edited")
        outline.load(records: records)
        #expect(outline.prompts.last?.preview == "Second request, edited")
        outline.reset()
        #expect(outline.prompts.isEmpty && outline.selectedID == nil)
    }

    @Test func historyWindowsMergeByOffsetWithoutRescanningCoveredRecords() {
        let outline = ConversationOutline()
        outline.absorb(records: [record("late", "user", "Late request"), record("reply", "assistant", "Reply")], offset: 120)
        outline.absorb(records: [record("early", "user", "Early request")], offset: 3)
        #expect(outline.prompts.map(\.id) == ["early", "late"])
        #expect(outline.prompts.map(\.offset) == [3, 120])
        // An earlier page prepended to the window: only new offsets are projected.
        outline.absorb(records: [record("mid", "user", "Middle"), record("late", "user", "Late request"),
                                 record("reply", "assistant", "Reply")], offset: 119)
        #expect(outline.prompts.map(\.id) == ["early", "mid", "late"])
        outline.follow("reply")
        #expect(outline.selectedID == "late")
    }

    @Test func onDemandScanSkipsPagesTheReaderAlreadyLoaded() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let pageSize = MemexClient.pageSize
        let first: [[String: Any]] = (0..<pageSize).map { ["record_id": "r\($0)", "record": ["role": "user", "text": "Prompt \($0)"]] }
        try JSONSerialization.data(withJSONObject: first).write(to: directory.appendingPathComponent("page-0.json"))
        let executable = directory.appendingPathComponent("fixture-cli")
        let script = #"""
        #!/bin/sh
        base=$(dirname "$0")
        while [ "$#" -gt 0 ]; do
          case "$1" in --offset) shift; offset="$1" ;; esac
          shift
        done
        echo "$offset" >> "$base/requested"
        if [ -f "$base/page-$offset.json" ]; then cat "$base/page-$offset.json"; else printf '[]'; fi
        """#
        try script.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        let outline = ConversationOutline()
        // The reader already holds the second page.
        outline.absorb(records: (0..<pageSize).map { record("s\($0)", "user", "Loaded \($0)") }, offset: pageSize)
        let session = Session(source: "codex", sessionID: "outline", sourcePath: "/fixture", project: "/project")
        outline.completeIfNeeded(session: session, client: MemexClient(executable: executable))
        let deadline = ContinuousClock.now.advanced(by: .seconds(20))
        while outline.scanning, ContinuousClock.now < deadline { try await Task.sleep(for: .milliseconds(10)) }
        #expect(!outline.scanning)
        #expect(outline.prompts.count == 2 * pageSize)
        let requested = try String(contentsOf: directory.appendingPathComponent("requested"), encoding: .utf8)
            .split(separator: "\n").map(String.init)
        #expect(requested == ["0", String(2 * pageSize)])
        outline.completeIfNeeded(session: session, client: MemexClient(executable: executable))
        #expect(!outline.scanning) // A complete outline is never rescanned.
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
