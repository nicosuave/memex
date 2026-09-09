import AppKit
import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct FindTests {
    @Test func literalMatchesPreserveMultipleOccurrencesAndFullToolContent() {
        let record = entry("tool", String(repeating: "padding ", count: 50_000) + "Needle [x] needle [x]", role: "tool_use")
        let hits = ConversationMatcher.matches([record], query: "NEEDLE [x]")
        #expect(hits.count == 2)
        #expect(hits.map(\.occurrence) == [0, 1])
        #expect(hits[0].range.location == 400_000)
    }

    @Test func navigationRevealsCollapsedPairAndExactOccurrenceThenClearsHighlights() throws {
        let controller = TranscriptController()
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 400),
                              styleMask: [.titled], backing: .buffered, defer: false)
        window.contentViewController = controller
        let records = [entry("call", "needle input", role: "tool_use"),
                       entry("output", "needle first\n" + String(repeating: "padding\n", count: 400) + "needle last", role: "tool_result"),
                       entry("next", "another tool", role: "tool_use")]
        let hit = ConversationMatcher.matches([records[1]], query: "needle")[1]
        controller.update(sessionID: "a", records: records, provider: "codex", findQuery: "needle", findHit: hit, findGeneration: 1)
        window.contentView?.layoutSubtreeIfNeeded()
        let row = try #require(controller.rows.firstIndex { $0.id == "activity:call" })
        let value = controller.measurement(at: row)
        #expect(value.body.contains("needle last"))
        let selected = try #require(controller.selectedFindRange)
        #expect(selected == ConversationMatcher.ranges(in: value.attributedBody.string, query: "needle").last)
        #expect(value.attributedBody.attribute(.backgroundColor, at: selected.location, effectiveRange: nil) != nil)
        #expect(!window.isVisible)
        #expect(controller.scrollView.contentView.bounds.minY > 500)
        controller.update(sessionID: "a", records: records, provider: "codex", findGeneration: 2)
        #expect(controller.selectedFindRange == nil)
        #expect(controller.measurement(at: row).attributedBody.attribute(.backgroundColor, at: selected.location, effectiveRange: nil) == nil)
    }

    @Test func repeatedNavigationRetainsUnaffectedRenderedBodies() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 400)
        let records = [entry("first", "needle first needle"), entry("second", "**Unchanged** message")]
        let hits = ConversationMatcher.matches(records, query: "needle")
        controller.update(sessionID: "reuse", records: records, provider: "codex",
                          findQuery: "needle", findHit: hits[0], findGeneration: 1)
        let before = controller.measurement(at: 1).attributedBody
        controller.update(sessionID: "reuse", records: records, provider: "codex",
                          findQuery: "needle", findHit: hits[1], findGeneration: 2)
        #expect(controller.measurement(at: 1).attributedBody === before)
        #expect(controller.selectedFindRange?.location == 13)
    }

    @Test func hiddenMarkdownSourceMatchRevealsAndSelectsItsOriginalOccurrence() {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 400)
        let record = entry("link", "[label](https://example.com/needle) needle")
        let hit = ConversationMatcher.matches([record], query: "needle")[0]
        controller.update(sessionID: "link", records: [record], provider: "codex",
                          findQuery: "needle", findHit: hit, findGeneration: 1)
        #expect(controller.measurement(at: 0).attributedBody.string == record.record.text)
        #expect(controller.selectedFindRange == NSRange(location: 28, length: 6))
    }

    @Test func scansBeyondFirstPageAndIsolatesCancelledQueryAndSession() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let executable = directory.appendingPathComponent("fixture-cli")
        let first: [[String: Any]] = (0..<60).map { ["record_id": "\($0)", "record": ["role": "assistant", "text": "early needle"]] }
        let later: [[String: Any]] = [["record_id": "later", "record": ["role": "tool_result", "text": "late NEEDLE needle"]]]
        try JSONSerialization.data(withJSONObject: first).write(to: directory.appendingPathComponent("first.json"))
        try JSONSerialization.data(withJSONObject: later).write(to: directory.appendingPathComponent("later.json"))
        let script = #"""
        #!/bin/sh
        base=$(dirname "$0")
        offset=0
        while [ "$#" -gt 0 ]; do
          case "$1" in
            --offset) shift; offset="$1" ;;
            --) shift; session="$1"; break ;;
          esac
          shift
        done
        if [ "$session" = slow ]; then
          touch "$base/slow-started"
          sleep 20
        fi
        if [ "$session" = other ]; then printf '[]'; exit; fi
        if [ "$offset" = 0 ]; then
          cat "$base/first.json"
        else
          touch "$base/later-requested"
          while [ ! -f "$base/release-later" ]; do sleep 0.05; done
          cat "$base/later.json"
        fi
        """#
        try script.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        let state = ConversationFindState(client: MemexClient(executable: executable))
        state.isOpen = true
        state.query = "needle"
        state.search(in: session("a"))
        try await wait { state.hits.count == 60 }
        #expect(state.isScanning)
        try await wait { FileManager.default.fileExists(atPath: directory.appendingPathComponent("later-requested").path) }
        try Data().write(to: directory.appendingPathComponent("release-later"))
        try await wait { !state.isScanning }
        #expect(state.hits.count == 62)
        #expect(state.hits.last?.recordID == "later")
        state.move(-1)
        #expect(state.selectedIndex == 61)
        state.move(1)
        #expect(state.selectedIndex == 0)
        state.search(in: session("slow"))
        try await wait { FileManager.default.fileExists(atPath: directory.appendingPathComponent("slow-started").path) }
        state.query = "late"
        state.search(in: session("a"))
        try await wait { !state.isScanning }
        #expect(state.hits.map(\.recordID) == ["later"])
        state.search(in: session("other"))
        try await wait { !state.isScanning }
        #expect(state.hits.isEmpty)
        state.close()
        #expect(state.query.isEmpty && !state.isOpen && state.selectedHit == nil)
    }

    private func wait(_ condition: () -> Bool) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(20))
        while !condition(), ContinuousClock.now < deadline { try await Task.sleep(for: .milliseconds(10)) }
        try #require(condition())
    }
    private func session(_ id: String) -> Session {
        Session(source: "codex", sessionID: id, sourcePath: "/fixture", project: "/project")
    }
    private func entry(_ id: String, _ text: String, role: String = "assistant") -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: role, text: text, toolName: "exec", toolInput: nil, toolOutput: nil))
    }
}
