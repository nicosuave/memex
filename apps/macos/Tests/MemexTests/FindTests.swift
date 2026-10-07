import AppKit
import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct FindTests {
    @Test func recordedProviderCallAndResultFindStayFormattedWithoutRawNotice() throws {
        // Recorded functions.exec shape: input/output are repeated verbatim in
        // text, and the result contains nested JSON in provider text blocks.
        let command = #"text(await tools.exec_command({cmd:"printf 'MEMEX_UI_TOOL_OK\\n'","workdir":"/tmp/memex-live-resume-check","max_output_tokens":1000}));"# + "\n"
        let output = #"[{"type":"input_text","text":"Script completed\nWall time 0.2 seconds\nOutput:\n"},{"type":"input_text","text":"{\"chunk_id\":\"002b9f\",\"wall_time_seconds\":0.000012292,\"exit_code\":0,\"original_token_count\":5,\"output\":\"MEMEX_UI_TOOL_OK\\n\"}"}]"#
        let records = [
            entry("request", "Use your shell tool to print MEMEX_UI_TOOL_OK.", role: "user"),
            TranscriptRecord(recordID: "call", record: Message(role: "tool_use", text: command, toolName: "exec",
                toolInput: command, toolOutput: nil, eventID: "provider-call")),
            TranscriptRecord(recordID: "result", record: Message(role: "tool_result", text: output, toolName: "exec",
                toolInput: nil, toolOutput: output, eventID: "provider-result", parentToolUseID: "provider-call")),
        ]
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 400)
        let hits = ConversationMatcher.matches(records, query: "MEMEX_UI_TOOL_OK")
        #expect(hits.count == 3)
        for (index, hit) in hits.enumerated() {
            controller.update(sessionID: "recorded", records: records, provider: "codex",
                              findQuery: "MEMEX_UI_TOOL_OK", findHit: hit, findGeneration: index + 1)
            let row = try #require(controller.rows.firstIndex { row in
                if case .group = row { return false }
                return row.records.contains { $0.id == hit.recordID }
            })
            let value = controller.measurement(at: row)
            #expect(!value.showsRaw)
            #expect(!value.showsRawControl)
            let selection = try #require(controller.selectedFindRange)
            #expect((value.attributedBody.string as NSString).substring(with: selection) == "MEMEX_UI_TOOL_OK")
            if index > 0 {
                let occurrences = ConversationMatcher.ranges(in: value.attributedBody.string, query: "MEMEX_UI_TOOL_OK")
                #expect(selection == occurrences[index - 1])
                #expect(value.attributedBody.string.contains("output\nMEMEX_UI_TOOL_OK\nexit_code\n0"))
            }
            let cell = try #require(controller.table.view(atColumn: 0, row: row, makeIfNecessary: true))
            #expect(!cell.subviews.compactMap { $0 as? NSButton }.contains { !$0.isHidden && $0.title == "Raw content shown for Find" })
        }
        // A transport-only type has no formatted counterpart and must still
        // expose the source occurrence with an honest raw-content notice.
        let hidden = try #require(ConversationMatcher.matches(records, query: "input_text").first)
        controller.update(sessionID: "recorded", records: records, provider: "codex",
                          findQuery: "input_text", findHit: hidden, findGeneration: 4)
        let row = try #require(controller.rows.firstIndex { $0.id == "activity:call" })
        #expect(controller.measurement(at: row).showsRawControl)
        #expect(controller.measurement(at: row).attributedBody.string == output)
        #expect(controller.selectedFindRange == hidden.range)
        let cell = try #require(controller.table.view(atColumn: 0, row: row, makeIfNecessary: true))
        #expect(cell.subviews.compactMap { $0 as? NSButton }.contains { !$0.isHidden && $0.title == "Raw content shown for Find" })
    }

    @Test func visibleDuplicateBesideHiddenDestinationKeepsMarkdownAndExactSelection() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 400)
        let record = entry("duplicates", "[needle](https://example.com/needle) **needle**")
        let hits = ConversationMatcher.matches([record], query: "needle")
        #expect(hits.count == 3)
        for (generation, occurrence) in [0, 2, 1, 2].enumerated() {
            controller.update(sessionID: "duplicates", records: [record], provider: "codex",
                              findQuery: "needle", findHit: hits[occurrence], findGeneration: generation + 1)
            let value = controller.measurement(at: 0)
            let selected = try #require(controller.selectedFindRange)
            if occurrence == 1 {
                #expect(value.showsRaw)
                #expect(value.attributedBody.string == record.record.text)
                #expect(selected == hits[occurrence].range)
            } else {
                #expect(!value.showsRaw)
                #expect(value.attributedBody.string == "needle needle")
                #expect(selected == NSRange(location: occurrence == 0 ? 0 : 7, length: 6))
                if occurrence == 2 {
                    let font = try #require(value.attributedBody.attribute(.font, at: selected.location, effectiveRange: nil) as? NSFont)
                    #expect(NSFontManager.shared.traits(of: font).contains(.boldFontMask))
                }
            }
        }
    }

    @Test func toolFindMapsDuplicateSourceOccurrencesAfterFieldReordering() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 400)
        let source = #"{"chunk_id":"needle","exit_code":0,"output":"needle first\nneedle last"}"#
        let record = TranscriptRecord(recordID: "reordered", record: Message(role: "tool_result", text: "",
            toolName: "exec", toolInput: nil, toolOutput: source))
        let hits = ConversationMatcher.matches([record], query: "needle")
        for index in hits.indices {
            controller.update(sessionID: "reordered", records: [record], provider: "codex",
                              findQuery: "needle", findHit: hits[index], findGeneration: index + 1)
            let value = controller.measurement(at: 0)
            #expect(!value.showsRaw)
            #expect(value.attributedBody.string.hasPrefix("output\nneedle first\nneedle last"))
            let visible = ConversationMatcher.ranges(in: value.attributedBody.string, query: "needle")
            #expect(controller.selectedFindRange == visible[index == 0 ? 2 : index - 1])
        }
    }

    @Test func pairedIdenticalFieldsKeepRecordIdentityAndSourceEscapesFallbackPrecisely() throws {
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 400)
        let source = #"{"output":"needle\nneedle"}"#
        let records = ["call", "result"].enumerated().map { index, id in
            TranscriptRecord(recordID: id, record: Message(role: index == 0 ? "tool_use" : "tool_result", text: source,
                toolName: "exec", toolInput: index == 0 ? source : nil, toolOutput: index == 1 ? source : nil))
        }
        let hit = try #require(ConversationMatcher.matches(records, query: "needle").last)
        controller.update(sessionID: "paired", records: records, provider: "codex", findQuery: "needle", findHit: hit, findGeneration: 1)
        let value = controller.measurement(at: 0)
        #expect(!value.showsRaw)
        #expect(controller.selectedFindRange == ConversationMatcher.ranges(in: value.attributedBody.string, query: "needle").last)
        let escaped = try #require(ConversationMatcher.matches(records, query: #"\n"#).last)
        controller.update(sessionID: "paired", records: records, provider: "codex", findQuery: #"\n"#, findHit: escaped, findGeneration: 2)
        #expect(controller.measurement(at: 0).showsRaw)
        #expect(controller.measurement(at: 0).attributedBody.string == source)
        #expect(controller.selectedFindRange == escaped.range)
    }

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
