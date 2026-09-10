import AppKit
import Testing
@testable import Memex

/// Opt-in profiling of a captured `memex session --full --format json` page. Reports
/// counts and timings only; source content and record identifiers never enter logs.
@Suite(.serialized) @MainActor struct ReaderPerformanceTests {
    @Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_READER_PERF_INPUT"] != nil))
    func capturedPageDecodeAndNativeLayout() throws {
        let path = try #require(ProcessInfo.processInfo.environment["MEMEX_READER_PERF_INPUT"])
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        _ = NSApplication.shared
        for iteration in 0..<3 {
            let decodeStart = ContinuousClock.now
            let records = try JSONDecoder().decode([TranscriptRecord].self, from: data)
            let decodeMS = milliseconds(since: decodeStart)
            #expect(!records.isEmpty)

            let reader = TranscriptController()
            let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 800, height: 700),
                                  styleMask: [.titled, .resizable], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            window.contentViewController = reader
            window.contentView?.layoutSubtreeIfNeeded()
            defer { window.close() }

            let updateStart = ContinuousClock.now
            reader.update(sessionID: "performance", records: records, provider: "codex", startsAtEnd: true)
            let updateMS = milliseconds(since: updateStart)
            let layoutStart = ContinuousClock.now
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            let layoutMS = milliseconds(since: layoutStart)
            #expect(!reader.rows.isEmpty)

            let warmStart = ContinuousClock.now
            reader.update(sessionID: "performance", records: records, provider: "codex", startsAtEnd: true)
            window.contentView?.layoutSubtreeIfNeeded()
            let warmMS = milliseconds(since: warmStart)

            print(String(format: "reader_perf iteration=%d bytes=%d records=%d rows=%d decode_ms=%.2f update_ms=%.2f layout_ms=%.2f render_total_ms=%.2f warm_update_ms=%.2f",
                         iteration, data.count, records.count, reader.rows.count, decodeMS, updateMS, layoutMS, updateMS + layoutMS, warmMS))
        }
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_READER_PERF_INPUT"] != nil))
    func capturedPageLargestToolDisclosure() throws {
        let path = try #require(ProcessInfo.processInfo.environment["MEMEX_READER_PERF_INPUT"])
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        let records = try JSONDecoder().decode([TranscriptRecord].self, from: data)
        let activities = TranscriptItem.group(records).flatMap(\.activities).filter {
            $0.records.contains { ["tool_use", "tool_result", "tool"].contains($0.record.role) }
        }
        func bytes(_ activity: TranscriptActivity) -> Int {
            activity.records.reduce(0) { count, entry in
                count + entry.record.text.utf8.count + (entry.record.toolInput?.utf8.count ?? 0)
                    + (entry.record.toolOutput?.utf8.count ?? 0)
            }
        }
        let largest = try #require(activities.max { bytes($0) < bytes($1) })
        _ = NSApplication.shared
        let reader = TranscriptController()
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 800, height: 700),
                              styleMask: [.titled, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentViewController = reader
        defer { window.close() }
        window.contentView?.layoutSubtreeIfNeeded()
        reader.update(sessionID: "disclosure-performance", records: records, provider: "codex", startsAtEnd: true)
        window.contentView?.layoutSubtreeIfNeeded()

        func timedToggle(_ id: String, phase: String) {
            let start = ContinuousClock.now
            reader.toggle(id)
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            let elapsed = milliseconds(since: start)
            print(String(format: "reader_disclosure_perf phase=%@ bytes=%d records=%d rows=%d elapsed_ms=%.2f",
                         phase, bytes(largest), largest.records.count, reader.rows.count, elapsed))
        }
        if let groupIndex = reader.rows.firstIndex(where: { row in
            guard case .group = row else { return false }
            return row.records.contains { $0.id == largest.records[0].id }
        }) {
            let groupID = reader.rows[groupIndex].id
            reader.table.scrollRowToVisible(groupIndex)
            window.contentView?.layoutSubtreeIfNeeded()
            timedToggle(groupID, phase: "group_open")
            timedToggle(groupID, phase: "group_close")
            timedToggle(groupID, phase: "group_reopen")
        }
        let toolID = "activity:\(largest.id)"
        let toolIndex = try #require(reader.rows.firstIndex { $0.id == toolID })
        reader.table.scrollRowToVisible(toolIndex)
        window.contentView?.layoutSubtreeIfNeeded()
        timedToggle(toolID, phase: "tool_first_open")
        #expect(reader.measurement(at: toolIndex).isExpanded)
        timedToggle(toolID, phase: "tool_close")
        #expect(!reader.measurement(at: toolIndex).isExpanded)
        timedToggle(toolID, phase: "tool_reopen")
        #expect(reader.measurement(at: toolIndex).isExpanded)
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_READER_PERF_INPUT"] != nil))
    func capturedPagePrependLatency() throws {
        let path = try #require(ProcessInfo.processInfo.environment["MEMEX_READER_PERF_INPUT"])
        let data = try Data(contentsOf: URL(fileURLWithPath: path))
        let decodeStart = ContinuousClock.now
        let records = try JSONDecoder().decode([TranscriptRecord].self, from: data)
        print(String(format: "pagination_decode bytes=%d records=%d ms=%.2f", data.count, records.count, milliseconds(since: decodeStart)))
        #expect(records.count > 60)
        let projectionCosts = records.map { record in
            let start = ContinuousClock.now
            _ = TranscriptPresentation.project([record])
            return (record.record.role, record.record.text.utf8.count, milliseconds(since: start))
        }
        for (role, bytes, ms) in projectionCosts.sorted(by: { $0.2 > $1.2 }).prefix(5) {
            print(String(format: "pagination_projection role=%@ bytes=%d ms=%.2f", role, bytes, ms))
        }
        _ = NSApplication.shared
        for iteration in 0..<3 {
            let reader = TranscriptController()
            let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 800, height: 700),
                styleMask: [.titled, .resizable], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            window.contentViewController = reader
            defer { window.close() }
            window.contentView?.layoutSubtreeIfNeeded()
            reader.update(sessionID: "pagination", records: Array(records.suffix(60)), provider: "codex", hasEarlier: true)
            window.contentView?.layoutSubtreeIfNeeded()
            reader.scrollView.contentView.scroll(to: .zero)
            let projectStart = ContinuousClock.now
            let projected = TranscriptPresentation.project(records)
            let projectMS = milliseconds(since: projectStart)
            let consecutiveStart = ContinuousClock.now
            _ = TranscriptItem.groupConsecutive(projected)
            let consecutiveMS = milliseconds(since: consecutiveStart)
            let groupStart = ContinuousClock.now
            _ = TranscriptItem.group(records)
            let groupingMS = milliseconds(since: groupStart)
            let start = ContinuousClock.now
            reader.update(sessionID: "pagination", records: records, provider: "codex")
            let updateMS = milliseconds(since: start)
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
            print(String(format: "pagination_prepend iteration=%d project_ms=%.2f consecutive_ms=%.2f group_ms=%.2f update_ms=%.2f total_ms=%.2f rows=%d",
                iteration, projectMS, consecutiveMS, groupingMS, updateMS, milliseconds(since: start), reader.rows.count))
        }
    }

    private func milliseconds(since start: ContinuousClock.Instant) -> Double {
        let duration = start.duration(to: .now).components
        return Double(duration.seconds) * 1_000 + Double(duration.attoseconds) / 1_000_000_000_000_000
    }
}
