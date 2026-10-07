import AppKit
import CryptoKit
import Darwin
import Testing
@testable import Memex

/// Opt-in renderer measurements against the same text corpus used by T3's UI.
/// This exercises real native rows, scrolling and incremental updates, but excludes
/// acquisition/indexing, provider transport, app launch and GPU presentation.
/// Windows remain offscreen. `displayIfNeeded` is a synchronous drawing boundary,
/// not evidence that a frame was presented to the user.
@Suite(.serialized) @MainActor struct MatchedCorpusReaderPerformanceTests {
    @Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_MATCHED_PERF_CORPUS"] != nil))
    func matchedCorpusOpenAndSwitch() async throws {
        let fixture = try CorpusFixture.load()
        for (label, records) in fixture.workloads {
            for iteration in 0..<5 {
                try await withReaderHost { host in
                    let cold = host.update(sessionID: "target", records: records)
                    try host.expectLastRecord(records)
                    report(label: "open_\(label)", iteration: iteration, records: records.count,
                           samples: [cold], extra: ["cache": "fresh_controller_process_warm"])

                    _ = host.update(sessionID: "other", records: fixture.shortRecords)
                    let switched = host.update(sessionID: "target", records: records)
                    try host.expectLastRecord(records)
                    report(label: "switch_\(label)", iteration: iteration, records: records.count,
                           samples: [switched])

                    let unchanged = host.update(sessionID: "target", records: records)
                    report(label: "unchanged_\(label)", iteration: iteration, records: records.count,
                           samples: [unchanged])
                }
            }
        }
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_MATCHED_PERF_CORPUS"] != nil))
    func matchedCorpusLongHistoryWheelScroll() async throws {
        let fixture = try CorpusFixture.load()
        for iteration in 0..<5 {
            try await withAsyncReaderHost { host in
                _ = host.update(sessionID: "long", records: fixture.longRecords)
                try host.expectLastRecord(fixture.longRecords)
                // Each batch starts at the end in a fresh controller. These are
                // direct native events without OS delivery or display scheduling.
                let initialY = host.reader.scrollView.contentView.bounds.minY
                var costs: [Cost] = []
                var opportunityWaitMS: [Double] = []
                for _ in 0..<120 {
                    let mutationMS = try autoreleasepool {
                        let event = try #require(CGEvent(scrollWheelEvent2Source: nil, units: .pixel,
                                                        wheelCount: 1, wheel1: 120, wheel2: 0, wheel3: 0))
                        let wheel = try #require(NSEvent(cgEvent: event))
                        let start = ContinuousClock.now
                        host.reader.scrollView.scrollWheel(with: wheel)
                        return elapsedMS(start)
                    }
                    // NSScrollView can animate wheel events. Permit an AppKit
                    // run-loop opportunity before layout, as the real wheel
                    // routing regression does. This wait is NOT renderer work.
                    let waitStart = ContinuousClock.now
                    try await Task.sleep(for: .milliseconds(16))
                    opportunityWaitMS.append(elapsedMS(waitStart))
                    let layoutMS = autoreleasepool {
                        let start = ContinuousClock.now
                        host.layout()
                        return elapsedMS(start)
                    }
                    costs.append(Cost(mutationMS: mutationMS, totalMS: mutationMS + layoutMS))
                }
                let finalY = host.reader.scrollView.contentView.bounds.minY
                try #require(finalY < initialY)
                report(label: "long_wheel", iteration: iteration, records: fixture.longRecords.count,
                       samples: costs, extra: ["wheel_pixels": 120, "distance_pixels": initialY - finalY,
                                               "cadence": "16ms_minimum_between_events", "input": "direct_native_wheel",
                                               "boundary": "native_wheel_dispatch_plus_post_yield_layout",
                                               "opportunity_wait_ms": opportunityWaitMS,
                                               "total_excludes": "scheduler_wait_and_asynchronous_animation_work"])
            }
        }
    }

    @Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_MATCHED_PERF_CORPUS"] != nil))
    func matchedCorpusIncrementalAssistantUpdate() async throws {
        let fixture = try CorpusFixture.load()
        for (label, original) in fixture.workloads where label != "short" {
            let complete = try #require(original.last)
            #expect(complete.record.role == "assistant")
            let characters = Array(complete.record.text)
            // Keep the corpus's final message identity and content. Replace its
            // text with cumulative 64-character prefixes, just as the reader sees
            // repeated snapshots for a single growing assistant message.
            let prefixes = stride(from: 64, to: characters.count, by: 64).map { $0 } + [characters.count]
            for iteration in 0..<5 {
                try await withReaderHost { host in
                    var records = Array(original.dropLast())
                    _ = host.update(sessionID: "stream", records: records)
                    try host.expectLastRecord(records)
                    var costs: [Cost] = []
                    for count in prefixes {
                        var message = complete.record
                        message.text = String(characters.prefix(count))
                        let growing = TranscriptRecord(recordID: complete.id, record: message)
                        if records.count == original.count { records[records.count - 1] = growing }
                        else { records.append(growing) }
                        costs.append(host.update(sessionID: "stream", records: records, followLatest: true))
                    }
                    #expect(records == original)
                    try host.expectLastRecord(original)
                    report(label: "incremental_\(label)", iteration: iteration, records: records.count,
                           samples: costs, extra: ["chunk_characters": 64, "final_characters": characters.count,
                                                   "cadence": "unpaced", "transport": "direct_snapshot_update"])
                }
            }
        }
    }

    private struct Cost {
        let mutationMS: Double
        let totalMS: Double
    }

    /// Pools and queue turns are deliberately outside the reported operations.
    /// A synchronous test otherwise retains autoreleased AppKit objects and never
    /// services the reader's queued MainActor navigation updates between batches.
    private func withReaderHost(_ operation: (ReaderHost) throws -> Void) async throws {
        let lifetime = HostLifetime()
        try autoreleasepool {
            let host = try ReaderHost()
            lifetime.reader = host.reader
            lifetime.window = host.window
            defer { host.close() }
            try operation(host)
        }
        try await requireReleased(lifetime)
    }

    private func withAsyncReaderHost(_ operation: (ReaderHost) async throws -> Void) async throws {
        // Do not hold an autorelease pool across suspension. Initial construction,
        // each event/update/layout and teardown instead have their own pools.
        let lifetime = try await runAsyncReaderHost(operation)
        try await requireReleased(lifetime)
    }

    private func runAsyncReaderHost(_ operation: (ReaderHost) async throws -> Void) async throws -> HostLifetime {
        let host = try autoreleasepool { try ReaderHost() }
        let lifetime = HostLifetime()
        lifetime.reader = host.reader
        lifetime.window = host.window
        defer { autoreleasepool { host.close() } }
        try await operation(host)
        return lifetime
    }

    private func requireReleased(_ lifetime: HostLifetime) async throws {
        // Allow queued AppKit teardown and MainActor work to finish before the
        // next batch; neither waiting nor object destruction enters the timings.
        for _ in 0..<2 {
            await withCheckedContinuation { continuation in
                DispatchQueue.main.async { continuation.resume() }
            }
            await Task.yield()
        }
        try #require(lifetime.reader == nil, "Previous benchmark reader is still retained")
        try #require(lifetime.window == nil, "Previous benchmark window is still retained")
    }

    @MainActor private final class HostLifetime {
        weak var reader: TranscriptController?
        weak var window: NSWindow?
    }

    @MainActor private final class ReaderHost {
        let reader = TranscriptController()
        let window: NSWindow

        init() throws {
            _ = NSApplication.shared
            let contentSize = NSSize(width: 1000, height: 720)
            // TranscriptController's loadView creates a zero-frame NSScrollView.
            // Unlike the SwiftUI representable, an offscreen NSWindow host has no
            // parent layout proposal to size that view. Give it the intended
            // viewport before installation, then retain that size on the window.
            window = NSWindow(contentRect: NSRect(origin: .zero, size: contentSize),
                              styleMask: [.titled, .resizable], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            reader.view.setFrameSize(contentSize)
            window.contentViewController = reader
            window.setContentSize(contentSize)
            reader.scrollView.tile()
            layout()
            try #require(abs(reader.view.bounds.width - contentSize.width) < 1)
            try #require(abs(reader.view.bounds.height - contentSize.height) < 1)
            try #require(reader.scrollView.contentView.bounds.width > 900)
            try #require(reader.scrollView.contentView.bounds.height > 700)
        }

        func layout() {
            window.contentView?.layoutSubtreeIfNeeded()
            window.displayIfNeeded()
        }

        func update(sessionID: String, records: [TranscriptRecord], followLatest: Bool = false) -> Cost {
            autoreleasepool {
                let start = ContinuousClock.now
                reader.update(sessionID: sessionID, records: records, provider: "codex",
                              startsAtEnd: true, followLatest: followLatest)
                let mutationMS = elapsedMS(start)
                layout()
                return Cost(mutationMS: mutationMS, totalMS: elapsedMS(start))
            }
        }

        func close() {
            window.close()
            // close() alone does not clear these strong ownership links when
            // isReleasedWhenClosed is false, as required for Swift-owned windows.
            window.contentViewController = nil
            window.contentView = nil
        }

        func expectLastRecord(_ records: [TranscriptRecord]) throws {
            try #require(reader.rows.count == records.count)
            let last = try #require(records.last)
            try #require(reader.rows.last?.records.last == last)
            let viewport = reader.scrollView.documentVisibleRect
            try #require(viewport.width > 900 && viewport.height > 700)
            let visible = reader.table.rows(in: viewport)
            try #require(visible.location != NSNotFound && visible.length > 0)
            try #require(NSMaxRange(visible) >= reader.rows.count)
        }
    }

    private struct Corpus: Decodable {
        struct Project: Decodable { let id: String }
        struct Thread: Decodable {
            struct Message: Decodable {
                let id: String
                let turnID: String
                let role: String
                let text: String
            }
            let id: String
            let messages: [Message]
        }
        let version: Int
        let t3Commit: String
        let projects: [Project]
        let threads: [Thread]
    }

    private struct Manifest: Decodable {
        let projects: Int
        let threads: Int
        let messages: Int
        let longThreadID: String
        let longMessages: Int
        let shortThreadID: String
        let corpusSHA256: String
        let t3Commit: String
    }

    private struct CorpusFixture {
        let shortRecords: [TranscriptRecord]
        let longRecords: [TranscriptRecord]
        var workloads: [(String, [TranscriptRecord])] {
            [("short", shortRecords), ("long_initial_60", Array(longRecords.suffix(60))),
             ("long_full_2000", longRecords)]
        }

        static func load() throws -> Self {
            let path = try #require(ProcessInfo.processInfo.environment["MEMEX_MATCHED_PERF_CORPUS"])
            let url = URL(fileURLWithPath: path)
            let data = try Data(contentsOf: url)
            let manifestData = try Data(contentsOf: url.deletingLastPathComponent().appendingPathComponent("manifest.json"))
            let manifest = try JSONDecoder().decode(Manifest.self, from: manifestData)
            let digest = SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
            try #require(digest == manifest.corpusSHA256)
            let decodeStart = ContinuousClock.now
            let corpus = try JSONDecoder().decode(Corpus.self, from: data)
            let decodeMS = elapsedMS(decodeStart)
            try #require(corpus.version == 1 && corpus.t3Commit == manifest.t3Commit)
            try #require(corpus.projects.count == 20 && manifest.projects == 20)
            try #require(corpus.threads.count == 1000 && manifest.threads == 1000)
            try #require(corpus.threads.reduce(0) { $0 + $1.messages.count } == 11990 && manifest.messages == 11990)
            try #require(Set(corpus.threads.map(\.id)).count == corpus.threads.count)

            // Conversion retains exact source identity, role, turn and full Markdown
            // text. No provider lifecycle/tool rows are invented. Conversion happens
            // outside the renderer timers and is reported separately.
            let conversionStart = ContinuousClock.now
            let converted = corpus.threads.map { thread in
                thread.messages.map { message in
                    TranscriptRecord(recordID: message.id,
                                     record: Message(role: message.role, text: message.text, toolName: nil,
                                                     toolInput: nil, toolOutput: nil, eventID: message.id,
                                                     sourceTurnID: message.turnID))
                }
            }
            let conversionMS = elapsedMS(conversionStart)
            let longIndex = try #require(corpus.threads.firstIndex { $0.id == manifest.longThreadID })
            let shortIndex = try #require(corpus.threads.firstIndex { $0.id == manifest.shortThreadID })
            try #require(converted[longIndex].count == 2000 && manifest.longMessages == 2000)
            try #require(converted[shortIndex].count == 10)
            emit(["case": "fixture", "sha256": digest, "t3_commit": corpus.t3Commit,
                  "corpus_bytes": data.count, "threads": corpus.threads.count, "messages": manifest.messages,
                  "decode_ms": decodeMS, "conversion_ms": conversionMS])
            return Self(shortRecords: converted[shortIndex], longRecords: converted[longIndex])
        }
    }

    private func report(label: String, iteration: Int, records: Int, samples: [Cost], extra: [String: Any] = [:]) {
        let totals = samples.map(\.totalMS).sorted()
        let middle = totals.count / 2
        let median = totals.count.isMultiple(of: 2) ? (totals[middle - 1] + totals[middle]) / 2 : totals[middle]
        var values: [String: Any] = [
            "case": label, "iteration": iteration, "records": records, "samples": samples.count,
            "boundary": "native_update_layout_offscreen_display", "viewport_width": 1000, "viewport_height": 720,
            "total_ms": samples.map(\.totalMS), "mutation_ms": samples.map(\.mutationMS),
            "layout_ms": samples.map { $0.totalMS - $0.mutationMS },
            "median_ms": median, "p95_ms": totals[Int(ceil(Double(totals.count) * 0.95)) - 1],
            "max_ms": totals.last ?? 0, "operations_over_16_67ms": totals.filter { $0 > 16.67 }.count,
            "operations_over_33_33ms": totals.filter { $0 > 33.33 }.count,
        ]
        values.merge(extra) { _, new in new }
        emit(values)
    }
}

private func elapsedMS(_ start: ContinuousClock.Instant) -> Double {
    let duration = start.duration(to: .now).components
    return Double(duration.seconds) * 1000 + Double(duration.attoseconds) / 1_000_000_000_000_000
}

private func emit(_ values: [String: Any]) {
    var values = values
    var load = [Double](repeating: 0, count: 3)
    let available = getloadavg(&load, 3)
    let activeCores = ProcessInfo.processInfo.activeProcessorCount
    values["host_logical_cores"] = ProcessInfo.processInfo.processorCount
    values["host_active_logical_cores"] = activeCores
    values["host_load_1m"] = available > 0 ? load[0] as Any : NSNull()
    values["host_load_exceeds_active_cores"] = available > 0 ? (load[0] > Double(activeCores)) as Any : NSNull()
    values["timing_environment"] = "shared_host_wall_time_not_isolated_cpu_time"
    guard let data = try? JSONSerialization.data(withJSONObject: values, options: [.sortedKeys]),
          let line = String(data: data, encoding: .utf8) else { return }
    print("matched_reader_perf \(line)")
}
