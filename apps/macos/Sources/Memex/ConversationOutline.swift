import Foundation
import Observation

struct ConversationPrompt: Identifiable, Equatable {
    let id: String
    let offset: Int
    let preview: String
}

/// Indexes the user prompts of the selected conversation.
///
/// Prompts come from records the reader already holds: the live snapshot, or
/// each history window as it pages. Unread parts of a history transcript are
/// scanned only on demand (when the rail is used), skipping pages the reader
/// has already loaded, and the scan is cancelled when the selection changes.
@Observable @MainActor final class ConversationOutline {
    private(set) var prompts: [ConversationPrompt] = []
    private(set) var scanning = false
    private(set) var error: String?
    var selectedID: String?
    @ObservationIgnored private var recordOffsets: [String: Int] = [:]
    @ObservationIgnored private var covered = IndexSet()
    @ObservationIgnored private var liveRecords: [TranscriptRecord] = []
    @ObservationIgnored private var scan: Task<Void, Never>?
    @ObservationIgnored private var scannedSessionID: String?
    @ObservationIgnored private var complete = false

    /// Forget the previous conversation and cancel its scan.
    func reset() {
        scan?.cancel()
        scan = nil
        scannedSessionID = nil
        complete = false
        liveRecords = []
        recordOffsets = [:]
        covered = IndexSet()
        if scanning { scanning = false }
        if error != nil { error = nil }
        if !prompts.isEmpty { prompts = [] }
        if selectedID != nil { selectedID = nil }
    }

    /// Live transcripts are held completely in memory. Streaming changes only
    /// the tail, so reproject records from the first changed one onward.
    func load(records: [TranscriptRecord]) {
        let unchanged = zip(liveRecords, records).prefix { $0 == $1 }.count
        guard unchanged != records.count || records.count != liveRecords.count else { return }
        liveRecords = records
        complete = true
        recordOffsets = recordOffsets.filter { $0.value < unchanged }
        for (index, record) in records.enumerated().dropFirst(unchanged) {
            if recordOffsets[record.id] == nil { recordOffsets[record.id] = index }
            if recordOffsets[record.sourceID] == nil { recordOffsets[record.sourceID] = index }
        }
        let kept = prompts.filter { $0.offset < unchanged }
        let added = Self.entries(Array(records[unchanged...]), offset: unchanged, excludingIDs: Set(kept.map(\.id)))
        let updated = kept + added
        if updated != prompts { prompts = updated }
        if let selectedID, !prompts.contains(where: { $0.id == selectedID }) { self.selectedID = nil }
    }

    /// Merges a loaded history window at its transcript offset.
    func absorb(records: [TranscriptRecord], offset: Int) {
        let range = offset..<(offset + records.count)
        guard !records.isEmpty, !covered.contains(integersIn: range) else { return }
        // Only records outside previously absorbed windows are projected.
        var known = Set(prompts.map(\.id))
        var added: [ConversationPrompt] = []
        for (index, record) in records.enumerated() where !covered.contains(offset + index) {
            if recordOffsets[record.id] == nil { recordOffsets[record.id] = offset + index }
            if recordOffsets[record.sourceID] == nil { recordOffsets[record.sourceID] = offset + index }
            if let prompt = Self.entries([record], offset: offset + index, excludingIDs: known).first {
                known.insert(prompt.id)
                added.append(prompt)
            }
        }
        covered.insert(integersIn: range)
        guard !added.isEmpty else { return }
        prompts = (prompts + added).sorted { $0.offset < $1.offset }
    }

    /// Reads the parts of a history transcript the reader has not loaded.
    /// Runs at most once per conversation; `reset()` cancels it.
    func completeIfNeeded(session: Session?, client: MemexClient) {
        guard let session, !complete, scan == nil || scannedSessionID != session.id else { return }
        scan?.cancel()
        scannedSessionID = session.id
        scanning = true
        error = nil
        scan = Task { [weak self] in
            var offset = 0
            do {
                while true {
                    try Task.checkCancellation()
                    guard let self else { return }
                    let pageRange = offset..<(offset + MemexClient.pageSize)
                    if self.covered.contains(integersIn: pageRange) { offset = pageRange.upperBound; continue }
                    let page = try await client.records(for: session, offset: offset)
                    try Task.checkCancellation()
                    self.absorb(records: page, offset: offset)
                    if page.count < MemexClient.pageSize { break }
                    offset += page.count
                }
                self?.complete = true
            } catch is CancellationError {
                return
            } catch {
                self?.error = error.localizedDescription
            }
            self?.scanning = false
        }
    }

    static func entries(_ records: [TranscriptRecord], offset: Int, excludingIDs: Set<String> = []) -> [ConversationPrompt] {
        var seen = excludingIDs
        return records.enumerated().compactMap { index, record in
            guard record.record.role == "user" else { return nil }
            let visible = TranscriptPresentation.project([record]).filter { !$0.record.isInstruction }
                .map { $0.record.text }.joined(separator: " ")
            let preview = visible.split(whereSeparator: \.isWhitespace).joined(separator: " ")
            guard !preview.isEmpty, seen.insert(record.id).inserted else { return nil }
            return ConversationPrompt(id: record.id, offset: offset + index, preview: String(preview.prefix(180)))
        }
    }

    func move(_ direction: Int) -> ConversationPrompt? {
        guard !prompts.isEmpty else { return nil }
        let current = prompts.firstIndex { $0.id == selectedID }
        let index = min(prompts.count - 1, max(0, (current ?? (direction > 0 ? -1 : prompts.count)) + direction))
        selectedID = prompts[index].id
        return prompts[index]
    }

    func follow(_ recordID: String?) {
        guard let recordID, let offset = recordOffsets[recordID] else { return }
        let id = prompts.last(where: { $0.offset <= offset })?.id
        if selectedID != id { selectedID = id }
    }
}
