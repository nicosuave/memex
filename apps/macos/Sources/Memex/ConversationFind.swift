import Foundation
import Observation

struct ConversationFindHit: Equatable, Sendable {
    let recordID: String
    var recordOffset: Int = 0
    let occurrence: Int
    let range: NSRange
}

enum ConversationMatcher {
    static func ranges(in text: String, query: String) -> [NSRange] {
        guard !query.isEmpty else { return [] }
        let source = text as NSString
        var result: [NSRange] = []
        var offset = 0
        while offset < source.length {
            if Task.isCancelled { break }
            let range = source.range(of: query, options: [.caseInsensitive, .literal],
                                     range: NSRange(location: offset, length: source.length - offset))
            guard range.location != NSNotFound, range.length > 0 else { break }
            result.append(range)
            offset = NSMaxRange(range)
        }
        return result
    }

    static func body(_ record: TranscriptRecord) -> String {
        if record.record.isActivity || record.record.isInstruction {
            return TranscriptActivity(records: [record]).body
        }
        return record.record.text
    }

    static func matches(_ records: [TranscriptRecord], query: String, offset: Int = 0) -> [ConversationFindHit] {
        records.enumerated().flatMap { index, record in
            ranges(in: body(record), query: query).enumerated().map {
                ConversationFindHit(recordID: record.id, recordOffset: offset + index, occurrence: $0.offset, range: $0.element)
            }
        }
    }
}

@Observable @MainActor
final class ConversationFindState {
    var isOpen = false
    var query = ""
    private(set) var hits: [ConversationFindHit] = []
    private(set) var selectedIndex: Int?
    private(set) var isScanning = false
    private(set) var scannedRecords = 0
    private(set) var error: String?
    private(set) var generation = 0
    @ObservationIgnored private var task: Task<Void, Never>?
    @ObservationIgnored private var token = UUID()
    @ObservationIgnored private let client: MemexClient

    init(client: MemexClient) { self.client = client }
    var selectedHit: ConversationFindHit? {
        selectedIndex.flatMap { hits.indices.contains($0) ? hits[$0] : nil }
    }
    var status: String {
        if error != nil { return "Find failed" }
        if query.isEmpty { return "" }
        if let selectedIndex { return "\(selectedIndex + 1) of \(hits.count)" }
        return isScanning ? "Searching…" : "No matches"
    }
    var statusDetail: String {
        if let error { return error }
        if query.isEmpty { return "Find literal text throughout this conversation" }
        return "\(hits.count) matches in \(scannedRecords) messages\(isScanning ? " searched so far" : "")"
    }
    func close() {
        isOpen = false
        query = ""
        reset()
    }
    func reset() {
        task?.cancel()
        token = UUID()
        hits = []; selectedIndex = nil; isScanning = false; scannedRecords = 0; error = nil
        generation += 1
    }
    func search(in session: Session?) {
        reset()
        guard isOpen, let session, !query.isEmpty else { return }
        let query = query
        let token = token
        let client = client
        isScanning = true
        task = Task { [weak self] in
            do {
                try await Task.sleep(for: .milliseconds(160))
                var offset = 0
                while true {
                    try Task.checkCancellation()
                    let page = try await client.records(for: session, offset: offset)
                    let pageOffset = offset
                    let worker = Task.detached(priority: .userInitiated) {
                        ConversationMatcher.matches(page, query: query, offset: pageOffset)
                    }
                    let matches = await withTaskCancellationHandler { await worker.value } onCancel: { worker.cancel() }
                    try Task.checkCancellation()
                    guard let self, self.token == token else { return }
                    self.hits += matches
                    self.scannedRecords += page.count
                    if self.selectedIndex == nil, !self.hits.isEmpty {
                        self.selectedIndex = 0
                        self.generation += 1
                    }
                    if page.count < MemexClient.pageSize { self.isScanning = false; return }
                    offset += page.count
                }
            } catch is CancellationError {
            } catch {
                guard let self, self.token == token else { return }
                self.isScanning = false
                self.error = error.localizedDescription
            }
        }
    }
    func move(_ delta: Int) {
        guard !hits.isEmpty else { return }
        selectedIndex = ((selectedIndex ?? (delta > 0 ? -1 : 0)) + delta + hits.count) % hits.count
        generation += 1
    }
}
