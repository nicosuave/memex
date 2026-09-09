import Foundation
import Observation

struct ConversationPrompt: Identifiable, Equatable {
    let id: String
    let offset: Int
    let preview: String
}

/// Scans the same paged source as Find, without changing the reader's loaded window.
@Observable @MainActor final class ConversationOutline {
    private(set) var prompts: [ConversationPrompt] = []
    private(set) var scanning = false
    private(set) var error: String?
    var selectedID: String?
    private var token = UUID()
    private var recordOffsets: [String: Int] = [:]

    func load(session: Session?, client: MemexClient) async {
        let token = UUID()
        self.token = token
        prompts = []; selectedID = nil; error = nil
        recordOffsets = [:]
        guard let session else { scanning = false; return }
        scanning = true
        defer { if self.token == token { scanning = false } }
        do {
            var offset = 0
            while true {
                try Task.checkCancellation()
                let page = try await client.records(for: session, offset: offset)
                try Task.checkCancellation()
                guard self.token == token else { return }
                for (index, record) in page.enumerated() { recordOffsets[record.id] = offset + index }
                prompts += Self.entries(page, offset: offset)
                if page.count < MemexClient.pageSize { break }
                offset += page.count
            }
        } catch is CancellationError {
        } catch { if self.token == token { self.error = error.localizedDescription } }
    }

    static func entries(_ records: [TranscriptRecord], offset: Int) -> [ConversationPrompt] {
        records.enumerated().compactMap { index, record in
            guard record.record.role == "user" else { return nil }
            let visible = TranscriptPresentation.project([record]).filter { !$0.record.isInstruction }
                .map { $0.record.text }.joined(separator: " ")
            let preview = visible.split(whereSeparator: \.isWhitespace).joined(separator: " ")
            guard !preview.isEmpty else { return nil }
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
        selectedID = prompts.last(where: { $0.offset <= offset })?.id
    }
}
