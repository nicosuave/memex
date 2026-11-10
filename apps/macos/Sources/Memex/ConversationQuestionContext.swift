import Foundation
import UniformTypeIdentifiers

/// Native question replies carry strings, not prompt media blocks. Capture text
/// bytes once and include their contents in the answer sent to that exact request.
struct ConversationQuestionContext: Identifiable, Equatable, Sendable {
    static let maximumBytes = 1_048_576
    static let maximumCount = 10
    let id: String
    let name: String
    let bytes: Data

    static func capture(_ urls: [URL], existing: [Self]) throws -> [Self] {
        guard existing.count + urls.count <= maximumCount else {
            throw ConversationRuntimeError(message: "Attach at most \(maximumCount) text files to an answer.")
        }
        var remaining = maximumBytes - existing.reduce(0) { $0 + $1.bytes.count }
        return try urls.map { url in
            guard url.isFileURL else { throw unsupported(url) }
            let values = try url.resourceValues(forKeys: [.isRegularFileKey, .contentTypeKey])
            guard values.isRegularFile == true else { throw unsupported(url) }
            let mediaTypes: [UTType] = [.image, .audio, .movie, .pdf, .archive]
            if let type = values.contentType,
               mediaTypes.contains(where: { type.conforms(to: $0) }) {
                throw unsupported(url)
            }
            let handle = try FileHandle(forReadingFrom: url)
            defer { try? handle.close() }
            let bytes = try handle.read(upToCount: max(0, remaining) + 1) ?? Data()
            guard bytes.count <= remaining else {
                throw ConversationRuntimeError(message: "Question attachments must total 1 MB or less.")
            }
            guard String(data: bytes, encoding: .utf8) != nil, !bytes.contains(0) else { throw unsupported(url) }
            remaining -= bytes.count
            return Self(id: UUID().uuidString, name: url.lastPathComponent, bytes: bytes)
        }
    }

    static func answerText(_ text: String, contexts: [Self]) -> String {
        ([text].filter { !$0.isEmpty } + contexts.map {
            "Attached answer context: \($0.name)\n\(String(decoding: $0.bytes, as: UTF8.self))"
        }).joined(separator: "\n\n")
    }

    private static func unsupported(_ url: URL) -> ConversationRuntimeError {
        .init(message: "\(url.lastPathComponent): question replies support UTF-8 text files only. Images, audio, video, PDF and binary files cannot be sent as native answer media. Attach them to a separate prompt instead.")
    }
}

/// Navigation never dispatches or removes a pending request. Native IDs, rather
/// than position, keep drafts associated with the right question as replies settle.
struct ConversationQuestionNavigation {
    private(set) var ids: [String] = []
    private(set) var selectedID: String?
    var index: Int { selectedID.flatMap { ids.firstIndex(of: $0) } ?? 0 }
    var canGoBack: Bool { index > 0 }
    var canGoNext: Bool { index + 1 < ids.count }

    mutating func reconcile(_ currentIDs: [String]) {
        let previousIndex = index
        ids = currentIDs
        if let selectedID, ids.contains(selectedID) { return }
        selectedID = ids.isEmpty ? nil : ids[min(previousIndex, ids.count - 1)]
    }

    mutating func move(_ offset: Int) {
        guard !ids.isEmpty else { return }
        selectedID = ids[min(max(0, index + offset), ids.count - 1)]
    }
}
