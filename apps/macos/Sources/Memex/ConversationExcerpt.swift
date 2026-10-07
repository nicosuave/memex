import Foundation

enum ConversationExcerpt {
    /// The CLI snippet may keep 80 characters before a hit. A narrow list needs
    /// less leading context so the matching passage survives its two-line limit.
    static func text(_ snippet: String, query: String) -> String {
        let phrase = query.trimmingCharacters(in: .whitespacesAndNewlines)
            .trimmingCharacters(in: CharacterSet(charactersIn: "\""))
        guard !phrase.isEmpty else { return snippet }
        let words = phrase.split(whereSeparator: \.isWhitespace).map(String.init)
            .filter { !["AND", "OR", "NOT"].contains($0) }
        let hit = snippet.range(of: phrase, options: [.caseInsensitive, .literal])
            ?? words.compactMap { snippet.range(of: $0, options: [.caseInsensitive, .literal]) }
                .min(by: { $0.lowerBound < $1.lowerBound })
        guard let hit else { return snippet }
        let start = snippet.index(hit.lowerBound, offsetBy: -20, limitedBy: snippet.startIndex) ?? snippet.startIndex
        return (start == snippet.startIndex ? "" : "…") + snippet[start...]
    }
}
