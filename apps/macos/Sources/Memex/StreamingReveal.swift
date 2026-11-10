import Foundation

/// Smooths bursty arrivals of a streaming message. The full text is always
/// kept; only a prefix of it is shown. Each time the text grows, the reveal
/// rate is set so the whole backlog drains in `drainDuration` (Zed's typewriter
/// buffer), so large chunks do not pop in and small ones do not stutter.
/// Positions are UTF-16 offsets into the target text.
struct StreamingRevealPacer: Equatable {
    static let drainDuration = 0.2
    /// Keeps a few trailing characters from creeping in one per frame.
    static let minimumRate = 60.0

    private(set) var revealed: Int
    private(set) var targetLength: Int
    private(set) var rate = 0.0
    /// Unrounded reveal position. Boundary snapping can leave `revealed`
    /// behind it; the cursor keeps the budget so the drain time holds.
    private var cursor: Double
    private var lastTime: Double?

    /// Starts fully revealed: text that exists when a message is first shown
    /// appears at once, and only later growth is paced.
    init(length: Int) {
        revealed = length
        targetLength = length
        cursor = Double(length)
    }

    var isComplete: Bool { revealed >= targetLength }
    var backlog: Int { targetLength - revealed }

    /// The target grew by appending; recompute the rate from the whole backlog.
    mutating func extend(to length: Int, at time: Double) {
        guard length > targetLength else { return }
        if isComplete { lastTime = time }
        targetLength = length
        rate = max(Self.minimumRate, (Double(length) - cursor) / Self.drainDuration)
    }

    /// Advances the cursor to `time` and returns the position it reached.
    mutating func advance(to time: Double) -> Int {
        defer { lastTime = time }
        guard !isComplete, let lastTime else { return revealed }
        // A stalled frame reveals more, but never more than one drain's worth.
        let elapsed = min(max(0, time - lastTime), Self.drainDuration)
        cursor = min(Double(targetLength), cursor + rate * elapsed)
        return Int(cursor.rounded(.down))
    }

    /// Records the position actually shown after boundary snapping.
    mutating func commit(_ position: Int) {
        revealed = min(targetLength, max(revealed, position))
        if isComplete { cursor = Double(targetLength) }
    }

    mutating func flush() {
        revealed = targetLength
        cursor = Double(targetLength)
    }
}

enum StreamingReveal {
    /// Re-layout no more often than a pass costs / 0.15, between 30 Hz and 8 Hz,
    /// so long messages spend a bounded share of the main thread on layout.
    static func layoutInterval(forCost cost: Double) -> Double {
        min(1.0 / 8, max(1.0 / 30, cost / 0.15))
    }

    /// Whether growth is revealed gradually; otherwise everything is shown at once.
    static func shouldPace(windowVisible: Bool, reduceMotion: Bool, findActive: Bool, userSelecting: Bool) -> Bool {
        windowVisible && !reduceMotion && !findActive && !userSelecting
    }

    /// A position at most `desired` (when possible) at which to cut the shown
    /// prefix: preferably after whitespace, never inside a grapheme cluster,
    /// and never inside a token the tail repair cannot hide (an escape, a run
    /// of emphasis or code markers, an entity, an autolink or HTML tag).
    /// Returns `revealed` when no safe position lies ahead yet.
    static func cut(in text: NSString, from revealed: Int, toward desired: Int) -> Int {
        let length = text.length
        guard desired < length else { return length }
        guard desired > revealed else { return revealed }
        var position = desired
        // Prefer a word boundary within a short window behind the budget.
        var candidate = desired
        while candidate > max(revealed, desired - 48) {
            if isWhitespace(text.character(at: candidate - 1)) { position = candidate; break }
            candidate -= 1
        }
        position = clusterBoundary(in: text, at: position, after: revealed)
        position = tokenBoundary(in: text, at: position, after: revealed)
        return position > revealed ? min(position, length) : revealed
    }

    private static func clusterBoundary(in text: NSString, at position: Int, after revealed: Int) -> Int {
        guard position > 0, position < text.length else { return position }
        let cluster = text.rangeOfComposedCharacterSequence(at: position)
        guard cluster.location < position else { return position }
        return cluster.location > revealed ? cluster.location : NSMaxRange(cluster)
    }

    private static func tokenBoundary(in text: NSString, at position: Int, after revealed: Int) -> Int {
        guard position > 0, position < text.length else { return position }
        let previous = text.character(at: position - 1)
        let next = text.character(at: position)
        // A backslash escape belongs to the character it escapes.
        if previous == backslash {
            return position - 1 > revealed ? position - 1 : clusterEnd(in: text, at: position)
        }
        // Inside a run such as `**` or a fence of backticks.
        if previous == next, markerRun.contains(previous) {
            var start = position - 1
            while start > 0, text.character(at: start - 1) == previous { start -= 1 }
            if start > revealed { return start }
            var end = position
            while end < text.length, text.character(at: end) == previous { end += 1 }
            return end
        }
        // An entity such as `&amp;` that has not reached its semicolon.
        var start = position - 1
        while start > max(0, position - 12), isEntityCharacter(text.character(at: start)) { start -= 1 }
        if text.character(at: start) == ampersand, start < position - 1 || isEntityCharacter(next) {
            if start > revealed { return start }
            if let end = scan(text, from: position, limit: 12, until: { $0 == semicolon }, stopAt: { !isEntityCharacter($0) }) { return end + 1 }
            return revealed
        }
        // An autolink or tag between `<` and `>`.
        start = position - 1
        while start > max(0, position - 64), !isWhitespace(text.character(at: start)), text.character(at: start) != lessThan,
              text.character(at: start) != greaterThan { start -= 1 }
        if text.character(at: start) == lessThan, start + 1 < text.length, startsTag(text.character(at: start + 1)) {
            if start > revealed { return start }
            if let end = scan(text, from: position, limit: 256, until: { $0 == greaterThan }, stopAt: { _ in false }) { return end + 1 }
            return revealed
        }
        return position
    }

    private static func clusterEnd(in text: NSString, at position: Int) -> Int {
        position < text.length ? NSMaxRange(text.rangeOfComposedCharacterSequence(at: position)) : position
    }

    private static func scan(_ text: NSString, from position: Int, limit: Int, until match: (unichar) -> Bool,
                             stopAt stop: (unichar) -> Bool) -> Int? {
        var index = position
        while index < min(text.length, position + limit) {
            let character = text.character(at: index)
            if match(character) { return index }
            if stop(character) { return nil }
            index += 1
        }
        return nil
    }

    private static let backslash = unichar(UInt8(ascii: "\\"))
    private static let ampersand = unichar(UInt8(ascii: "&"))
    private static let semicolon = unichar(UInt8(ascii: ";"))
    private static let lessThan = unichar(UInt8(ascii: "<"))
    private static let greaterThan = unichar(UInt8(ascii: ">"))
    private static let markerRun: Set<unichar> = Set("*_~`".utf16)

    private static func isWhitespace(_ character: unichar) -> Bool {
        character == 0x20 || character == 0x09 || character == 0x0A || character == 0x0D
    }

    private static func isEntityCharacter(_ character: unichar) -> Bool {
        (0x30...0x39).contains(character) || (0x41...0x5A).contains(character) || (0x61...0x7A).contains(character) || character == 0x23
    }

    private static func startsTag(_ character: unichar) -> Bool {
        (0x41...0x5A).contains(character) || (0x61...0x7A).contains(character) || character == 0x2F || character == 0x21
    }
}
