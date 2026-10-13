import Foundation

/// Prove the identity of one source occurrence through the actual renderer.
/// Counts alone cannot disambiguate reordered tool fields or visible text beside
/// an identical hidden link destination. Replace just that occurrence with a
/// syntax-neutral token and accept its position only if restoring the original
/// text reproduces the entire displayed string. Hidden text, escaped source,
/// markup syntax and transformations that change parsing conservatively fail.
enum RenderedFindMapping {
    static func range(source: String, hit: NSRange, rendered: String,
                      renderReplacing: (String) -> String) -> NSRange? {
        let source = source as NSString
        guard hit.location != NSNotFound, hit.length > 0, NSMaxRange(hit) <= source.length else { return nil }
        let token = "MemexFind" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let changed = renderReplacing(token) as NSString
        let occurrences = ConversationMatcher.ranges(in: changed as String, query: token)
        guard occurrences.count == 1, let occurrence = occurrences.first else { return nil }
        let restored = changed.replacingCharacters(in: occurrence, with: source.substring(with: hit))
        // Find and TextKit use UTF-16 offsets, so canonical Unicode equality is
        // insufficient here: the displayed code units must match exactly.
        guard restored.utf16.elementsEqual(rendered.utf16) else { return nil }
        return NSRange(location: occurrence.location, length: hit.length)
    }
}

/// Shared native reading lane. Bubbles, replies, tools, header and composer use
/// these dimensions; nesting consumes width inside the lane rather than moving it.
enum ConversationReadingLane {
    static let maximumWidth: CGFloat = 800
    static let minimumMargin: CGFloat = 30
    static func width(in viewport: CGFloat) -> CGFloat {
        max(120, min(maximumWidth, viewport - minimumMargin * 2))
    }
    static func origin(in viewport: CGFloat) -> CGFloat { (viewport - width(in: viewport)) / 2 }
}
