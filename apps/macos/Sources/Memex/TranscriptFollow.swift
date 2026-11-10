import CoreGraphics

/// Whether a live transcript keeps the latest content in view. The state
/// changes only from the reader's own movement: scrolling, dragging the
/// scroller, paging, or navigating to a passage. Content growth and the
/// controller's own pinning or anchoring never change it, so a reply that grows
/// while the reader is elsewhere never pulls them back to the end.
struct TranscriptFollowState: Equatable {
    /// Points from the end that still count as "at the end" when the reader stops.
    static let tolerance: CGFloat = 8

    enum Mode: Equatable { case following, detached }

    enum Event: Equatable {
        /// A trackpad or wheel gesture, or scroller tracking, began.
        case liveScrollBegan
        /// The reader moved the viewport, by a gesture, keys, or navigation.
        /// `distanceFromEnd` is positive when content lies below the viewport.
        case readerMoved(distanceFromEnd: CGFloat)
        case liveScrollEnded(distanceFromEnd: CGFloat)
        /// Content changed size or the controller re-pinned or re-anchored.
        case contentChanged
        /// An explicit request to show the latest content.
        case jumpToLatest
    }

    private(set) var mode: Mode = .following
    private(set) var isUserScrolling = false

    var isFollowing: Bool { mode == .following }
    /// Pin to the end only while following and not fighting a gesture in progress.
    var shouldPin: Bool { mode == .following && !isUserScrolling }

    mutating func handle(_ event: Event) {
        switch event {
        case .liveScrollBegan:
            isUserScrolling = true
        case .readerMoved(let distance):
            mode = distance <= Self.tolerance ? .following : .detached
        case .liveScrollEnded(let distance):
            isUserScrolling = false
            mode = distance <= Self.tolerance ? .following : .detached
        case .contentChanged:
            break
        case .jumpToLatest:
            isUserScrolling = false
            mode = .following
        }
    }
}
