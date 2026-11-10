# Native macOS techniques for a zero-jank streaming chat transcript

Researcher scope: native macOS/iOS techniques and open-source Swift implementations. Read-only. Date: 2026-11-09.

## Method, evidence labels, and limits

- **VERIFIED (src)** = I read the cited `path:line` in a shallow clone at the SHA in the table below.
- **VERIFIED (doc)** = read directly from Apple's documentation JSON (`developer.apple.com/tutorials/data/documentation/...json`), which carries exact availability and discussion text.
- **VERIFIED (transcript)** = Apple WWDC transcript or third-party page read through a summarizing fetch tool; substance is reliable, but re-check the wording before quoting it publicly.
- **UNVERIFIED** = search-result snippet, single forum report, or my own inference. Stated as such.
- Nothing was built or run. Every Swift snippet marked "illustrative" is mine, uncompiled, and written for macOS 14+ / Swift 6.
- Timing numbers in this report are constants read out of source, not measurements I made. I found no public, reproducible frame-time benchmark comparing these containers on macOS; the only quantified claims are Apple's WWDC25 list figures and the constants the libraries themselves ship.

Clones live under `/private/tmp/claude-501/-Volumes-Dev-Code-memex--claude-worktrees-agent-app-ux-patterns-13a76b/f8deb1e4-a002-4452-a81e-c8ef8a51001b/scratchpad/repos/`.

| Dir | Repo | SHA |
|---|---|---|
| swift-Lakr233-FlowDown | Lakr233/FlowDown | b6044aeaed55985caf14d52450e9632f32723211 |
| swift-Lakr233-MarkdownView | Lakr233/MarkdownView (4.8.5) | 3941f1857005d648f452fd1bfbdbe01d37961fd2 |
| swift-Lakr233-Litext | Lakr233/Litext | 70925d2d879916f6a46e30cdb516ec7ddbf66506 |
| swift-Lakr233-ListViewKit | Lakr233/ListViewKit | cca344d79bc671ce06a1f21a32aca181ca1d6273 |
| swift-vivy-company-aizen | vivy-company/aizen | 3ccde932e192acd249ede4ae522f7a9c19ba9140 |
| swift-vivy-company-VVDevKit | vivy-company/VVDevKit | 1f507448eb26f43fa833ed93e6a02b9f0f71e1ff |
| swift-jazzyalex-agent-sessions | jazzyalex/agent-sessions | f133cc71e497f5c3be9ae5e0671470fe5cc06314 |
| swift-ttnear-Clarc | ttnear/Clarc | ba4c0d9f43165bb755f9d168ebfeb60fab19712d |
| swift-PicoMLX-PicoMarkdownView | PicoMLX/PicoMarkdownView | f192326a906d87fcacd3013ea2511b8c1e2d8c54 |
| swift-microsoft-SwiftStreamingMarkdown | microsoft/SwiftStreamingMarkdown | 95bb755a9b23a1aea8682b9ebc912cb72b176c95 |
| swift-LiYanan2004-MarkdownView | LiYanan2004/MarkdownView | bc8f5fbcb068426b7029da51522ff0124b66ac89 |
| swift-gonzalezreal-textual | gonzalezreal/textual | 01b51875a5406eefc95f52a058cb059e7bc94dc4 |
| swift-gonzalezreal-swift-markdown-ui | gonzalezreal/swift-markdown-ui | 8371aeb32f35795a9e559029fb08613355733626 |
| swift-krzyzanowskim-STTextView | krzyzanowskim/STTextView | c7d519318b06ebb7a99a641e4aa8831105d3b7d4 |
| swift-markiv-SwiftUI-Shimmer | markiv/SwiftUI-Shimmer | 0226e21f9bf355d40e07e5f5e1c33679d50e167f |
| swift-farouqaldori-claude-island | farouqaldori/claude-island | 10f1d240e2b4965c2e3c82a3a916458d0541b2be |
| swift-AugustDev-enchanted | AugustDev/enchanted | dc9bff882411a1317ff0cc5869de7e0515897df0 |
| swift-johnbean393-Sidekick | johnbean393/Sidekick | 83b3e6e0d8d6831a890e4df73435a3987b086e20 |
| swift-synth-inc-onit | synth-inc/onit | 910240c6d872cb16a59c082bd849359f4a4ce64a |
| swift-dnakov-hairball | dnakov/hairball | fbb4282ca428e4a76f6a4379d4ae844e9bfea95a |
| codex (cloned by another researcher) | openai/codex | caae161dd0c82f2e04e57e6f9d738fafbce503ba |

Context I checked in Memex itself (worktree HEAD dd76a36, i.e. `main`, not the PR under review): the existing reader is a view-based `NSTableView` with `usesAutomaticRowHeights = false`, a per-row measurement cache, TextKit 1 measurement, clip-view bounds observation and manual offset preservation. VERIFIED (src) `apps/macos/Sources/Memex/TranscriptController.swift:159,173,520-552,555,1049-1087`. That is the same architecture the best AppKit reference below uses, so the recommendations are extensions of it, not a rewrite.

---

## 1. Long, live-updating transcript containers

### 1.1 What the examined apps actually ship

| App | Container | Row heights | Evidence |
|---|---|---|---|
| FlowDown (UIKit/Catalyst) | Custom `ListView` (ListViewKit) on its own scroll view; AppKit variant is a plain `NSView`, not `NSScrollView` | Explicit `.height {}` closure per row type, estimates corrected in time slices | VERIFIED (src) `FlowDown/Interface/MessageListView/MessageListView+Adapter.swift:17-109`; `ListViewKit/Sources/ListScrollView.swift:520` |
| Aizen (native macOS ACP client) | One Metal view, `VVChatTimeline`; SwiftUI only hosts it | Own layout engine, Fenwick tree | VERIFIED (src) `aizen/Features/Chat/UI/Components/ChatMessageList.swift:5,39,91-99`; `VVDevKit/Docs/VVChatTimelinePerformance.md:78` |
| agent-sessions (macOS) | View-based `NSTableView` | `heightOfRow` + cache keyed by row id, width bucket, font bucket | VERIFIED (src) `AgentSessions/Views/TranscriptBlockListView.swift:955,2089,2190-2222` |
| Clarc (macOS 15+) | SwiftUI `ScrollView` + plain `VStack`, older messages folded behind a button past 30 | SwiftUI | VERIFIED (src) `Packages/Sources/ClarcChatKit/MessageListView.swift:16-61` |
| claude-island | `ScrollView` + `LazyVStack`, vertically flipped | SwiftUI | VERIFIED (src) `ClaudeIsland/UI/Views/ChatView.swift:275-309` |
| Enchanted | `ScrollViewReader` + `scrollTo(messages.last)` | SwiftUI | VERIFIED (src) `Enchanted/UI/Shared/Chat/Components/MessageListVIew.swift:42-104` |
| Memex reader (main) | View-based `NSTableView` | `heightOfRow` + measurement cache | VERIFIED (src) as above |

Observation across the nine apps grepped: none uses `NSCollectionView` for the transcript and none uses SwiftUI `List` (VERIFIED by grep counts over all `.swift` files in each clone). The two apps with the most performance engineering (FlowDown, Aizen) both abandoned system containers entirely.

### 1.2 Container comparison

**SwiftUI `List`.**
- macOS 26 claim: "lists of over 100,000 items now load 6x faster" and "update up to 16x faster", plus "improved scheduling of user interface updates on iOS and macOS". VERIFIED (transcript) WWDC25 session 256, https://developer.apple.com/videos/play/wwdc2025/256/. Those are load/update figures for huge uniform lists; the session gives no scrolling numbers and no benchmark setup.
- No API to pin a visible anchor when rows above change height, no cross-row text selection, row chrome fights custom bubbles. Not chosen by any examined app. Not recommended.

**`LazyVStack` in `ScrollView`.**
- Heights of unrealized subviews are estimated "from the average of views already placed", "the lazy stack doesn't know about changes to off-screen views", the estimate changes while scrolling, and Apple's own advice is to avoid absolute content offsets with lazy stacks and to fix item sizes where possible. VERIFIED (transcript) WWDC26 session 321 "Dive into lazy stacks and scrolling with SwiftUI", https://developer.apple.com/videos/play/wwdc2026/321/. A transcript of variable-height markdown is the worst case for that model.
- Symptoms in the field: variable heights produce stutter and jumps (Apple forums thread 685461, UNVERIFIED single reports); on iOS 18.3 a 1,000-cell feed with random heights dropped frames and "scroll jumped erratically" under scrollbar drags (VERIFIED (transcript) https://blog.jacobstechtavern.com/p/swiftui-scroll-performance-the-120fps, visible portion only, iPhone not Mac).
- Prepending history: `ScrollViewProxy.scrollTo` after a prepend reportedly stopped restoring position on the 27 releases, `.defaultScrollAnchor` snapped to newest, `scrollPosition(id:)` re-targeted; the reporter fell back to adjusting the backing scroll view's offset. UNVERIFIED (one forum report, FB24968838, no Apple reply) https://developer.apple.com/forums/thread/848608. Treat SwiftUI prepend anchoring as unreliable across OS versions.
- The modern scroll APIs are macOS 15+: `ScrollPosition`, `scrollPosition(_:anchor:)`, `onScrollGeometryChange(for:of:action:)`, `onScrollPhaseChange`, `onScrollTargetVisibilityChange`, `defaultScrollAnchor(_:for:)` with `ScrollAnchorRole` (`initialOffset`, `sizeChanges`, `alignment`). VERIFIED (doc). On Memex's macOS 14 floor only `defaultScrollAnchor(_:)`, `scrollPosition(id:anchor:)` and `scrollTargetLayout()` exist. VERIFIED (doc).
- `onScrollGeometryChange`: "You should avoid updating large parts of your app whenever the scroll geometry changes"; transform to a narrow `Equatable`. VERIFIED (doc). Clarc's use is the right shape (distance-from-bottom < 120 mapped to a `Bool`). VERIFIED (src) `MessageListView.swift:99-106`.
- `ScrollViewReader` pitfalls seen in source: Aizen defers scroll requests through an 80 ms task specifically to avoid "ScrollViewProxy may not be accessed during view updates" (VERIFIED (src) `aizen/Features/Chat/Application/ChatTimelineStore.swift:98-101,129-142`); Clarc sleeps one frame before scrolling and two more before fading the list in on session switch (VERIFIED (src) `MessageListView.swift:107-125`). Sleeps-as-synchronization is the smell that the container does not expose "layout finished".
- Inverted list trick (`scaleEffect(x: 1, y: -1)` on the scroll view and each row, data reversed): gives free stick-to-bottom because new content lands at offset 0. VERIFIED (src) claude-island `ChatView.swift:287-309`. On macOS it also inverts wheel direction, scroller position, and any AppKit-hosted subview (text selection, context menus, libghostty terminals) unless each is re-flipped. UNVERIFIED inference from how the transform applies; claude-island hides scroll indicators and has no hosted AppKit rows. Not suitable for Memex.

**`NSTableView`, view-based.**
- With `usesAutomaticRowHeights` (Auto Layout derived heights, macOS 10.13+, VERIFIED (doc)) the table measures lazily near the viewport and estimates the rest since macOS 13, with a stale-height-cache bug on `reloadData()` in 13.0 and a `NSTableViewCanEstimateRowHeights` opt-out. VERIFIED (transcript) https://christiantietze.de/posts/2022/11/nstableview-variable-row-heights-broken-macos-ventura-13-0. Forum reports of grow-but-not-shrink rows and Sonoma flicker with dynamic heights are UNVERIFIED single reports (threads 696976, 758302).
- With explicit `tableView(_:heightOfRow:)` and `usesAutomaticRowHeights = false` there is no estimation: every height is yours, so nothing above the viewport changes unless you say so. This is what agent-sessions and Memex do. The cost is that you must be able to measure a row without building its view.
- `noteHeightOfRows(withIndexesChanged:)` "immediately retiles" and, for view-based tables, "will animate"; wrap it in an `NSAnimationContext` group with `duration = 0`. VERIFIED (doc). Memex already does this (`TranscriptController.swift:539-542`).
- Row views are recycled. Recycling a row means rebuilding its markdown document, which is the dominant cost; FlowDown's answer is to never hand a row to another entry (see 1.4).

**`NSCollectionView` + compositional layout.** Available since macOS 10.15 (VERIFIED (doc)). Not used by any examined app; I found no source or measurement supporting it for variable-height streaming text on macOS. Self-sizing through estimated dimensions has the same estimate-then-correct jump class as automatic table heights. UNVERIFIED inference. No reason to prefer it over the table Memex already has.

**Single TextKit document for the whole transcript.**
- TextKit 2 lays out only the viewport and estimates total height; `usageBoundsForTextContainer` changes while scrolling so the scroller jumps, Apple calls it as-designed, and jumping to the end is "fragile". VERIFIED (transcript) https://blog.krzyzanowskim.com/2025/08/14/textkit-2-the-promised-land/ (author of STTextView). STTextView's README lists the open TextKit 2 radars (VERIFIED (src) `STTextView/README.md:233-256`); it is an editor component, not a transcript container.
- PicoMarkdownView is the best worked example of the approach at message scope: one `NSTextView` per streamed message, block-level diffs applied to `NSTextStorage`, hosted SwiftUI/AppKit attachments for tables and widgets, selection continuous across the document, and it requires macOS 27 for the TextKit 2 path. VERIFIED (src) `PicoMarkdownView/README.md:5,51-55`; `Sources/PicoMarkdownView/Views/TextKitStreamingController.swift:583-640,669-695`.
- Gains: free cross-message selection and the system find bar (`NSTextView.usesFindBar`, VERIFIED (doc)). Costs for Memex: tool cards, diffs, plans and libghostty terminals all become text attachments; stick-to-bottom depends on an estimated document height under TextKit 2; TextKit 1 with `allowsNonContiguousLayout` (VERIFIED (doc) that the property exists) avoids the estimate but lays out contiguously whenever you ask for the end of the document. UNVERIFIED cost judgement. Recommended only inside one message, not for the transcript.

**Custom list (ListViewKit, VVChatTimeline).** Both independently converge on the same core: a Fenwick tree of row heights so "appending a row, measuring a row and locating an offset are each O(log n)", each height flagged measured-or-estimate. VERIFIED (src) `ListViewKit/Sources/ListLayoutEngine.swift:8-18,27-35`; `VVDevKit/Docs/VVChatTimelinePerformance.md:78`. This is the ceiling for feel and the highest cost to own.

### 1.3 The scroll-jump problem and its one real fix

Every implementation that does not jump enforces the same invariant: **pick a visible anchor (row id + offset into row), and after any height change above the viewport, move the scroll origin by the delta in the same transaction, without animation.**

- ListViewKit: measuring returns the delta and the list compensates immediately.
  ```swift
  // ListViewKit/Sources/ListView.swift:702-711 @ cca344d
  private final func measureViewport() {
      rowLayout.prepareForLayout()
      let dy = rowLayout.measureRows(intersecting: mountRect, anchoredAt: viewportRect)
      if dy != 0 {
          compensateScrollOffset(by: dy)
      }
      scheduleSliceDrain()
  }
  ```
  VERIFIED (src).
- agent-sessions: `captureScrollAnchor()` records first visible row id and `bounds.minY - rowRect.minY`; `restoreScrollAnchor` re-finds the row, `layoutSubtreeIfNeeded()`, `contentView.scroll(to:)`, `reflectScrolledClipView`. VERIFIED (src) `TranscriptBlockListView.swift:3407-3426`. The same pair is what makes prepending history jump-free: capture, splice rows, restore.
- Memex already has the per-row form of this in its disclosure toggle path. VERIFIED (src) `TranscriptController.swift:520-552`.
- VVChatTimeline names the update kinds and validates them: `.tailAppend` may not force bottom, `.hydrateCorrection` may never scroll, `.jumpToLatest` must. VERIFIED (src) `VVDevKit/Sources/VVChatTimeline/VVChatTimelineUpdateSemantics.swift:16-33`. Encoding "why did the layout change" as a typed cause is worth copying: it turns scroll bugs into assertion failures.

Off-screen measurement must be budgeted. ListViewKit drains pending measurements in slices of 5 ms ("At 120Hz a frame is 8.3ms, so 5ms leaves room to draw"), pauses during user scrolling and live resize, and holds off 150 ms after a width change. VERIFIED (src) `ListViewKit/Sources/ListView+SliceDrain.swift:18-31,72-103`. FlowDown adds a 4 ms pass that re-typesets retained off-screen rows nearest the viewport first, default run-loop mode only so it never competes with a drag. VERIFIED (src) `FlowDown/.../MessageListView+RetainingListView.swift:35-42,114-165`.

Width changes are the other jump source: every cached height becomes an estimate. agent-sessions buckets width so sub-pixel churn keeps the cache valid (VERIFIED (src) `TranscriptBlockListView.swift:2129-2136,2190-2194`); FlowDown keeps an LRU pool of 24 sizing views so a width-only change "reframes what is already built" instead of rebuilding CoreText framesetters (VERIFIED (src) `MessageListView+SizingViewPool.swift:13-34`); Memex keeps glyph shaping across width changes in `TranscriptTextLayout` (VERIFIED (src) `TranscriptController.swift:1049-1087`).

### 1.4 Stick-to-bottom during streaming

Consensus design in source:

1. **Follow is a state, set by user intent, not derived each frame.** FlowDown: `isAutoScrollingToBottom` goes false in `scrollViewWillBeginDragging`, true again only when a drag or deceleration ends within 2 pt of the bottom, and true when the user sends. VERIFIED (src) `MessageListView.swift:30,52-55,207-216,343-361`. Aizen keeps `userScrolledUp` separate from `isNearBottom` and ignores scroll-position changes during layout resizes. VERIFIED (src) `ChatTimelineStore.swift:33,73-91,115-123`.
2. **Capture "was near bottom" before layout, re-pin after.** FlowDown does it inside `layoutSubviews`. VERIFIED (src) `MessageListView.swift:185-205`.
3. **Pin to the document end, not to the last row.** `scrollRowToVisible(last)` leaves a tall last row at its top; agent-sessions computes `rect(ofRow: last).maxY - clip.height` and scrolls the clip view. VERIFIED (src) `TranscriptBlockListView.swift:3366-3376`.
4. **When following, new rows appear at their final position with no positional animation.** "inserted items should appear at target position immediately — no accordion Y-offset. This avoids bounce on short items (tool calls, summaries)." VERIFIED (src) `VVDevKit/Docs/VVChatTimelinePerformance.md:54`.
5. **Do not recycle the streaming row.** FlowDown retains one row view per entry: a row leaving the viewport is only removed from the hierarchy and "comes back as the same view", so "the reply being written keeps the text in flight and the pacing of its stream when it is scrolled away and back." VERIFIED (src) `MessageListView+RetainingListView.swift:11-27,54-81`.
6. **Keep the live tail out of the settled layout.** Clarc renders the streaming message in a separate stack below the settled one ("text deltas don't affect settled layout") and rebuilds the settled array only at stream boundaries, with animations disabled in the transaction. VERIFIED (src) `MessageListView.swift:66-73,126-131,161-166`.

AppKit specifics for user intent: `NSScrollView.willStartLiveScrollNotification` is "Posted on the main thread at the beginning of user-initiated live scroll tracking (gesture scroll or scroller tracking...)" and `didEndLiveScrollNotification` at the end (macOS 10.9+). VERIFIED (doc). Those separate user scrolls from your own `scroll(to:)` calls, which `boundsDidChangeNotification` cannot (it fires for both; it requires `postsBoundsChangedNotifications = true`, VERIFIED (doc)).

Illustrative AppKit follow controller (uncompiled):

```swift
@MainActor
final class TranscriptFollow {
    private(set) var isFollowing = true
    private var userIsScrolling = false
    private let tolerance: CGFloat = 2            // FlowDown uses 2 pt
    unowned let scrollView: NSScrollView
    unowned let table: NSTableView

    init(scrollView: NSScrollView, table: NSTableView) {
        self.scrollView = scrollView; self.table = table
        let nc = NotificationCenter.default
        nc.addObserver(forName: NSScrollView.willStartLiveScrollNotification, object: scrollView, queue: .main) { [weak self] _ in
            MainActor.assumeIsolated { self?.userIsScrolling = true; self?.isFollowing = false }
        }
        nc.addObserver(forName: NSScrollView.didEndLiveScrollNotification, object: scrollView, queue: .main) { [weak self] _ in
            MainActor.assumeIsolated { self?.userIsScrolling = false; self?.isFollowing = self?.isAtBottom ?? true }
        }
    }

    var isAtBottom: Bool {
        let clip = scrollView.contentView
        return table.frame.maxY - clip.bounds.maxY <= tolerance
    }

    /// Wrap every mutation that can change document height.
    func performLayoutChange(anchorRowID: (() -> (row: Int, offset: CGFloat)?)? = nil, _ mutate: () -> Void) {
        let clip = scrollView.contentView
        let follow = isFollowing && !userIsScrolling
        let anchor = follow ? nil : anchorRowID?()
        NSAnimationContext.runAnimationGroup { ctx in
            ctx.duration = 0
            ctx.allowsImplicitAnimation = false
            mutate()                               // insertRows / noteHeightOfRows / reconfigure cells
        }
        table.layoutSubtreeIfNeeded()
        let y: CGFloat
        if follow {
            y = max(0, table.frame.maxY - clip.bounds.height)
        } else if let anchor {
            y = table.rect(ofRow: anchor.row).minY + anchor.offset
        } else { return }
        clip.scroll(to: NSPoint(x: 0, y: y))
        scrollView.reflectScrolledClipView(clip)
    }
}
```

The caller resolves the anchor row index after the mutation (by stable row id), as agent-sessions does.

### 1.5 Text selection across messages

Per-row SwiftUI `Text` cannot do it: with `.textSelection(.enabled)` on a container each `Text` stays individually selectable (VERIFIED (doc) describes per-view selection; cross-view limitation UNVERIFIED but universally reported). Three working patterns in source:

- **Coordinator over per-row `NSTextView`s** (fits Memex as-is). agent-sessions keeps intra-row selection native, escalates to a controller when a drag crosses a card boundary, then drives each visible row's `setSelectedRange` and routes Copy and Select All through the table. VERIFIED (src) `TranscriptBlockListView.swift:5351-5400` and `:4569`.
- **Selection group over custom labels.** Litext `TextSelectionGroup`: "A drag or a selection handle that leaves one member continues into the member under the pointer", Copy joins members with a separator, labels held weakly, has AppKit interaction files. VERIFIED (src) `Litext/Sources/Litext/TextLabelView/Selection/TextSelectionGroup.swift:17-29`. FlowDown uses it for table cells, not across messages.
- **One interaction overlay above many text fragments.** Textual puts an `NSTextInteractionView` over all rendered `Text` fragments of a document with a shared `TextSelectionModel` and exclusion rects for scrollable code blocks. VERIFIED (src) `textual/Sources/Textual/Internal/TextInteraction/AppKit/NSTextInteractionView.swift:5-12`. Document scope only; needs macOS 15 (`Package.swift` platforms).

Recycling interacts with selection: a coordinator must store selection in model coordinates (row id + character range), not in views, so a row scrolling out and back re-renders its part.

### 1.6 Find-in-transcript

- Search the model, not the views: off-screen rows have no views in any virtualized container. Memex already has `ConversationFind.swift`.
- If the system find bar is wanted over a table, `NSTextFinder` works with any client implementing `NSTextFinderClient`; the protocol's `string(at:effectiveRange:endsWithSearchBoundary:)` is defined for content "composed of a concatenation of all its substrings", which maps onto rows. VERIFIED (doc). No examined app does this (grep for `NSTextFinder` in agent-sessions returned nothing), so it is an option, not a proven path.
- Jump-to-match must go through the same anchor machinery: resolve the row, ensure it is measured (ListViewKit re-resolves an animated scroll destination after every layout pass because "the offset is only as good as the heights it was computed from", VERIFIED (src) `ListView+Scroll.swift:25-31`), then scroll.

### 1.7 What is new or fixed by OS release

- **macOS 15**: the whole modern scroll API set listed in 1.2; `textRenderer(_:)` modifier (macOS 15, VERIFIED (doc); the `TextRenderer` protocol itself is listed as macOS 14); `Mutex` (macOS 15, VERIFIED (doc)).
- **macOS 26**: List 6x/16x, better update scheduling, nested lazy stacks defer loading, a SwiftUI Instruments template with long-body-update lanes and a cause-and-effect graph (VERIFIED (transcript) WWDC25-256 and WWDC25-306); `Observations` async sequence for transactional `@Observable` change tracking (macOS 26, VERIFIED (doc)).
- **macOS 27**: lazy-stack prefetching performs "body evaluation and layout" before a view scrolls on screen; off-screen views and their `@State` are kept for some updates then dropped; `onScrollTargetVisibilityChange` recommended over absolute offsets (VERIFIED (transcript) WWDC26-321). One unresolved regression report on `scrollTo` with unrealized lazy rows (UNVERIFIED, 1.2).
- None of this changes the conclusion for a macOS 14 deployment target: the APIs that would make a SwiftUI transcript tolerable are gated at 15+, and the anchoring behaviour differs by release.

### Recommendation (section 1)

Keep the view-based `NSTableView` with explicit `heightOfRow` and a height cache. It is the only system container where no height is ever estimated behind your back, it is what Memex's reader and agent-sessions already do, and it works on macOS 14. Add, in this order:

1. One `performLayoutChange` choke point (1.4) through which every height-affecting mutation passes, with a typed cause (tail append, stream update, hydrate correction, prepend, jump to latest) and an explicit follow-or-anchor decision. No animated `noteHeightOfRows`, ever.
2. Follow state driven by live-scroll notifications plus "user sent a message", never by raw bounds changes.
3. The streaming row exempt from recycling (hold a strong reference to its cell view while the turn is live), or rendered as a live-tail view outside the table and committed to a row at identical height when the turn settles.
4. Budgeted off-screen measurement after width changes (4-5 ms slices, paused while scrolling), nearest-viewport first.
5. Cross-row selection via a model-coordinate coordinator over the existing per-row text views.

Only consider ListViewKit (it has a native AppKit path and one small dependency) or a custom list if profiling shows `NSTableView` retiling itself is the bottleneck; ListViewKit's own comment that re-tiling an `NSScrollView` costs "an order of magnitude more work than a scroll frame's own layout" (VERIFIED (src) `ListScrollView.swift:626-630`) is the argument for it, but it is their measurement, not mine.

---

## 2. Streaming markdown without jank

### 2.1 The shape every good implementation shares

1. Coalesce tokens before they reach layout (20-33 Hz, not per token, not per frame).
2. Close the unfinished tail speculatively so markers never flash.
3. Identify blocks by position; completed blocks keep their rendered fragment and their view.
4. Rebuild or patch only the tail; cap rebuild frequency by measured cost.
5. Animate new glyphs with a cheap per-frame fade that touches only the lines in flight.
6. On completion, parse once more without the speculative repair and land on the exact text.

### 2.2 Update cadence found in source

| Implementation | Coalescing | Evidence |
|---|---|---|
| FlowDown text emitter | 20 ticks over 0.5 s; degrades to 15 Hz/0.5 s past 1,000 chars, 9 Hz/1 s past 2,000, 3 Hz/1 s past 5,000, because "Large outputs make each Markdown layout pass slow (~0.1s)" | VERIFIED (src) `FlowDown/Backend/Model/ModelManager+Inference.swift:368-373,376-396` |
| FlowDown reasoning emitter | 30 ticks over 1.0 s | VERIFIED (src) same file `:362-367` |
| Lakr233 MarkdownView (idle) | Combine throttle, default 1/20 s | VERIFIED (src) `MarkdownView/Sources/MarkdownView/MarkdownTextView/MarkdownTextView.swift:65`; `MarkdownTextView+Private.swift:65-70` |
| Lakr233 MarkdownStreamView (streaming) | Cost-adaptive: wait = rebuild cost / 0.15, clamped to 1/30...1/8 s; timer in `.common` mode "so the answer keeps arriving while the reader scrolls" | VERIFIED (src) `MarkdownStreamView.swift:103-109,161-200,210-217` |
| Aizen | Agent text flushed every 50 ms, thoughts every 60 ms, message finalized after 200 ms idle | VERIFIED (src) `aizen/Features/Chat/Infrastructure/ACP/ChatAgentSession.swift:75,85,89`; `ChatAgentSession+StreamingFinalization.swift:50-56,98-110` |
| VVChatTimeline draft throttler | 30 ms, latest wins | VERIFIED (src) `VVDevKit/Sources/VVChatTimeline/VVChatDraftThrottler.swift:10,33-47` |
| LiYanan2004 MarkdownView | 50 ms default render throttle | VERIFIED (src) `Sources/MarkdownView/Parser/Streaming/MarkdownStreamingRenderThrottle+Environment.swift:9` |
| Enchanted / claude-island | 100 ms throttler / 50 ms Combine throttle | VERIFIED (src) `Enchanted/Stores/ConversationStore.swift:25`; `ClaudeIsland/Core/NotchViewModel.swift:136` |
| Microsoft SwiftStreamingMarkdown | None; each snapshot is parsed in turn off-main and published | VERIFIED (src) `Sources/MarkdownText/StreamedMarkdownView.swift:84-97` |

Takeaway: nobody relayouts markdown at display rate. Smoothness at 120 Hz comes from the glyph fade and from scrolling staying free, not from 120 layouts per second. The adaptive rule (rebuild no more often than cost/0.15) is the best of these because it self-tunes for long messages instead of hard-coding thresholds.

### 2.3 Smoothing bursty tokens into an even cadence

FlowDown's `BalancedEmitter` is an actor that buffers chunks and emits `ceil(buffer.count / frequency)` characters per tick with a tick of `duration / frequency`, so any backlog drains over roughly `duration` regardless of burst size.

```swift
// FlowDown/Backend/Model/BalancedEmitter.swift:40-45, 68-77 @ b6044ae (abridged)
func add(_ chunk: String) {
    guard !chunk.isEmpty else { return }
    buffer += chunk
    batchSize = max(1, Int(ceil(Double(buffer.count) / Double(frequency))))
    dispatchLoopIfRequired()
}
// loop: emit prefix(batchSize); Task.sleep(stepDelay) where
// stepDelay = (duration * 1000) / Double(frequency)
```
VERIFIED (src). Caveat visible in that file: the loop does `await MainActor.run { SoundEffectPlayer.shared.play() }` per tick (`:74`), a main hop per emission that Memex should not copy.

The same idea as a frame-driven governor (illustrative, uncompiled). Backlog decays with time constant `targetLag`, so bursts are absorbed and the text never falls more than about `targetLag` behind:

```swift
struct RevealGovernor {
    var targetLag: Double = 0.30      // seconds of backlog tolerated; FlowDown's text emitter uses 0.5
    var minRate: Double = 60          // chars/s floor so short replies still flow
    private var carry: Double = 0

    mutating func take(backlog: Int, dt: Double) -> Int {
        guard backlog > 0 else { carry = 0; return 0 }
        let rate = max(minRate, Double(backlog) / targetLag)
        carry += rate * dt
        let n = min(backlog, Int(carry))
        carry -= Double(n)
        return n
    }
}
```

Reveal on grapheme-cluster boundaries (Litext's default granularity is `.cluster`, VERIFIED (src) `LTXFadeInAnimator.swift:68`). When the stream ends, do not snap: FlowDown keeps `isStreaming` on until the last chunk's fade finishes, then turns it off. VERIFIED (src) `FlowDown/.../Components/AiMessageView.swift:58-88`.

### 2.4 Unclosed constructs mid-stream

Lakr233's `StreamingTail` is the most complete treatment. It computes a repair for the document end and never touches the middle: "The repaired text is the first `keptLength` UTF-8 bytes of the document followed by `suffix`... So every block before the one being written stays byte for byte what it was, and a rebuild keeps reusing them." It holds back a last line of bare markers (`-`, `1.`, `#`, `>`, `**`, a setext `---`), holds back an unfinished fence info line "so a language is not guessed from half its name", closes emphasis/strong/strikethrough/code spans opened on the last line, closes an unfinished link destination, drops an unfinished image, gives a table header its delimiter row early, holds back a partial HTML tag or entity, and changes nothing inside fenced code or unclosed `$$`. VERIFIED (src) `MarkdownView/Sources/MarkdownParser/MarkdownParser/MarkdownParser+StreamingTail.swift:23-42`; scanner at `StreamingTailScanner.swift:8-80`. Design rule stated there: "a wrong guess shows up as text snapping into a different shape, which is what the repair exists to avoid" (`:12-15`), so it leans toward changing nothing.

Microsoft does the equivalent after parsing, as swift-markdown `MarkupRewriter`s on the last text node (partial strong/emphasis via regex, partial tables). VERIFIED (src) `Sources/MarkdownText/Parser/Rewriters/PartialEmphasisRewriter.swift:8-60`; `Parser/MarkdownParserImpl.swift:11-14,34-40`. Since Memex uses swift-markdown, this is the directly portable variant; the pre-parse text repair is more thorough and parser-independent.

Both end the same way: the finished text is parsed again without the repair. VERIFIED (src) `FlowDown/.../MessageListView+NodesCache.swift:66-69`; LiYanan `StreamingMarkdownReader.swift:75-80` ("Perform a full parse to make sure the content renders correctly... e.g. link reference").

### 2.5 Reparse only the tail; stable block identity

Three verified strategies, in increasing cleverness:

- **Reparse everything, reuse rendered blocks.** Lakr233 runs cmark-gfm on the whole string each rebuild (`MarkdownParser.swift:41-59`) and relies on `BlockFragmentCache`: "Entries are matched on position and node together, never on the node alone... positions are stable and only the tail changes", and code blocks/tables are cached "together with the view each one placed". VERIFIED (src) `MarkdownView/Sources/MarkdownView/MarkdownTextBuilder/BlockFragmentCache.swift:11-31,100-106`. cmark is fast enough that parse is not the cost; attributed-string building and layout are.
- **Stable prefix + buffer.** VVDevKit keeps `stableBlocks` and parses only the newly stabilized suffix plus the trailing buffer, falling back to a full rebuild when `content.hasPrefix(state.content)` fails. VERIFIED (src) `VVDevKit/Sources/VVChatTimeline/VVChatMessageRenderer.swift:1866-1920`. Their perf doc: "`draftRebuild` count should be ~2/turn" (`Docs/VVChatTimelinePerformance.md:42`).
- **Incremental reparse on swift-markdown.** LiYanan2004 keeps root-block source ranges, walks back from the last block to the nearest boundary separated by a blank line, reparses the source from there, and grafts the new tail onto the previous document's stable children with `withUncheckedChildren`.
  ```swift
  // LiYanan2004/MarkdownView Sources/MarkdownView/Parser/MarkdownDocumentParser.swift:594-608 @ bc8f5fb
  var reparsedRootBlockIndex = rootBlockRanges.count - 1
  while reparsedRootBlockIndex > 0 {
      let previousRootBlockRange = rootBlockRanges[reparsedRootBlockIndex - 1]
      let currentRootBlockRange = rootBlockRanges[reparsedRootBlockIndex]
      let separatorText = sourceText[
          previousRootBlockRange.upperBound..<currentRootBlockRange.lowerBound
      ]
      if separatorText.contains("\n\n") { break }
      reparsedRootBlockIndex -= 1
  }
  ```
  VERIFIED (src); merge at `:55-136`. It assumes append-only input (`guard newSourceText.hasPrefix(previousSourceText)`, `:71`).

Identity is positional everywhere: Microsoft derives a block id from child indices (`Models/Markup+ID.swift:12-20`), Pico uses `BlockID`s with an `indexByID` table, Lakr uses slot index. A content-hash id would re-create the tail block on every token; a UUID would re-create everything. One wrinkle in Microsoft's `Markup+ID`: the loop appends `self.indexInParent` once per ancestor rather than each ancestor's index, so nested blocks at the same depth and sibling index share an id; harmless inside a per-parent `ForEach`, wrong as a global key. VERIFIED (src), consequence is my reading.

**Patch the text storage, do not replace it.** Pico turns parser output into typed diffs (`blockStarted`, `runsAppended`, `codeAppended`, `tableRowAppended`, `blockEnded`, `blocksDiscarded`) and edits only the affected range; for an open paragraph it keeps the already-rendered prefix and replaces only the tail.

```swift
// PicoMarkdownView Sources/PicoMarkdownView/Views/TextKitStreamingController.swift:685-690 @ f192326
// Preserve the already-rendered prefix of an open paragraph/code block.
// Its terminal newline moves as text streams; only that tail is edited.
let prefixLength = reusablePrefixLength(old: record.nsAttributed, new: newAttributed)
let edit = NSRange(location: range.location + prefixLength, length: range.length - prefixLength)
let replacement = newAttributed.attributedSubstring(from: NSRange(location: prefixLength, length: newAttributed.length - prefixLength))
storage.replaceCharacters(in: edit, with: replacement)
```
VERIFIED (src). With TextKit 1 this keeps layout for everything before the edit; `setAttributedString` (what Microsoft's `ParagraphNSView` does per paragraph, `ParagraphNSView.swift:136`) invalidates the whole paragraph, which is acceptable only because their unit is one paragraph.

### 2.6 AttributedString vs NSAttributedString vs custom layout

- Every renderer here that draws through AppKit/UIKit text builds `NSAttributedString` directly: Lakr233 (fragments cached as `NSAttributedString`), Microsoft (`NSMutableAttributedString` in renderables, TextKit 1 `NSTextView` per paragraph on macOS, `ParagraphNSView.swift:18,33-42`), Pico (`NSTextStorage`), Memex (`TranscriptTextLayout`). VERIFIED (src).
- SwiftUI-`Text` renderers (swift-markdown-ui, Textual, Clarc's own `MarkdownView`) get composability and lose control over partial updates; swift-markdown-ui is in maintenance mode with development moved to Textual (VERIFIED (src) `swift-markdown-ui/README.md:1-9`), and Textual targets macOS 15+ (`textual/Package.swift`). Neither has a streaming path (grep for stream/incremental in Textual finds only text-position code).
- Custom layout: Litext (CoreText, own selection and attachments, AppKit files present) and VVMarkdown (Metal glyph batching). These are where per-glyph fade and partial redraw come from.
- For Memex: stay on `NSAttributedString` + TextKit 1 for measurement and display. Do not round-trip through Swift `AttributedString` per update (UNVERIFIED performance claim; no source measured it, but none of the fast paths do it).

### 2.7 Off-main work

- Parse and block conversion off-main, one main hop to publish: Microsoft (`StreamedMarkdownView.swift:86-95`), FlowDown (a `userInteractive` serial queue builds entries and pre-parses, then one `Task { @MainActor }` applies; `MessageListView.swift:27,39-51,287-306,315`). VERIFIED (src).
- Know what stays on main. FlowDown's cache builds `MarkdownContent` via `DispatchQueue.main.sync` from the worker (`MessageListView+NodesCache.swift:45-59`), because math images need the main actor. VERIFIED (src). A synchronous hop from a worker is a deadlock hazard and a hidden main-thread cost; prefer making the off-main product fully `Sendable` and finishing main-only pieces during apply.
- VVDevKit warns that a renderer that is not `@MainActor` must wrap "pre-await shared state reads AND post-await cache writes in `MainActor.run {}`" or crash in `Dictionary._Variant`. VERIFIED (src) `Docs/VVChatTimelinePerformance.md:48`.
- Text measurement with `NSLayoutManager` on a background thread is possible only with private text storage/layout objects never attached to a view. UNVERIFIED (general AppKit knowledge; no examined app measures off-main, they budget on main instead).

### 2.8 Syntax highlighting cost and caching

Lakr233's pipeline is the model: highlight on a `userInitiated` queue, cache maps by key, post a notification naming the finished keys, and each view recolors only code views it shows without relayout ("Colour never changes a block's size, so the document and its layout stay exactly as they are"). Before that fix "a code block finishing in one message used to rebuild all of them." VERIFIED (src) `MarkdownView/Sources/MarkdownView/MarkdownTextView/MarkdownTextView+Private.swift:17-61`; `Components/CodeView/CodeHighlighter.swift:131-140`. The streaming tail also withholds the fence info line until complete so the language is not guessed from a prefix (2.4). Rules that follow: highlighting must never change metrics (same font, no bold-width change), must be keyed by (language, content hash), and for an open fence should run at a much lower cadence than text (or only on fence close) since each run rescans the block. Microsoft and LiYanan use highlight.js wrappers (HighlightSwift, Highlightr per their `Package.swift`); Aizen uses Tree-sitter through VVDevKit (`aizen/AGENTS.md:209`). Memex has its own `CodeSyntax.swift`; I did not read it.

### 2.9 Fade-in of new text

- **Litext `LTXFadeInAnimator`** (CoreText, AppKit-capable): one batch per change, at most 24 in flight; opacity quantized to 32 levels so "a frame invalidates a batch's lines only when one of its units moved to another step"; a line in flight is drawn run by run with `CTRunDraw` at each stretch's alpha while finished text on the same line is drawn opaque; only the strips of lines being animated live in a separate layer that is redrawn per frame. Defaults: 0.32 s ease-out, stagger 14 ms capped at 0.16 s. Reduce Motion keeps "a short, plain fade: no stagger and no rise". VERIFIED (src) `Litext/Sources/LitextAnimation/LTXFadeInAnimator.swift:23-39,66-75`; `LTXAnimatableLabel+AnimationLayer.swift:21-37`.
- **FlowDown's tuning on top**: 0.5 s linear per character, stagger 20 ms capped at 0.25 s, 32 batches; new code blocks and tables fade in whole over the same duration; only appended text animates so a reused block view never replays. VERIFIED (src) `FlowDown/.../Support/ChatMarkdownView.swift:15-31,42-43`; `MarkdownStreamView.swift:263-275`.
- **Microsoft on macOS**: `NSTextView` subclass with a `CADisplayLink`, new range split into words, each word fades 0.5 s, total stagger 0.1 s spread across the words, by rewriting foreground-color alpha attributes each frame. VERIFIED (src) `ParagraphNSView.swift:25,144-163,244-260`; `UI/Paragraph/ParagraphAnimation.swift:9-10`. Simple and compatible with TextKit 1, but attribute edits invalidate glyph display for those ranges every frame.
- **SwiftUI `TextRenderer`**: per-glyph opacity via `Text.Layout` run slices, eased with `UnitCurve.easeOut`, driven as an `Animatable` renderer. VERIFIED (src) `Sources/MarkdownText/TextTransition/FadeInTextTransition.swift:75-102`. Requires macOS 15 for the modifier and, being a custom `Animatable`, runs view code every frame (see 4.2).

A cheaper fade that fits a TextKit 1 `NSTextView` row and needs no per-frame text work: put a `CAGradientLayer` mask over only the last line fragment's new glyph rect and animate its `locations` once per reveal batch (render-server animation, section 4). UNVERIFIED design; no examined app does exactly this.

### 2.10 Reference streaming packages, assessed

| Package | Approach | Fit for Memex |
|---|---|---|
| Lakr233/MarkdownView + Litext | cmark-gfm, tail repair, block fragment cache, CoreText labels, per-glyph fade; macOS 13+, AppKit code paths | Best feel. Different parser (cmark-gfm directly) and its own text engine; adopt ideas or the whole stack, not half |
| microsoft/SwiftStreamingMarkdown | swift-markdown 0.7.3, off-main full reparse, positional ids, `@Equatable` SwiftUI block views, TextKit 1 paragraphs, word fade; macOS 14+ | Same parser as Memex; good source for partial-markup rewriters and the AppKit paragraph view |
| LiYanan2004/MarkdownView | swift-markdown 0.8, true incremental tail reparse, 50 ms throttle, final full parse | Best source for incremental parsing on swift-markdown |
| PicoMLX/PicoMarkdownView | Own incremental tokenizer off-main, block diffs into one text storage, hosted attachments; macOS 27 for the TextKit 2 path | Best source for diff-patching `NSTextStorage` and selection-preserving edits |
| vivy-company/VVDevKit | Own parser, stable prefix + buffer, Metal rendering | Reference for update semantics and perf notes, not a dependency |
| gonzalezreal/textual, swift-markdown-ui | SwiftUI `Text` pipeline; no streaming path; Textual has the selection overlay | Selection design only |
| dnakov/hairball | SwiftUI; parse cache added because the parser "ran on every streaming token and on every scroll frame" from `View.init` | Cautionary example (VERIFIED (src) `Sources/HairballUI/Support/MarkdownParseCache.swift:4-12`) |
| krzyzanowskim/STTextView | TextKit 2 editor | Not a transcript renderer |

### Recommendation (section 2)

Keep swift-markdown and `NSAttributedString`. Build a per-message streaming state with: (a) an append-only source buffer; (b) a stable prefix of rendered blocks keyed by position, promoted when a blank-line boundary outside any open fence is passed (LiYanan's rule); (c) tail-only reparse of the open region after a speculative close (port the Lakr233 `StreamingTail` rules or Microsoft's rewriters); (d) prefix-preserving `replaceCharacters` into the row's text storage (Pico); (e) a cost-adaptive rebuild cap of `cost / 0.15` clamped to 1/30...1/8 s (Lakr233); (f) a reveal governor with about 0.3 s lag; (g) highlight off-main, keyed, recolor-only; (h) on completion, full parse without repair, diff blocks by (position, content), and replace only blocks that differ so the settled row is pixel-identical when nothing changed. Height changes go through the section 1 choke point. Sketch (illustrative, uncompiled):

```swift
struct StreamingMarkdown {
    private(set) var source = ""
    private(set) var stable: [RenderedBlock] = []       // never rebuilt while streaming
    private(set) var tail: [RenderedBlock] = []          // rebuilt on each pass
    private var stableEnd: String.Index

    init() { stableEnd = "".startIndex }

    /// Returns the index of the first block whose rendering changed.
    mutating func append(_ delta: String, render: (Markup, _ position: Int) -> RenderedBlock) -> Int {
        let offset = source.distance(from: source.startIndex, to: stableEnd)
        source += delta
        stableEnd = source.index(source.startIndex, offsetBy: offset)

        let open = String(source[stableEnd...])
        let doc = Document(parsing: StreamingTail.close(open))          // speculative close, tail only
        let blocks = Array(doc.blockChildren)
        // Promote every block that ends before the last blank-line boundary and is not an open fence.
        let promote = StreamingTail.settledBlockCount(in: open, blocks: blocks)
        let firstChanged = stable.count
        for (i, b) in blocks.prefix(promote).enumerated() { stable.append(render(b, firstChanged + i)) }
        if promote > 0, let end = blocks[promote - 1].range?.upperBound {
            stableEnd = open.index(atSourceLocation: end, base: stableEnd, in: source)   // helper: map line/column to index
        }
        tail = blocks.dropFirst(promote).enumerated().map { render($1, stable.count + $0) }
        return firstChanged
    }
}
```

Known hazards to test: link reference definitions that arrive after their use (fixed only by the final full parse), lists changing between tight and loose, setext headings, lazy continuation lines, and a table header that turns out not to be a table.

---

## 3. Open-source native Swift AI chat / agent apps

### 3.1 Ranking by what they teach about feel

1. **Lakr233/FlowDown** (with ListViewKit, MarkdownView, Litext). The most deliberate feel engineering I found: retained rows, time-budgeted pre-layout, tail repair, per-glyph fade, burst smoothing, shimmer as a mask, rate-limited status label. UIKit/Catalyst app; the three libraries have native AppKit paths.
2. **vivy-company/aizen + VVDevKit**. Native macOS ACP client (same domain as the PR: agent text, thoughts, tool calls, plans, diffs, permissions), Metal timeline, explicit update semantics, and a written performance guide from profiling "90+ turns, 0.02s chunk delay".
3. **jazzyalex/agent-sessions**. AppKit `NSTableView` transcript with height cache, anchor capture/restore, document-end pinning and cross-block selection: the closest structural match to Memex.

Also useful: **ttnear/Clarc** (cleanest macOS 15 SwiftUI scroll API usage and the settled/streaming split), **PicoMarkdownView** (TextKit diff patching), **microsoft/SwiftStreamingMarkdown** (swift-markdown streaming).

Surveyed by grep only, nothing exemplary for feel: Enchanted (`ScrollViewReader`, 100 ms throttle), Sidekick (250 ms debounced persistence saves, `MessageStreamCoordinator.swift:26`), Onit, claude-island (inverted `LazyVStack`). Not found or not examined: RxCode (product site only, no repository located), Kiln, Foundry, Canopy, Dimillian's projects (UNVERIFIED that any has a native Swift transcript worth studying).

### 3.2 FlowDown: transcript view, streaming path, state

- **State**: no Observation. A `CurrentValueSubject<([Message], Bool), Never>` per session, combined with activity text and an executing flag, delivered on a private `userInteractive` queue. VERIFIED (src) `FlowDown/Backend/Conversation/Pipeline/ConversationSession.swift:24-30`; `MessageListView.swift:27,39-51`.
- **Off-main**: on that queue, entries are built and every AI message is pre-parsed into the markdown cache; then one main-actor task applies the entry array. VERIFIED (src) `MessageListView.swift:287-339`.
- **Diffing**: `listView.apply(entries, animated: true)` diffs by `Entry.ID` inside ListViewKit (`ListDifference.swift`); the row for an unchanged id is reconfigured, and `setMarkdownPackage` skips work when the package object is identical (`AiMessageView.swift:71-79`).
- **Caches**: parse cache keyed by message id with (content hash, isStreaming, theme) validity (`MessageListView+NodesCache.swift:17-43`); sizing view pool (LRU 24); block fragment cache inside the markdown view.
- **Session switch**: list alpha set to 0 on session change, first apply is unanimated, scroll set to max offset, fade to 1 over 0.25 s after 0.1 s. VERIFIED (src) `MessageListView.swift:32-37,317-327`. Simple, hides the first-layout scramble; costs about 350 ms of perceived latency, which "instant session switching" should beat by keeping per-session height caches warm.
- **Status label**: changes at most once a second so "a status flipping quickly never cuts a transition short". VERIFIED (src) `Components/Support/ThinkingStatusLabel.swift:4-10,88-108`. Worth copying for tool-call status lines.
- **Weak spots**: `DispatchQueue.main.sync` from the worker; a main hop per smoothing tick for a sound effect; `#if DEBUG` scroll diagnostics left in the hot path (`MessageListView.swift:111-133`).

### 3.3 Aizen + VVDevKit

- **Streaming path**: ACP chunks append to `pendingAgentText`; a 50 ms task flushes by replacing the last incomplete agent `MessageItem` in the `messages` array (whole-array reassignment). VERIFIED (src) `ChatAgentSession+StreamingFinalization.swift:50-110`.
- **State**: `ObservableObject` with `@Published var messages`/`toolCalls` (`ChatTimelineStore.swift:25-30`). The SwiftUI wrapper hashes every message and tool call into a `timelineSignature` on each body evaluation and uses `.task(id:)` to resync the controller. VERIFIED (src) `ChatMessageList.swift:58-76,109-115`. That is O(transcript) per flush on the main thread and would not scale to thousands of entries without the trimming they do (`trimMessagesIfNeeded`, `ChatAgentSession+MessageMutation.swift:120-124`). The whole-transcript invalidation is survivable only because SwiftUI owns exactly one node (the Metal host); everything else is the controller's diff.
- **Timeline**: typed update causes and follow policies (`VVChatTimelineUpdateSemantics.swift`), draft API (`beginStreamingAssistantMessage`, `appendToDraftMessage`, `finalizeMessage`, `VVChatTimelineController.swift:247-282`), incremental draft relayout from the common block prefix.
- **Perf notes worth reading in full** (`Docs/VVChatTimelinePerformance.md`): `syncPublicState` "was re-mapping 3 arrays... on every draft update" (fixed by precomputing in the snapshot); an LRU touch that was O(n) per access; an icon lookup doing `NSWorkspace.icon(for:)` plus TIFF encoding on main per file per turn; `isApplyingUpdate` gating scroll handling during apply "to prevent intermediate renders". These are the classes of bug to look for in the Memex PR.
- Metal is not a recommendation for Memex: it forfeits system text selection, accessibility and find, which VVDevKit reimplements.

### 3.4 agent-sessions

- Imperative `BlockTableController` owning rows, height cache, selection and scroll anchoring (`TranscriptBlockListView.swift:488,673`). Height cache keyed by (row id, width bucket, font bucket, image count); a counter of cache misses exists so tests can assert reuse (`:1307`). Markdown rows measure "the RENDERED attributed string (same one the cell displays)" (`:2201-2209`): measuring and drawing from the same object is what prevents the one-frame height correction flash.
- Windowed loading with `widen(toIncludeBlock:)` that splices rows "with the viewport pinned (no jump)" (`:3428-3440`).
- It is a transcript reader, not a live chat, and the file is over 5,000 lines; take the mechanisms, not the structure.

### 3.5 State-management guidance drawn from these

- Apple's own trace-based guidance: when each row reads a shared array, "the @Observable macro has created a dependency for each view on the whole array", so "every item view's body runs"; the fix is a per-item observable model reached through an `@ObservationIgnored` dictionary. VERIFIED (transcript) WWDC25 session 306, https://developer.apple.com/videos/play/wwdc2025/306/. Also: do not put frequently changing values (geometry, timers) in the environment.
- For a transcript: the transcript model publishes only structural changes (ids inserted/removed/reordered); each entry is its own `@Observable` (or, in AppKit, the controller pushes to the one affected cell). Streaming text mutates one entry model; nothing that observes the list re-evaluates. Clarc gets a cheap version of this by extracting the message list so it alone depends on `messages` and by snapshotting settled items into `@State` (`MessageListView.swift:5,10,161-166`). Microsoft marks every block view `@Equatable` via a macro package so unchanged blocks skip body (`StreamedMarkdownView.swift:24`, `DocumentView.swift:14`).
- In the AppKit transcript Memex has, the equivalent is: never `reloadData()` during a stream; `insertRows(at:withAnimation: [])` for new entries (macOS 10.7+, VERIFIED (doc)) and direct reconfiguration of the one live cell.
- `Observations` (macOS 26) gives transactional async change streams but is unavailable at the macOS 14 floor. VERIFIED (doc).

### Recommendation (section 3)

Read, in this order: FlowDown `MessageListView.swift`, `MessageListView+RetainingListView.swift`, `MessageListView+SizingViewPool.swift`, `Components/AiMessageView.swift`, `Components/Support/ChatMarkdownView.swift`; Lakr233/MarkdownView `MarkdownStreamView.swift`, `MarkdownParser+StreamingTail.swift`, `BlockFragmentCache.swift`; VVDevKit `Docs/VVChatTimelinePerformance.md` and `VVChatTimelineUpdateSemantics.swift`; agent-sessions `TranscriptBlockListView.swift:2089-2222,3360-3426,5351-5400`. Use Aizen's perf doc as a review checklist against the PR.

---

## 4. Shimmer and in-progress affordances

### 4.1 Reference parameters found in source

| Source | Sweep | Curve | Resting vs peak | Band | Evidence |
|---|---|---|---|---|---|
| FlowDown `ShimmerLayer` | 1.5 s, repeats forever | linear (default `CABasicAnimation`, no timing function set) | mask alpha 0.5 vs 1.0 | 0.4 of masked width (locations `[-0.4,-0.2,0]` to `[1,1.2,1.4]`) | VERIFIED (src) `FlowDown/.../Components/Support/ShimmerLayer.swift:7-39` |
| markiv/SwiftUI-Shimmer | 1.5 s + 0.25 s delay per cycle | linear | 0.3 vs 1.0 | 0.3 | VERIFIED (src) `Sources/Shimmer/Shimmer.swift:48-55` |
| OpenAI Codex TUI | 2.0 s period, phase locked to process start | linear position, raised-cosine band | blend toward highlight up to 0.9 | half-width 5 cells, 10 cells of padding each side | VERIFIED (src) `codex-rs/tui/src/shimmer.rs:27-36,46-54` |

The ChatGPT and Claude desktop apps' exact values are not public; anything more specific than "about 1.5-2 s linear sweep, soft band, text dimmed to roughly half" is UNVERIFIED. Two details from FlowDown matter more than the numbers: size the mask to the text width, not the view ("a band sweeping a wide, mostly empty view crosses the words for only a moment"), and make the mask's resting alpha equal to the dim level the settled state uses so starting and stopping the shimmer does not change the text's brightness. VERIFIED (src) `ShimmerLayer.swift:3-6`; `ThinkingStatusLabel.swift:120-125`.

### 4.2 Which implementations keep animating when the main thread stalls

- **`CAGradientLayer.locations` animated with `CABasicAnimation`**: `locations` is documented "Animatable" (VERIFIED (doc)). An explicit `CAAnimation` added to a layer is interpolated by Core Animation's renderer, not by app code per frame, so it continues through main-thread stalls. That last clause is standard Core Animation behaviour but I could not find an Apple sentence stating it for macOS (the Core Animation guide only says work is handed to graphics hardware); community tests agree. UNVERIFIED as a documented guarantee. FlowDown relies on it and sets `isRemovedOnCompletion = false` so the animation "outlives a trip to the background". Verify in Memex by blocking main for 500 ms in a debug build while the shimmer runs.
- **SwiftUI built-in animatable effects**: "For built-in animatable attributes like scaleEffect, SwiftUI is very efficient. It's able to do this work off the main thread and without calling into any of your view code." VERIFIED (transcript) WWDC23 session 10156. Whether an animated `LinearGradient` start/end point used as a `.mask` (the SwiftUI-Shimmer technique) gets that treatment on macOS is UNVERIFIED; assume not.
- **Custom `Animatable` / `TextRenderer` animation**: "for every frame of the animation, body is going to be called". VERIFIED (transcript) same session. Stalls with main.
- **`TimelineView(.animation)`**: redraws its content on a schedule (VERIFIED (doc)); content evaluation is view code on main. Stalls with main, and re-evaluates every frame for the whole time it is on screen.
- **`CADisplayLink`-driven drawing** (Litext fade, Microsoft word fade, `NSView.displayLink(target:selector:)` macOS 14+, VERIFIED (doc)): callbacks are on main. Stalls with main.
- **SF Symbols effects** (`.symbolEffect(.pulse)`, `.variableColor` macOS 14+; `.breathe` macOS 15+; AppKit `NSImageView.addSymbolEffect`): availability VERIFIED (doc). Where they are rendered is UNVERIFIED; they are system-driven and cheap, and a reasonable choice for small glyph affordances (tool running, connecting).
- **`.redacted(reason: .placeholder)`** (macOS 11+, VERIFIED (doc)): static skeleton shapes, no motion by itself; combine with a mask shimmer for loading a session's first page. For a transcript, skeleton rows should have the cached heights of the real rows so the swap does not move anything.

So: status-text shimmer and skeleton shimmer should be Core Animation mask animations; the glyph fade may stay display-link driven because a stalled main thread means no new text anyway.

### 4.3 Copyable implementation (illustrative, uncompiled, macOS 14+)

```swift
import AppKit
import QuartzCore

/// Alpha-only gradient used as a mask: text under it rests at `restingAlpha` and a bright band sweeps across.
final class ShimmerMaskLayer: CAGradientLayer {
    static let period: CFTimeInterval = 1.8     // FlowDown 1.5, Codex 2.0
    static let restingAlpha: CGFloat = 0.5      // equals the settled "dim" style
    static let band: Double = 0.30              // fraction of the text width, feathered both sides
    private static let key = "shimmer"

    override init() {
        super.init()
        startPoint = CGPoint(x: 0, y: 0.5)
        endPoint = CGPoint(x: 1, y: 0.5)
        let dim = NSColor.white.withAlphaComponent(Self.restingAlpha).cgColor
        colors = [dim, NSColor.white.cgColor, dim]
        locations = [0, 0, 0]
        actions = ["bounds": NSNull(), "position": NSNull(), "locations": NSNull()]  // no implicit animations
    }
    override init(layer: Any) { super.init(layer: layer) }
    @available(*, unavailable) required init?(coder: NSCoder) { fatalError() }

    func start() {
        guard animation(forKey: Self.key) == nil else { return }
        let half = Self.band / 2
        let sweep = CABasicAnimation(keyPath: "locations")
        sweep.fromValue = [-Self.band, -half, 0.0]
        sweep.toValue = [1.0, 1.0 + half, 1.0 + Self.band]
        sweep.duration = Self.period
        sweep.timingFunction = CAMediaTimingFunction(name: .linear)
        sweep.repeatCount = .infinity
        sweep.isRemovedOnCompletion = false
        // Phase-lock all shimmers so two status lines never drift (Codex locks to process start).
        sweep.timeOffset = CACurrentMediaTime().truncatingRemainder(dividingBy: Self.period)
        add(sweep, forKey: Self.key)
    }
    func stop() { removeAnimation(forKey: Self.key) }
}

/// Hosts any text view and sweeps a shimmer over the text's own width.
final class ShimmerContainerView: NSView {
    private let mask = ShimmerMaskLayer()
    private var observer: NSObjectProtocol?
    var textWidth: CGFloat = 0 { didSet { needsLayout = true } }
    var isActive = false { didSet { if isActive != oldValue { update() } } }

    override init(frame: NSRect) {
        super.init(frame: frame)
        wantsLayer = true
        observer = NSWorkspace.shared.notificationCenter.addObserver(
            forName: NSWorkspace.accessibilityDisplayOptionsDidChangeNotification,
            object: nil, queue: .main
        ) { [weak self] _ in MainActor.assumeIsolated { self?.update() } }
    }
    @available(*, unavailable) required init?(coder: NSCoder) { fatalError() }

    override func layout() {
        super.layout()
        CATransaction.begin(); CATransaction.setDisableActions(true)
        mask.frame = CGRect(x: 0, y: 0, width: min(bounds.width, ceil(textWidth)), height: bounds.height)
        CATransaction.commit()
    }
    override func viewDidMoveToWindow() { super.viewDidMoveToWindow(); update() }

    private func update() {
        let reduce = NSWorkspace.shared.accessibilityDisplayShouldReduceMotion
        let run = isActive && window != nil && !reduce
        mask.stop()
        layer?.mask = run ? mask : nil
        // With no mask, the caller draws the text at restingAlpha itself (static dim, no motion).
        alphaValue = (isActive && !run) ? ShimmerMaskLayer.restingAlpha : 1
        if run { mask.start() }
    }
}
```

Notes:
- Right-to-left: swap `startPoint`/`endPoint` and anchor the mask frame to the trailing edge (FlowDown `ShimmerLayer.fit`, `:48-61`).
- Reduced motion: `NSWorkspace.shared.accessibilityDisplayShouldReduceMotion` plus `accessibilityDisplayOptionsDidChangeNotification`, which must be observed on `NSWorkspace.shared.notificationCenter`. VERIFIED (doc). FlowDown swaps the shimmer for static half-opacity text and also drops text transition animation under Reduce Motion (`ThinkingStatusLabel.swift:131,149`). In SwiftUI the equivalent is `@Environment(\.accessibilityReduceMotion)` (VERIFIED (doc)).
- Stop the animation when the view leaves its window; a repeating render-server animation on an off-screen layer still costs compositor time. UNVERIFIED cost, but FlowDown gates on `window != nil`.
- If the status text lives in SwiftUI, wrap the container in `NSViewRepresentable` rather than re-implementing with `.mask` + `withAnimation(.linear.repeatForever)`; the wrapper keeps the off-main property.
- A pure SwiftUI fallback that matches the look (stalls with main, fine for non-critical spots): SwiftUI-Shimmer's approach with `Animation.linear(duration: 1.8).repeatForever(autoreverses: false)`, gradient opacities 0.5/1/0.5, `bandSize` 0.3.

### 4.4 Other affordances

- **Status text changes**: rate-limit to one visible change per second and cross-fade (FlowDown, 3.2). Tool-call status flipping at token rate reads as flicker.
- **Activity row**: FlowDown shows the thinking row only when the stream has been silent for 2 s after visible progress, and hides it the moment a token renders (VERIFIED (src) `FlowDown/Backend/Conversation/Pipeline/ConversationSession.swift:53,66-73,93-103`). That avoids a shimmer row popping in and out between chunks, which would also move the layout.
- **No spinners**: a shimmer row has a fixed height, so it can be reserved in the layout before the first token and replaced in place.

### Recommendation (section 4)

One `ShimmerMaskLayer` class used for thinking/status text and for skeleton rows, 1.8 s linear, band 0.3 of text width, resting alpha equal to the settled dim style, phase-locked, mask sized to the text, gated on window presence and Reduce Motion. SF Symbol `.pulse`/`.variableColor` for icon-sized states. Keep `TimelineView` and custom `Animatable` out of anything that must look alive while the main thread is busy.

---

## 5. Optimistic UI mechanics

### 5.1 Avoiding a main-actor hop per token

The pattern that every fast path above reduces to: producers write into a buffer from wherever they run; the main thread reads it at most once per frame (or per flush interval).

Illustrative, uncompiled. `OSAllocatedUnfairLock` is macOS 13+ (VERIFIED (doc)); `Mutex` would need macOS 15.

```swift
import AppKit
import os

/// Written by the ACP reader on any executor; drained on the main actor.
final class DeltaInbox<ID: Hashable & Sendable>: Sendable {
    struct Batch: Sendable { var text: [ID: String] = [:]; var order: [ID] = [] }
    private let state = OSAllocatedUnfairLock(initialState: Batch())
    private let wake: AsyncStream<Void>.Continuation
    let wakeups: AsyncStream<Void>

    init() {
        // A one-slot buffer coalesces any number of yields into a single pending wakeup.
        (wakeups, wake) = AsyncStream.makeStream(of: Void.self, bufferingPolicy: .bufferingNewest(1))
    }
    func append(_ delta: String, to id: ID) {
        state.withLock { b in
            if b.text[id] == nil { b.order.append(id) }
            b.text[id, default: ""] += delta
        }
        wake.yield()
    }
    func drain() -> Batch { state.withLock { b in let out = b; b = Batch(); return out } }
}

@MainActor
final class FramePump: NSObject {
    private var link: CADisplayLink?
    private let onFrame: (CFTimeInterval) -> Bool        // return false when there is nothing left to do

    init(view: NSView, onFrame: @escaping (CFTimeInterval) -> Bool) {
        self.onFrame = onFrame
        super.init()
        link = view.displayLink(target: self, selector: #selector(tick(_:)))   // macOS 14+
        link?.add(to: .main, forMode: .common)           // keeps ticking during scroll tracking
        link?.isPaused = true
    }
    func wake() { link?.isPaused = false }
    @objc private func tick(_ link: CADisplayLink) {
        if !onFrame(link.targetTimestamp - link.timestamp) { link.isPaused = true }
    }
}

// Wiring, on the main actor:
// Task { for await _ in inbox.wakeups { pump.wake() } }
// onFrame: drain inbox into per-entry backlogs; governor.take(...) per live entry;
//          rebuild the tail only if the cost-adaptive deadline has passed; return whether any backlog remains.
```

- `.bufferingNewest(1)`: "When the buffer is full, discard the oldest element" (VERIFIED (doc)); on a `Void` signal that is exactly coalescing. `DispatchSource.makeUserDataAddSource(queue: .main)` is the pre-concurrency equivalent (UNVERIFIED here, standard GCD).
- `.common` run-loop mode matters: Lakr233 schedules its rebuild timer in common modes "so the answer keeps arriving while the reader scrolls", while off-screen preparation is deliberately default-mode only so it yields to scrolling. VERIFIED (src) `MarkdownStreamView.swift:179-180`; `MessageListView+RetainingListView.swift:116-122`. Use both rules.
- Structural events (new tool call, plan update, permission request) should go through the same inbox in arrival order so text and structure cannot reorder; apply them in the same frame.
- swift-async-algorithms `throttle(for:latest:)` "produces elements such that at least a specific interval has elapsed between them" and does it without extra tasks; a `reducing:` overload folds skipped elements, which is the right primitive for folding deltas. VERIFIED (doc) https://github.com/apple/swift-async-algorithms/blob/main/Sources/AsyncAlgorithms/AsyncAlgorithms.docc/Guides/Throttle.md (I did not check which release exposes it without the underscore prefix).
- Do not persist per token. Sidekick debounces `ModelContext.save()` to 250 ms "so streaming inference no longer rewrites the entire conversations.json on every token". VERIFIED (src) `Sidekick/Logic/Persistence/MessageStreamCoordinator.swift:5-8,26-28`.

### 5.2 Keeping the composer responsive

- The composer must never share a layout pass with the transcript. In AppKit that means separate view subtrees and no Auto Layout constraint from composer height into the table's frame that is resolved synchronously during a stream; if the composer grows, change the scroll view's content insets and re-pin through the section 1 choke point.
- Clear the composer and insert the user row in the same run-loop turn as the key event, before any `await`. FlowDown's `userDidSendMessage` subject fires synchronously and re-arms auto-follow (`MessageListView.swift:52-55`); Aizen appends the user message to the array immediately and persists in a detached task (`ChatAgentSession+MessageMutation.swift:11-37`). VERIFIED (src).
- All transcript work per frame should fit a budget (ListViewKit 5 ms, FlowDown 4 ms, Lakr233 15% of main). A composer keystroke that lands during a 100 ms markdown rebuild is the jank users notice first; the cost-adaptive rebuild cap bounds it.
- Reconciliation without a flash: give the optimistic user row a client-generated id, and when the agent's echo or persisted record arrives, map it onto the same row id and the same row view. With retained rows (FlowDown) or direct cell reconfiguration, an identical update is a no-op (`AiMessageView.swift:71`: `if package !== representedPackage`). A reload of the row, or any id change, is what produces the flash.

### 5.3 Animating the sent message from composer into the transcript

- `matchedGeometryEffect` only works inside one SwiftUI hierarchy and only links geometry: "the system will interpolate their frame rectangles in window space... the `matchedGeometryEffect()` modifier only arranges for the geometry of the views to be linked, not their rendering." VERIFIED (doc). Pitfalls that follow (UNVERIFIED as measured facts, they are consequences of that contract): it cannot bridge a SwiftUI composer and an AppKit table row; when source and destination widths differ the text re-wraps every frame of the flight; inside a lazy container the destination may not exist in the same transaction; and both views render during the transition unless one is hidden.
- `geometryGroup()` (macOS 14+, VERIFIED (doc)) fixes the related case where a row's subviews animate position separately when the parent moves; use it on SwiftUI-hosted row content that is inserted with animation.
- AppKit pitfalls: `noteHeightOfRows` and `insertRows` animate unless told not to (VERIFIED (doc)); `NSAnimationContext` implicit animations leak into table retiling when `allowsImplicitAnimation` is on in an enclosing group; an animated insertion while following fights the bottom pin (VVDevKit's "no accordion Y-offset" rule).
- What I recommend (UNVERIFIED design, not seen verbatim in any examined app): snapshot flight in an overlay layer.
  1. On send, synchronously insert the user row at its final measured height with no animation and re-pin; set the row's content alpha to 0.
  2. Render the composer's text into a `CALayer` snapshot in window coordinates on an overlay above both views.
  3. Animate the snapshot's `position` and `bounds` to the row's bubble rect with a `CASpringAnimation` (about 0.30-0.35 s, damping ratio near 0.85) and cross-fade bubble background in; these are render-server animations, so they stay smooth while the first tokens are parsed.
  4. On completion, in one `CATransaction` with actions disabled, set row alpha to 1 and remove the overlay.
  Under Reduce Motion, skip the flight and fade the row in over about 0.15 s. If the bubble's wrapped text differs from the composer's (different widths), cross-fade the snapshot into the row's own rendering during the last third instead of morphing it.
- The simpler alternative most desktop chat apps appear to use (UNVERIFIED): no flight; the row appears at its final position with a 0.12-0.18 s opacity plus 4 pt rise while the composer clears. With a correct bottom pin this already reads as instant.

### 5.4 Session switching

Not in the assignment's list but it shares the mechanics. What makes a switch feel instant in source: keep per-session height caches and scroll position (agent-sessions, Memex `savePosition`), apply the first snapshot unanimated at the final offset before the view is visible (FlowDown), and avoid frame-sleep choreography (Clarc sleeps 16 ms + 32 ms then fades 150 ms; FlowDown waits 100 ms then fades 250 ms). VERIFIED (src) for those constants, cited above. With explicit heights already cached, the table can be positioned in one layout pass and shown immediately.

### Recommendation (section 5)

Lock-protected inbox + coalesced wake + one display-link pump in common run-loop mode; text and structural events applied in arrival order once per frame; cost-adaptive tail rebuilds; persistence debounced off-main; optimistic rows keyed by client id and reconciled onto the same row view; send animation either omitted or done as a snapshot-layer flight with Core Animation, never by animating table row heights.

---

## Ranked: the 10 highest-leverage native techniques

1. **One layout-change choke point with explicit follow-or-anchor and zero-duration row updates.** Eliminates scroll jumps, bounce on tail append, and animated retiling in one place. (1.3, 1.4)
2. **Explicit row heights measured from the same object that draws, cached by (row id, width bucket).** No estimates, no one-frame height correction, instant session switch. (1.2, 3.4)
3. **Coalesce stream input to the frame with a lock-protected inbox and a single display-link pump; never hop to main per token.** (5.1)
4. **Cost-adaptive tail rebuild cap (`cost / 0.15`, 1/30...1/8 s) instead of per-token or per-frame relayout.** Bounds main-thread time as messages grow. (2.2)
5. **Positional block identity with tail-only reparse and prefix-preserving `replaceCharacters`.** Completed blocks never re-layout. (2.5)
6. **Speculative close of the unfinished tail, then a final unrepaired parse diffed block by block.** No marker flashes mid-stream, no flash at reconciliation. (2.4)
7. **Do not recycle the live row (or keep the live tail outside the settled list).** Preserves in-flight fade, selection and pacing across scroll. (1.4)
8. **Burst smoothing with a backlog-proportional reveal governor (about 0.3 s lag) plus a cheap glyph fade that redraws only lines in flight; let the fade finish before ending the stream.** (2.3, 2.9)
9. **Shimmer as a `CAGradientLayer` mask animated on `locations`, sized to the text, resting alpha equal to the settled dim style, gated on window and Reduce Motion.** Survives main-thread stalls. (4)
10. **Budgeted, interruptible background work on main: 4-5 ms slices for off-screen measurement, default run-loop mode so scrolling always wins; highlight off-main, keyed, recolor-only.** (1.3, 2.8)

Honourable mentions: typed update causes with assertions (VVChatTimeline); status text rate-limited to one change per second; model-coordinate cross-row selection; per-entry observation instead of array observation in any SwiftUI-hosted part.

## Open questions this research did not settle

- No measured comparison of `NSTableView` versus ListViewKit on macOS at several thousand rows; the choice above rests on architecture fit and source reading.
- Whether Core Animation mask animations on layer-backed `NSView`s keep running through main-thread stalls on macOS 14-27 should be confirmed with a deliberate stall in a debug build.
- The `scrollTo`/prepend regression on the 27 releases is a single unanswered forum report.
- Exact shimmer and send-animation parameters of the ChatGPT and Claude desktop apps are not public.

## Sources

Apple documentation (read as JSON): `onScrollGeometryChange(for:of:action:)`, `defaultScrollAnchor(_:)`, `defaultScrollAnchor(_:for:)`, `ScrollAnchorRole`, `ScrollPosition`, `scrollPosition(_:anchor:)`, `scrollPosition(id:anchor:)`, `scrollTargetLayout(isEnabled:)`, `onScrollPhaseChange(_:)`, `onScrollTargetVisibilityChange(idType:threshold:_:)`, `TextRenderer`, `textRenderer(_:)`, `textSelection(_:)`, `geometryGroup()`, `matchedGeometryEffect(id:in:properties:anchor:isSource:)`, `redacted(reason:)`, `symbolEffect(_:options:isActive:)`, `BreatheSymbolEffect`, `VariableColorSymbolEffect`, `TimelineView`, `accessibilityReduceMotion`, `Observations`, `NSTableView.usesAutomaticRowHeights`, `noteHeightOfRows(withIndexesChanged:)`, `insertRows(at:withAnimation:)`, `NSView.boundsDidChangeNotification`, `NSView.displayLink(target:selector:)`, `NSScrollView.willStartLiveScrollNotification`, `didEndLiveScrollNotification`, `NSTextFinder`, `NSTextFinderClient.string(at:effectiveRange:endsWithSearchBoundary:)`, `NSTextView.usesFindBar`, `NSLayoutManager.allowsNonContiguousLayout`, `NSCollectionViewCompositionalLayout`, `NSWorkspace.accessibilityDisplayShouldReduceMotion`, `accessibilityDisplayOptionsDidChangeNotification`, `CAGradientLayer.locations`, `CAMediaTiming.timeOffset`, `CADisplayLink`, `OSAllocatedUnfairLock`, `Mutex`, `AsyncStream.Continuation.BufferingPolicy.bufferingNewest(_:)`. Base URL pattern: https://developer.apple.com/documentation/<framework>/<symbol>.

WWDC: https://developer.apple.com/videos/play/wwdc2025/256/ (What's new in SwiftUI), https://developer.apple.com/videos/play/wwdc2025/306/ (Optimize SwiftUI performance with Instruments), https://developer.apple.com/videos/play/wwdc2026/321/ (Dive into lazy stacks and scrolling with SwiftUI), https://developer.apple.com/videos/play/wwdc2023/10156/ (Explore SwiftUI animation).

Blogs and forums: https://blog.krzyzanowskim.com/2025/08/14/textkit-2-the-promised-land/ ; https://christiantietze.de/posts/2022/11/nstableview-variable-row-heights-broken-macos-ventura-13-0 ; https://blog.jacobstechtavern.com/p/swiftui-scroll-performance-the-120fps ; https://developer.apple.com/forums/thread/848608 ; https://developer.apple.com/forums/thread/685461 ; https://developer.apple.com/forums/thread/696976 ; https://developer.apple.com/forums/thread/758302 ; https://developer.apple.com/library/archive/documentation/Cocoa/Conceptual/CoreAnimation_guide/Introduction/Introduction.html ; https://github.com/apple/swift-async-algorithms/blob/main/Sources/AsyncAlgorithms/AsyncAlgorithms.docc/Guides/Throttle.md
