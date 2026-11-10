import AppKit
import QuartzCore
import SwiftUI
import Testing
@testable import Memex

@Suite struct ShimmerSweepTests {
    @Test func phaseFollowsTheSharedClockSoLabelsResumeMidSweep() {
        let period = ShimmerSweep.period
        for time in [-3.25, 0, 0.4, 12.9, 86_400.123, 1_000_000.75] {
            let phase = ShimmerSweep.phase(at: time)
            #expect(phase >= 0 && phase < period)
            // Labels created whole passes apart are in step.
            #expect(abs(ShimmerSweep.phase(at: time + 7 * period) - phase) < 1e-6)
            // A label rebuilt later within the same pass continues from where the sweep is, not from 0.
            let later = (period - phase) / 2
            #expect(abs(ShimmerSweep.phase(at: time + later) - (phase + later)) < 1e-6)
        }
    }

    @Test func bandSweepsAllTextAndScalesWithLongText() {
        for width in [1.0, 12, 40, 80, 250, 1200, 4000] as [CGFloat] {
            let stops = ShimmerSweep.locations(forTextWidth: width)
            #expect(stops.from == stops.from.sorted() && stops.to == stops.to.sorted())
            // The band starts wholly before the first glyph and ends wholly after the last.
            #expect(stops.from.allSatisfy { $0 <= 0 })
            #expect(stops.to.allSatisfy { $0 >= 1 })
            let bandPoints = CGFloat(stops.from[2] - stops.from[0]) * width
            #expect(abs(bandPoints - ShimmerSweep.bandWidth(forTextWidth: width)) < 0.001)
        }
        // Long labels get a proportional band; short ones keep a readable minimum instead of a sliver.
        #expect(ShimmerSweep.bandWidth(forTextWidth: 1200) == 2 * ShimmerSweep.bandWidth(forTextWidth: 600))
        #expect(ShimmerSweep.bandWidth(forTextWidth: 10) == ShimmerSweep.bandWidth(forTextWidth: 30))
        #expect(ShimmerSweep.bandWidth(forTextWidth: 10) > 10 * ShimmerSweep.bandFraction)
        let empty = ShimmerSweep.locations(forTextWidth: 0)
        #expect((empty.from + empty.to).allSatisfy { $0.isFinite })
    }

    @Test func eitherReduceMotionSettingStopsTheSweep() {
        for onScreen in [true, false] {
            #expect(ShimmerSweep.mode(systemReduceMotion: true, appReduceMotion: false, onScreen: onScreen) == .still)
            #expect(ShimmerSweep.mode(systemReduceMotion: false, appReduceMotion: true, onScreen: onScreen) == .still)
        }
        #expect(ShimmerSweep.mode(systemReduceMotion: false, appReduceMotion: false, onScreen: true) == .sweeping)
        #expect(ShimmerSweep.mode(systemReduceMotion: false, appReduceMotion: false, onScreen: false) == .paused)
    }
}

/// Test processes get no window-server visibility, so this window reports itself on screen.
private final class VisibleWindow: NSWindow {
    override var occlusionState: NSWindow.OcclusionState { .visible }
}

@Suite @MainActor struct ShimmerTextViewTests {
    private let font = NSFont.systemFont(ofSize: NSFont.systemFontSize)

    @Test(.enabled(if: !NSWorkspace.shared.accessibilityDisplayShouldReduceMotion, "System Reduce Motion is on"))
    func visibleLabelKeepsItsPassAcrossTextChangesAndStopsWhenHiddenOrRemoved() async throws {
        let window = VisibleWindow(contentRect: NSRect(x: 0, y: 0, width: 400, height: 60),
                                   styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        defer { window.close() }
        let view = ShimmerTextView(frame: .zero)
        func sweep() -> CABasicAnimation? {
            view.sweepLayer.animationKeys()?.first.flatMap { view.sweepLayer.animation(forKey: $0) as? CABasicAnimation }
        }
        func show(_ text: String, reduceMotion: Bool = false) {
            view.update(text: text, font: font, systemReduceMotion: false, appReduceMotion: reduceMotion)
            view.frame.size = view.intrinsicContentSize
            view.layoutSubtreeIfNeeded()
        }
        show("Reading")
        window.contentView?.addSubview(view)
        view.layoutSubtreeIfNeeded()
        let first = try #require(sweep())
        // Passes start on period boundaries of the media clock, shared by every label.
        #expect(abs(ShimmerSweep.phase(at: first.beginTime + ShimmerSweep.period / 2) - ShimmerSweep.period / 2) < 1e-6)
        try await Task.sleep(for: .milliseconds(50))
        show("Reading workspace changes")
        let second = try #require(sweep())
        // The band is rebuilt for the wider text but stays in the same pass, so it does not jump back.
        #expect(second.fromValue as? [Double] != first.fromValue as? [Double])
        let passes = (second.beginTime - first.beginTime) / ShimmerSweep.period
        #expect(abs(passes - passes.rounded()) < 1e-6)
        view.isHidden = true
        #expect(sweep() == nil)
        view.isHidden = false
        #expect(sweep() != nil)
        show("Reading workspace changes", reduceMotion: true)
        #expect(sweep() == nil && view.highlightText.isHidden)
        show("Reading workspace changes")
        view.removeFromSuperview()
        #expect(sweep() == nil)
    }

    @Test func changingTextKeepsTheLayerTreeAndResizes() {
        let view = ShimmerTextView(frame: .zero)
        view.update(text: "Reading", font: font, systemReduceMotion: false, appReduceMotion: false)
        let shortWidth = view.intrinsicContentSize.width
        let mask = view.sweepLayer
        view.update(text: "Reading workspace changes", font: font, systemReduceMotion: false, appReduceMotion: false)
        view.frame.size = view.intrinsicContentSize
        view.layoutSubtreeIfNeeded()
        #expect(view.intrinsicContentSize.width > shortWidth)
        #expect(view.sweepLayer === mask && view.highlightText.layer?.mask === mask)
        #expect(view.restingText.text == "Reading workspace changes" && view.highlightText.text == "Reading workspace changes")
        #expect(mask.frame.width == view.intrinsicContentSize.width)
    }

    @Test(.enabled(if: !NSWorkspace.shared.accessibilityDisplayShouldReduceMotion, "System Reduce Motion is on"))
    func reduceMotionShowsPlainTextAndOffscreenLabelsDoNotAnimate() {
        let view = ShimmerTextView(frame: NSRect(x: 0, y: 0, width: 200, height: 20))
        view.update(text: "Transcribing recording…", font: font, systemReduceMotion: false, appReduceMotion: true)
        #expect(view.highlightText.isHidden && view.restingText.alphaValue == 1)
        view.update(text: "Transcribing recording…", font: font, systemReduceMotion: false, appReduceMotion: false)
        view.layoutSubtreeIfNeeded()
        // Without a window there is nothing to sweep, but the label already has its in-progress look.
        #expect(!view.highlightText.isHidden && view.restingText.alphaValue < 1)
        #expect(view.sweepLayer.animationKeys()?.isEmpty ?? true)
    }

    @Test func swiftUIWrapperTruncatesToItsSlotAndReadsTheAppMotionSetting() throws {
        let text = "Preparing worktree and conversation…"
        let host = NSHostingView(rootView: HStack(spacing: 0) {
            ShimmerText(text).frame(width: 60, alignment: .leading)
            ShimmerText(text)
        }.environment(\.memexReduceMotion, true).fixedSize())
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 600, height: 60),
                              styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = host
        defer { window.close() }
        host.layoutSubtreeIfNeeded()
        func labels(in view: NSView) -> [ShimmerTextView] {
            ((view as? ShimmerTextView).map { [$0] } ?? []) + view.subviews.flatMap { labels(in: $0) }
        }
        let found = labels(in: host).sorted { $0.convert($0.bounds, to: nil).minX < $1.convert($1.bounds, to: nil).minX }
        try #require(found.count == 2)
        let ideal = found[1].intrinsicContentSize.width
        #expect(found[0].bounds.width <= 60 && ideal > 60)
        #expect(abs(found[1].bounds.width - ideal) < 2)
        // The app's motion setting reaches the AppKit view, and the AppKit view adds no accessibility element.
        #expect(found.allSatisfy { $0.highlightText.isHidden && !$0.isAccessibilityElement() })
    }
}
