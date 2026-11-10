import Foundation
import Testing
@testable import Memex

struct StreamingRevealTests {
    /// Ticks at `frame` from `start` until the reveal completes; returns the elapsed time.
    private func drain(_ pacer: inout StreamingRevealPacer, from start: Double, frame: Double, limit: Double = 2) -> Double {
        var time = start
        while !pacer.isComplete && time - start < limit {
            time += frame
            pacer.commit(pacer.advance(to: time))
        }
        return time - start
    }

    @Test(arguments: [1.0 / 60, 1.0 / 120, 1.0 / 30])
    func anyBacklogDrainsInAboutTwoHundredMilliseconds(frame: Double) {
        for backlog in [40, 1_000, 20_000] {
            var pacer = StreamingRevealPacer(length: 0)
            pacer.extend(to: backlog, at: 0)
            let elapsed = drain(&pacer, from: 0, frame: frame)
            #expect(pacer.revealed == backlog)
            #expect(elapsed >= StreamingRevealPacer.drainDuration - frame - 1e-9)
            #expect(elapsed <= StreamingRevealPacer.drainDuration + frame + 1e-9)
        }
    }

    @Test func revealProgressesEvenlyBetweenArrivals() {
        var pacer = StreamingRevealPacer(length: 0)
        pacer.extend(to: 600, at: 0)
        var steps: [Int] = []
        var time = 0.0
        for _ in 0..<6 {
            time += 1.0 / 60
            let before = pacer.revealed
            pacer.commit(pacer.advance(to: time))
            steps.append(pacer.revealed - before)
        }
        // Linear drain: the same amount every frame, not a burst then a trickle.
        #expect(Set(steps).count <= 2)
        #expect(steps.allSatisfy { $0 >= 49 && $0 <= 51 })
    }

    @Test func aBurstDuringTheRevealRecomputesTheRateFromTheWholeBacklog() {
        var pacer = StreamingRevealPacer(length: 0)
        pacer.extend(to: 1_000, at: 0)
        var time = 0.0
        while time < 0.1 { time += 1.0 / 60; pacer.commit(pacer.advance(to: time)) }
        pacer.extend(to: 3_000, at: time)
        let elapsed = drain(&pacer, from: time, frame: 1.0 / 60)
        #expect(pacer.revealed == 3_000)
        #expect(abs(elapsed - StreamingRevealPacer.drainDuration) <= 1.0 / 60 + 1e-9)
    }

    @Test func existingTextIsShownAtOnceAndStallsRevealAtMostOneDrain() {
        var pacer = StreamingRevealPacer(length: 500)
        #expect(pacer.isComplete)
        pacer.extend(to: 1_500, at: 10)
        pacer.commit(pacer.advance(to: 10.05))
        #expect(pacer.revealed > 500 && pacer.revealed < 1_500)
        var stalled = StreamingRevealPacer(length: 0)
        stalled.extend(to: 100_000, at: 0)
        stalled.commit(stalled.advance(to: 0.01))
        #expect(stalled.revealed < 100_000)
        // After a one-second hitch the next frame catches up completely.
        stalled.commit(stalled.advance(to: 1.01))
        #expect(stalled.revealed == 100_000)
        pacer.flush()
        #expect(pacer.isComplete && pacer.revealed == 1_500)
    }

    @Test func smallBacklogsStillFlow() {
        var pacer = StreamingRevealPacer(length: 10)
        pacer.extend(to: 13, at: 0)
        let elapsed = drain(&pacer, from: 0, frame: 1.0 / 60)
        #expect(pacer.revealed == 13)
        #expect(elapsed <= StreamingRevealPacer.drainDuration)
    }

    @Test func layoutIntervalFollowsCostWithinThirtyAndEightHertz() {
        #expect(StreamingReveal.layoutInterval(forCost: 0.000_5) == 1.0 / 30)
        #expect(abs(StreamingReveal.layoutInterval(forCost: 0.01) - 0.01 / 0.15) < 1e-12)
        #expect(StreamingReveal.layoutInterval(forCost: 0.5) == 1.0 / 8)
    }

    @Test func revealFlushesWhenHiddenReducedFindingOrSelecting() {
        #expect(StreamingReveal.shouldPace(windowVisible: true, reduceMotion: false, findActive: false, userSelecting: false))
        #expect(!StreamingReveal.shouldPace(windowVisible: false, reduceMotion: false, findActive: false, userSelecting: false))
        #expect(!StreamingReveal.shouldPace(windowVisible: true, reduceMotion: true, findActive: false, userSelecting: false))
        #expect(!StreamingReveal.shouldPace(windowVisible: true, reduceMotion: false, findActive: true, userSelecting: false))
        #expect(!StreamingReveal.shouldPace(windowVisible: true, reduceMotion: false, findActive: false, userSelecting: true))
    }

    // MARK: Cut boundaries

    private func cut(_ text: String, _ revealed: Int, _ desired: Int) -> Int {
        StreamingReveal.cut(in: text as NSString, from: revealed, toward: desired)
    }

    @Test func cutsPreferTheEndOfAWord() {
        #expect(cut("hello world again", 0, 8) == 6)
        #expect(cut("hello world again", 6, 14) == 12)
        // No whitespace nearby: any grapheme boundary.
        #expect(cut("supercalifragilistic", 0, 7) == 7)
        #expect(cut("你好世界和平", 1, 3) == 3)
        #expect(cut("hello", 0, 99) == 5)
        #expect(cut("hello world", 6, 6) == 6)
    }

    @Test func cutsNeverSplitAGraphemeCluster() {
        let family = "👨‍👩‍👧 family"
        let text = family as NSString
        for desired in 1..<text.length {
            let position = cut(family, 0, desired)
            #expect(position == 0 || position == text.length || text.rangeOfComposedCharacterSequence(at: position).location == position)
        }
        let flag = "a🇺🇸b"
        #expect(cut(flag, 1, 2) == 1 || cut(flag, 1, 2) == 5)
        #expect(cut("e\u{301}tude", 0, 1) == 2)
    }

    @Test func cutsNeverStopInsideAnUnrepairableToken() {
        // Marker runs are revealed whole.
        #expect(cut("**bold** text", 0, 1) == 2)
        #expect(cut("say **bold**", 4, 5) == 6)
        // An entity waits for its semicolon.
        #expect(cut("a &amp; b", 2, 5) == 7)
        #expect(cut("a &amp; b", 0, 5) == 2)
        // An escape stays with what it escapes.
        #expect(cut("x \\*y", 2, 3) == 4)
        // An autolink or tag is revealed whole.
        #expect(cut("see <https://x.y/z> now", 4, 10) == 19)
        #expect(cut("see <b>bold</b>", 0, 6) == 4)
        // Prose with the same characters is unaffected.
        #expect(cut("R&D team", 0, 5) == 4)
        #expect(cut("2 < 3 and 4 > 1", 0, 5) == 4)
    }
}

struct TranscriptFollowStateTests {
    @Test func followingIsChangedOnlyByTheReader() {
        var state = TranscriptFollowState()
        #expect(state.isFollowing && state.shouldPin)
        // Growth and the controller's own pinning never detach.
        for _ in 0..<5 { state.handle(.contentChanged) }
        #expect(state.isFollowing)
        // A small movement within the tolerance stays attached.
        state.handle(.readerMoved(distanceFromEnd: TranscriptFollowState.tolerance))
        #expect(state.isFollowing)
        state.handle(.readerMoved(distanceFromEnd: 120))
        #expect(state.mode == .detached && !state.shouldPin)
        state.handle(.contentChanged)
        #expect(state.mode == .detached)
        state.handle(.readerMoved(distanceFromEnd: 0))
        #expect(state.isFollowing)
    }

    @Test func aGestureSuspendsPinningAndDecidesWhenItEnds() {
        var state = TranscriptFollowState()
        state.handle(.liveScrollBegan)
        #expect(state.isFollowing && !state.shouldPin)
        state.handle(.readerMoved(distanceFromEnd: 300))
        state.handle(.liveScrollEnded(distanceFromEnd: 300))
        #expect(state.mode == .detached && !state.isUserScrolling)
        state.handle(.liveScrollBegan)
        state.handle(.liveScrollEnded(distanceFromEnd: -20))
        #expect(state.shouldPin)
        state.handle(.readerMoved(distanceFromEnd: 50))
        state.handle(.jumpToLatest)
        #expect(state.shouldPin)
    }
}
