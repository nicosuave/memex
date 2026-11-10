import Foundation
import os

/// Timing for the chat hot path. Intervals appear in Instruments (os_signpost,
/// subsystem `dev.memex.app`, category `chat`); durations are also logged at
/// debug level so `log stream --predicate 'subsystem == "dev.memex.app"'` shows them.
enum ChatSignpost {
    static let signposter = OSSignposter(subsystem: "dev.memex.app", category: "chat")
    static let log = Logger(subsystem: "dev.memex.app", category: "chat")

    /// Measure one awaited step of a send, connect or publish. Runs on the
    /// caller's actor, so the body may use its isolated state.
    static func measure<T>(_ name: StaticString, isolation: isolated (any Actor)? = #isolation,
                           _ body: () async throws -> T) async rethrows -> T {
        let state = signposter.beginInterval(name)
        let start = ContinuousClock.now
        defer {
            signposter.endInterval(name, state)
            log.debug("\(name.description, privacy: .public) \(milliseconds(since: start), format: .fixed(precision: 1)) ms")
        }
        return try await body()
    }

    static func measureSync<T>(_ name: StaticString, _ body: () throws -> T) rethrows -> T {
        let state = signposter.beginInterval(name)
        defer { signposter.endInterval(name, state) }
        return try body()
    }

    /// A milestone measured from an earlier instant, such as Enter to first output.
    static func milestone(_ name: StaticString, since start: ContinuousClock.Instant) {
        signposter.emitEvent(name)
        log.debug("\(name.description, privacy: .public) \(milliseconds(since: start), format: .fixed(precision: 1)) ms")
    }

    static func milliseconds(since start: ContinuousClock.Instant) -> Double {
        let elapsed = ContinuousClock.now - start
        return Double(elapsed.components.seconds) * 1_000 + Double(elapsed.components.attoseconds) / 1e15
    }
}
