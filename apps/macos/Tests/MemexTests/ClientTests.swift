import Foundation
import Darwin
import Testing
@testable import Memex

@Test func commandDrainsLargeOutputAndErrorsWithoutDeadlock() throws {
    let output = try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sh"), arguments: ["-c", "i=0; while [ $i -lt 5000 ]; do echo output; echo error >&2; i=$((i+1)); done"], timeout: 5)
    #expect(output.count == 35_000)
}

@Test func commandSurfacesFailure() {
    #expect(throws: ClientError.self) {
        try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sh"), arguments: ["-c", "echo 'fixture failure' >&2; exit 2"], timeout: 5)
    }
}

@Test func cancelledCommandDoesNotStart() {
    let command = CommandRun()
    command.cancel()
    #expect(throws: CancellationError.self) {
        try command.execute(executable: URL(fileURLWithPath: "/usr/bin/true"), arguments: [], timeout: 5)
    }
}

@Test func commandTimesOut() {
    #expect(throws: ClientError.self) {
        try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sleep"), arguments: ["3"], timeout: 0.05)
    }
}

private final class ScanProgressRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var received: [ActivityScanProgress] = []
    func append(_ value: ActivityScanProgress) { lock.lock(); defer { lock.unlock() }; received.append(value) }
    var values: [ActivityScanProgress] { lock.lock(); defer { lock.unlock() }; return received }
}

@Test func advancingScanProgressKeepsColdBackfillAliveBeyondNormalReadTimeout() throws {
    let recorder = ScanProgressRecorder()
    let script = #"""
    for done in 0 1 2 3 4 5; do
      printf 'MEMEX_PROGRESS {"source":"codex","done":%s,"total":5}\n' "$done" >&2
      sleep 0.15
    done
    printf 'complete'
    """#
    let start = ContinuousClock.now
    let result = try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sh"),
        arguments: ["-c", script], timeout: 0.5, progress: recorder.append)
    #expect(start.duration(to: .now) > .milliseconds(500))
    #expect(String(decoding: result, as: UTF8.self) == "complete")
    #expect(recorder.values.map(\.done) == [0, 1, 2, 3, 4, 5])
}

@Test(arguments: ["1 1", "2 1", "0 0", "-1 9"])
func stalledRegressingAndInvalidScanCountersStillTimeOut(counters: String) {
    let script = #"""
    while :; do
      for done in $1; do
        printf 'MEMEX_PROGRESS {"source":"codex","done":%s,"total":5}\n' "$done" >&2
        sleep 0.05
      done
    done
    """#
    let start = ContinuousClock.now
    #expect(throws: ClientError.self) {
        try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sh"),
            arguments: ["-c", script, "fixture", counters], timeout: 0.25, progress: { _ in })
    }
    #expect(start.duration(to: .now) < .seconds(2))
}

@Test func coldScanRemainsCancellableWhileProgressAdvances() async throws {
    let command = CommandRun()
    let recorder = ScanProgressRecorder()
    let script = #"""
    done=1
    while [ "$done" -lt 1000 ]; do
      printf 'MEMEX_PROGRESS {"source":"codex","done":%s,"total":1000}\n' "$done" >&2
      done=$((done+1))
      sleep 0.05
    done
    """#
    let task = Task.detached {
        try command.execute(executable: URL(fileURLWithPath: "/bin/sh"),
            arguments: ["-c", script], timeout: 0.5, progress: recorder.append)
    }
    let deadline = Date().addingTimeInterval(3)
    while recorder.values.isEmpty && Date() < deadline { try await Task.sleep(for: .milliseconds(10)) }
    #expect(!recorder.values.isEmpty)
    command.cancel()
    do { _ = try await task.value; Issue.record("Expected cancellation") }
    catch is CancellationError {} catch { Issue.record("Unexpected error: \(error)") }
}

@Test func cancelledRunningRequestIsReaped() async throws {
    let command = CommandRun()
    let task = Task.detached {
        try command.execute(executable: URL(fileURLWithPath: "/bin/sleep"), arguments: ["10"], timeout: 15)
    }
    try await Task.sleep(for: .milliseconds(100))
    command.cancel()
    do {
        _ = try await task.value
        Issue.record("Expected cancellation")
    } catch is CancellationError {} catch { Issue.record("Unexpected error: \(error)") }
}

@Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_LIVE_TESTS"] == "1"))
func liveCLIListsReadsAndSearches() async throws {
    let client = MemexClient()
    let sessions = try await client.sessions(limit: 5)
    let session = try #require(sessions.first)
    let records = try await client.records(for: session, offset: 0)
    #expect(!records.isEmpty)
    let hits = try await client.search("memex", project: nil, source: nil, limit: 5)
    #expect(!hits.isEmpty)
}

@Test func searchProtectsLeadingDashesAndKeepsRootBeforeTerminator() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let executable = directory.appendingPathComponent("fixture-cli")
    let script = #"""
    #!/bin/sh
    saw_root=no
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--root" ]; then
        shift
        [ "$1" = "/fixture root" ] || exit 2
        saw_root=yes
      elif [ "$1" = "--" ]; then
        shift
        [ "$saw_root" = yes ] && [ "$#" = 1 ] && [ "$1" = "--help" ] || exit 3
        printf '[]'
        exit 0
      fi
      shift
    done
    exit 4
    """#
    try script.write(to: executable, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
    let hits = try await MemexClient(executable: executable, root: "/fixture root")
        .search("--help", project: nil, source: nil, limit: 5)
    #expect(hits.isEmpty)
}

@Test func timeoutReapsChildThatIgnoresTermination() {
    let start = ContinuousClock.now
    #expect(throws: ClientError.self) {
        try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sh"),
            arguments: ["-c", "trap '' TERM; exec sleep 10"], timeout: 0.1)
    }
    #expect(start.duration(to: .now) < .seconds(3))
}

@Test func timeoutKillsDescendantAfterDirectChildExits() throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let pidFile = directory.appendingPathComponent("descendant.pid")
    // The direct child exits on TERM; its descendant deliberately ignores it.
    let script = #"""
    trap 'exit 0' TERM
    /bin/sh -c 'trap "" TERM; echo $$ > "$1"; exec /bin/sleep 20' fixture "$1" &
    wait
    """#
    #expect(throws: ClientError.self) {
        try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sh"),
            arguments: ["-c", script, "fixture", pidFile.path], timeout: 0.3)
    }
    let pid = try #require(Int32(String(contentsOf: pidFile, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines)))
    defer { if kill(pid, 0) == 0 { kill(pid, SIGKILL) } }
    let deadline = Date().addingTimeInterval(2)
    while kill(pid, 0) == 0 && Date() < deadline { Thread.sleep(forTimeInterval: 0.025) }
    #expect(kill(pid, 0) == -1 && errno == ESRCH)
}

@Test func cancellationKillsDescendantAfterDirectChildExits() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let pidFile = directory.appendingPathComponent("descendant.pid")
    let script = #"""
    trap 'exit 0' TERM
    /bin/sh -c 'trap "" TERM; echo $$ > "$1"; exec /bin/sleep 20' fixture "$1" &
    wait
    """#
    let command = CommandRun()
    let task = Task.detached {
        try command.execute(executable: URL(fileURLWithPath: "/bin/sh"),
            arguments: ["-c", script, "fixture", pidFile.path], timeout: 5)
    }
    let readyDeadline = Date().addingTimeInterval(2)
    while !FileManager.default.fileExists(atPath: pidFile.path) && Date() < readyDeadline {
        try await Task.sleep(for: .milliseconds(25))
    }
    command.cancel()
    do {
        _ = try await task.value
        Issue.record("Expected cancellation")
    } catch is CancellationError {} catch { Issue.record("Unexpected error: \(error)") }
    let pid = try #require(Int32(String(contentsOf: pidFile, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines)))
    defer { if kill(pid, 0) == 0 { kill(pid, SIGKILL) } }
    let deadline = Date().addingTimeInterval(2)
    while kill(pid, 0) == 0 && Date() < deadline { try await Task.sleep(for: .milliseconds(25)) }
    #expect(kill(pid, 0) == -1 && errno == ESRCH)
}
