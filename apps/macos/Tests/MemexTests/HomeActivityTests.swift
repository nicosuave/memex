import Foundation
import Testing
@testable import Memex

@Test func homeActivityCacheReusesOnlyCompletedCurrentRequests() {
    let now = Date()
    let payload = HomeActivityPayload(metric: "tokens", bucketKeys: [], tokenUsageEnabled: true, partial: false, points: [])
    let cache = HomeActivityCache(criteria: "tokens", request: "tokens|1", payload: payload,
        machines: [:], failures: [:], complete: true, updatedAt: now)
    #expect(cache.isFresh(for: "tokens|1", now: now.addingTimeInterval(59)))
    #expect(!cache.isFresh(for: "tokens|1", now: now.addingTimeInterval(60)))
    #expect(!cache.isFresh(for: "tokens|2", now: now))
    let incomplete = HomeActivityCache(criteria: "tokens", request: "tokens|1", payload: payload,
        machines: [:], failures: [:], complete: false, updatedAt: now)
    #expect(!incomplete.isFresh(for: "tokens|1", now: now))
}

@Test func homeActivityUsesAggregateCLIAndAllFilters() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let executable = directory.appendingPathComponent("fixture-cli")
    let script = #"""
    #!/bin/sh
    [ "$1" = "--no-update-check" ] && [ "$2" = "--non-interactive" ] && [ "$3" = "activity" ] || exit 2
    shift 3
    [ "$1" = "--format" ] && [ "$2" = "json" ] || exit 3
    shift 2
    [ "$1" = "--metric" ] && [ "$2" = "tokens" ] || exit 4
    shift 2
    [ "$1" = "--range" ] && [ "$2" = "24h" ] || exit 5
    shift 2
    [ "$1" = "--machine" ] && [ "$2" = "nicbook-atm" ] || exit 6
    shift 2
    [ "$1" = "--origin" ] && [ "$2" = "regular" ] || exit 7
    shift 2
    [ "$1" = "--progress" ] && [ "$2" = "--raw" ] && [ "$3" = "--now-ms" ] && [ "$4" = "172800000" ] || exit 8
    shift 4
    [ "$1" = "--query=  --needle  " ] && [ "$2" = "--project" ] && [ "$3" = "memex" ] || exit 9
    shift 3
    [ "$1" = "--source" ] && [ "$2" = "codex" ] || exit 10
    printf '%s' '{"token_usage_enabled":true,"partial":true,"warnings":["usage cache disabled: database is read-only"],"points":[{"timestamp_ms":3600000,"source":"codex","value":1200},{"timestamp_ms":7200000,"source":"codex","value":300}]}'
    """#
    try script.write(to: executable, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
    let selection = HomeActivitySelection(metric: "tokens", timeframe: .day, query: "  --needle  ",
        project: "memex", source: "codex", origin: .all, nowMS: 172_800_000)
    let payload = try await MemexClient(executable: executable).activity(selection, machine: "nicbook-atm")
    #expect(payload.points.reduce(0) { $0 + $1.value } == 1500)
    #expect(payload.partial)
    #expect(payload.tokenUsageEnabled)
    #expect(payload.warnings?.first == "usage cache disabled: database is read-only")
}

@Test func homeActivityMergesMachinesBeforeCompressingAllTimeBuckets() {
    let day: UInt64 = 86_400_000
    let selection = HomeActivitySelection(metric: "sessions", timeframe: .all, query: nil,
        project: nil, source: nil, origin: .all, nowMS: 200 * day)
    let local = RawHomeActivityPayload(tokenUsageEnabled: true, partial: false,
        points: [RawHomeActivityPoint(timestampMS: day, source: "codex", value: 2)])
    let remote = RawHomeActivityPayload(tokenUsageEnabled: true, partial: false,
        points: [RawHomeActivityPoint(timestampMS: day, source: "codex", value: 3),
                 RawHomeActivityPoint(timestampMS: 150 * day, source: "claude", value: 7)])
    let merged = HomeActivityPayload.merge([local, remote], selection: selection, failed: true)
    #expect(merged.total == 12)
    #expect(merged.bucketKeys.count <= 60)
    #expect(merged.points.allSatisfy { merged.bucketKeys.contains($0.date) })
    #expect(merged.points.first { $0.source == "codex" }?.value == 5)
    #expect(merged.partial)
}

@Test @MainActor func homeActivityPublishesFastMachineBeforeSlowMachineCompletes() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let executable = directory.appendingPathComponent("fixture-cli")
    let script = #"""
    #!/bin/sh
    fixture_dir=${0%/*}
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--machine" ]; then
        shift
        machine=$1
        break
      fi
      shift
    done
    if [ "$machine" = "slow-peer" ]; then
      attempts=0
      while [ ! -f "$fixture_dir/release" ] && [ "$attempts" -lt 200 ]; do
        sleep 0.01
        attempts=$((attempts + 1))
      done
      [ -f "$fixture_dir/release" ] || exit 11
    fi
    printf '%s' '{"token_usage_enabled":true,"partial":false,"points":[{"timestamp_ms":86400000,"source":"codex","value":5}]}'
    """#
    try script.write(to: executable, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
    let selection = HomeActivitySelection(metric: "sessions", timeframe: .all, query: nil,
        project: nil, source: nil, origin: .all, nowMS: 172_800_000)
    var batches: [HomeActivityBatch] = []
    try await MemexClient(executable: executable).activityBatches(selection, machines: ["local", "slow-peer"]) { batch in
        batches.append(batch)
        if batch.machine == "local" { FileManager.default.createFile(atPath: directory.appendingPathComponent("release").path, contents: nil) }
    }
    #expect(batches.map(\.machine) == ["local", "slow-peer"])
    #expect(batches.allSatisfy { $0.error == nil && $0.payload != nil })
}
