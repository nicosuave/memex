import AppKit
import SwiftUI
import Foundation
import Testing
@testable import Memex

@Test func timeframeUsesRollingDurationsAndAllTimeClearsTheBound() throws {
    let now = Date(timeIntervalSince1970: 1_800_000_000)
    #expect(ConversationTimeframe.all.since(relativeTo: now) == nil)
    for (timeframe, days) in [(ConversationTimeframe.day, 1.0), (.week, 7.0), (.month, 30.0)] {
        let value = try #require(timeframe.since(relativeTo: now))
        let since = try Date.ISO8601FormatStyle().parse(value)
        #expect(now.timeIntervalSince(since) == days * 86_400)
    }
}

private struct FilterFixture {
    let directory: URL
    let client: MemexClient
    init() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let executable = directory.appendingPathComponent("cli")
        let recent = Date().addingTimeInterval(-3600).formatted(.iso8601)
        let old = Date().addingTimeInterval(-45 * 86_400).formatted(.iso8601)
        let script = #"""
        #!/bin/sh
        shift 2
        command="$1"; shift
        project=''; source=''; origin=all; since=''; machine=local
        while [ "$#" -gt 0 ]; do
          case "$1" in
            --project) shift; project="$1";;
            --source) shift; source="$1";;
            --origin) shift; origin="$1";;
            --since) shift; since="$1";;
            --machine) shift; machine="$1";;
          esac
          shift
        done
        if [ "$source" = claude ] && [ -e "$(dirname "$0")/hold" ]; then
          touch "$(dirname "$0")/started"
          while [ -e "$(dirname "$0")/hold" ]; do sleep 0.01; done
        fi
        awk -v project="$project" -v source="$source" -v origin="$origin" -v since="$since" -v machine="$machine" -v recent='RECENT_DATE' -v old='OLD_DATE' 'BEGIN {
          sources[1]="codex"; sources[2]="claude"; sources[3]="codex"; sources[4]="codex"; sources[5]="codex";
          projects[1]="memex"; projects[2]="memex"; projects[3]="other"; projects[4]="memex"; projects[5]="memex";
          kinds[1]="subagent"; kinds[2]="interactive"; kinds[3]="subagent"; kinds[4]="subagent"; kinds[5]="interactive";
          printf "["; count=0;
          for(i=1;i<=5;i++) {
            ts=(i==4 ? old : recent);
            if(project!="" && projects[i]!=project)continue;
            if(source!="" && sources[i]!=source)continue;
            if(origin!="all" && kinds[i]!=origin)continue;
            if(since!="" && ts<since)continue;
            if(count++)printf ",";
            printf "{\"source\":\"%s\",\"session_id\":\"r%d\",\"source_path\":\"/%s/r%d\",\"project\":\"%s\",\"last_at\":\"%s\",\"label\":\"Fixture %d\",\"snippet\":\"needle\",\"record_id\":\"match%d\",\"machine\":\"%s\"}",sources[i],i,machine,i,projects[i],ts,i,i,machine;
          }
          printf "]";
        }'
        """#
        try script.replacingOccurrences(of: "RECENT_DATE", with: recent)
            .replacingOccurrences(of: "OLD_DATE", with: old)
            .write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        client = MemexClient(executable: executable)
    }
    func cleanUp() { try? FileManager.default.removeItem(at: directory) }
}

@MainActor @Test func filtersIntersectForBrowsingAndSearchOnEverySelectedMachine() async throws {
    let fixture = try FilterFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    store.machines = [.local, MachineChoice(id: "fixture-remote", label: "Remote")]
    store.scope = .project("memex")
    store.filters = ConversationFilters(timeframe: .day, provider: .codex, origin: .subagent)
    await store.loadSessions()
    #expect(store.listError == nil)
    #expect(store.sessions.count == 2)
    #expect(Set(store.sessions.map(\.sessionID)) == ["r1"])
    #expect(Set(store.sessions.map(\.machineID)) == ["local", "fixture-remote"])
    store.query = "needle"
    await store.loadSessions()
    #expect(store.sessions.count == 2)
    #expect(Set(store.sessions.map(\.sessionID)) == ["r1"])
    #expect(store.sessions.allSatisfy { $0.searchRecordID == "match1" })
    store.query = ""
    store.filters.origin = .interactive
    await store.loadSessions()
    #expect(Set(store.sessions.map(\.sessionID)) == ["r5"])
    store.filters = .defaults
    await store.loadSessions()
    #expect(Set(store.sessions.map(\.sessionID)) == ["r1", "r2", "r4", "r5"])
    store.scope = .all
    await store.loadSessions()
    #expect(store.sessions.count == 10)
}

@MainActor @Test func filterChangesResetPaginationAndIgnoreOldResults() async throws {
    let fixture = try FilterFixture()
    defer { fixture.cleanUp() }
    let hold = fixture.directory.appendingPathComponent("hold")
    try Data().write(to: hold)
    let store = Store(client: fixture.client)
    store.filters.provider = .claude
    let pending = Task { await store.loadSessions() }
    let deadline = Date().addingTimeInterval(3)
    while !FileManager.default.fileExists(atPath: fixture.directory.appendingPathComponent("started").path), Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    store.sessionLimit = 600
    store.filters.provider = .codex
    #expect(store.sessionLimit == 200)
    await store.loadSessions()
    try FileManager.default.removeItem(at: hold)
    await pending.value
    #expect(store.sessions.count == 4)
    #expect(store.sessions.allSatisfy { $0.source == "codex" })
    #expect(!store.loadingSessions)
}

@MainActor @Test func explicitFilterChoicesSurviveRelaunchAndReset() throws {
    let suite = "dev.memex.filters.test.\(UUID())"
    let preferences = try #require(UserDefaults(suiteName: suite))
    defer { preferences.removePersistentDomain(forName: suite) }
    let store = Store(filterPreferences: preferences)
    #expect(store.filters == .defaults)
    store.filters = ConversationFilters(timeframe: .week, provider: .codex, origin: .interactive)
    let restored = Store(filterPreferences: preferences)
    #expect(restored.filters == store.filters)
    restored.filters = .defaults
    #expect(Store(filterPreferences: preferences).filters == .defaults)
}

@MainActor @Test func filterButtonRemainsCompactWithActiveFilters() {
    _ = NSApplication.shared
    let store = Store()
    store.filters = ConversationFilters(timeframe: .month, provider: .openclaw, origin: .interactive)
    let host = NSHostingView(rootView: ConversationFilterButton(store: store))
    host.frame = NSRect(x: 0, y: 0, width: 260, height: 40)
    host.layoutSubtreeIfNeeded()
    #expect(host.fittingSize.height <= 44)
    #expect(store.filters.summary == "Last 30 days · OpenClaw · Interactive")
}
