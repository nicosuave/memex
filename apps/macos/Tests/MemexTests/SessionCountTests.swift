import Foundation
import Testing
@testable import Memex

private struct CountFixture {
    let directory: URL
    let client: MemexClient

    init() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let executable = directory.appendingPathComponent("cli")
        let script = #"""
        #!/bin/sh
        directory=$(dirname "$0")
        shift 2
        command="$1"; shift
        count=no; machine=local; query=''; limit=200
        for argument in "$@"; do
          [ "$argument" = --count ] && count=yes
        done
        log="$directory/$command-$count-args"
        printf '%s\n' "$@" >> "$log"
        while [ "$#" -gt 0 ]; do
          case "$1" in
            --machine) shift; machine="$1";;
            --query=*) query=${1#--query=};;
            --limit) shift; limit="$1";;
          esac
          shift
        done
        if [ "$count" = yes ]; then
          printf 'request\n' >> "$directory/requests"
          if [ -e "$directory/hold" ] && { [ -z "$query" ] || [ "$query" = first ]; }; then
            touch "$directory/started"
            while [ -e "$directory/hold" ]; do sleep 0.01; done
          fi
          response="$directory/response-$machine"
          [ -n "$query" ] && [ -f "$directory/response-$query" ] && response="$directory/response-$query"
          if [ "$(cat "$response")" = error ]; then
            echo 'unsupported count' >&2; exit 2
          fi
          cat "$response"
        elif [ "$command" = projects ]; then
          if [ -e "$directory/projects-error" ]; then exit 2; fi
          printf '[{"project":"memex","session_count":230},{"project":"other","session_count":70}]'
        elif [ "$command" = machines ]; then
          printf '[{"id":"local","label":"This Mac"}]'
        elif [ "$command" = sessions ]; then
          awk -v n="$limit" -v machine="$machine" 'BEGIN {
            printf "[";
            for(i=0;i<n;i++) {
              if(i)printf ",";
              printf "{\"source\":\"codex\",\"session_id\":\"s%d\",\"source_path\":\"/s%d\",\"project\":\"memex\",\"machine\":\"%s\"}",i,i,machine;
            }
            printf "]";
          }'
        else
          printf '[]'
        fi
        """#
        try script.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        client = MemexClient(executable: executable)
        try write("response-local", #"{"total":1200}"#)
        try write("response-peer", #"{"total":300}"#)
    }
    func write(_ name: String, _ text: String = "") throws {
        try text.write(to: directory.appendingPathComponent(name), atomically: true, encoding: .utf8)
    }
    func remove(_ name: String) throws { try FileManager.default.removeItem(at: directory.appendingPathComponent(name)) }
    func read(_ name: String) throws -> String { try String(contentsOf: directory.appendingPathComponent(name), encoding: .utf8) }
    func cleanUp() { try? FileManager.default.removeItem(at: directory) }
    func waitUntilStarted() async throws {
        let deadline = Date().addingTimeInterval(30)
        while !FileManager.default.fileExists(atPath: directory.appendingPathComponent("started").path), Date() < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }
        try #require(FileManager.default.fileExists(atPath: directory.appendingPathComponent("started").path))
    }
}

@MainActor @Test func conversationCountDoesNotBlockPagesOrRepeatWhenScrolling() async throws {
    let fixture = try CountFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    try fixture.write("hold")
    let count = Task { await store.loadSessionCount() }
    await store.loadSessions()
    try await fixture.waitUntilStarted()
    #expect(store.sessions.count == 200)
    #expect(store.sessionTotal == nil)
    #expect(store.sessionCountLabel == "200+")
    #expect(store.listError == nil)
    try fixture.remove("hold")
    await count.value
    #expect(store.sessionTotal == 1200)
    #expect(store.sessionCountLabel == 1200.formatted())
    let countRequest = store.sessionCountRequestID
    store.loadMoreSessionsIfNeeded(visibleID: try #require(store.sessions.last?.id))
    #expect(store.sessionCountRequestID == countRequest)
    await store.loadSessions()
    await store.loadSessionCount()
    #expect(store.sessions.count == 400)
    #expect(store.sessionTotal == 1200)
    #expect(try fixture.read("requests").split(separator: "\n").count == 1)
}

@MainActor @Test func conversationCountRequiresEveryMachineAndAcceptsExactZero() async throws {
    let fixture = try CountFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    store.machines = [.local, MachineChoice(id: "peer", label: "Peer")]
    await store.loadSessionCount()
    #expect(store.sessionTotal == 1500)
    store.query = "legacy"
    #expect(store.sessionTotal == nil)
    try fixture.write("response-peer", #"{"total":null,"reason":"fast_identity_unavailable"}"#)
    await store.loadSessionCount()
    #expect(store.sessionTotal == nil)
    #expect(store.listError == nil)
    store.machineSelection = .machine("local")
    await store.loadSessionCount()
    #expect(store.sessionTotal == 1200)
    store.query = "no matches"
    try fixture.write("response-local", #"{"total":0}"#)
    await store.loadSessionCount()
    #expect(store.sessionTotal == 0)
    #expect(store.sessionCountLabel == "0")
}

@MainActor @Test func staleConversationCountCannotReplaceNewCriteria() async throws {
    let fixture = try CountFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    try fixture.write("hold")
    try fixture.write("response-first", #"{"total":111}"#)
    try fixture.write("response-second", #"{"total":222}"#)
    store.query = "first"
    let first = Task { await store.loadSessionCount() }
    try await fixture.waitUntilStarted()
    store.query = "second"
    await store.loadSessionCount()
    #expect(store.sessionTotal == 222)
    try fixture.remove("hold")
    await first.value
    #expect(store.sessionTotal == 222)
    store.scope = .project("memex")
    #expect(store.sessionTotal == nil)
    await store.loadSessionCount()
    #expect(store.sessionTotal == 222)
    store.filters.provider = .claude
    #expect(store.sessionTotal == nil)
    await store.loadSessionCount()
    #expect(store.sessionTotal == 222)
}

@Test func countClientPreservesScopeAndOldPeerFallbackIsOnlyForUnfilteredBrowsing() async throws {
    let fixture = try CountFixture()
    defer { fixture.cleanUp() }
    let total = try await fixture.client.sessionCount(query: "--leading value", project: "memex", source: "codex",
        machine: "peer", since: "2026-09-01T00:00:00Z", origin: .subagent)
    #expect(total == 300)
    let arguments = try fixture.read("sessions-yes-args").split(separator: "\n").map(String.init)
    for argument in ["--query=--leading value", "--project", "memex", "--source", "codex", "--machine", "peer",
                     "--since", "2026-09-01T00:00:00Z", "--origin", "subagent"] {
        #expect(arguments.contains(argument))
    }
    try fixture.write("response-peer", "error")
    #expect(try await fixture.client.sessionCount(machine: "peer") == 300)
    #expect(try await fixture.client.sessionCount(project: "memex", machine: "peer") == 230)
    #expect(try await fixture.client.sessionCount(project: "missing", machine: "peer") == 0)
    #expect(try await fixture.client.sessionCount(query: "text", machine: "peer") == nil)
    #expect(try await fixture.client.sessionCount(source: "codex", machine: "peer") == nil)
    #expect(try await fixture.client.sessionCount(machine: "peer", since: "2026-09-01T00:00:00Z") == nil)
    #expect(try await fixture.client.sessionCount(machine: "peer", origin: .includingReviews) == nil)
}

@MainActor @Test func failedConversationCountDoesNotHideLoadedRowsOrClaimPartialTotal() async throws {
    let fixture = try CountFixture()
    defer { fixture.cleanUp() }
    try fixture.write("response-peer", "error")
    try fixture.write("projects-error")
    let store = Store(client: fixture.client)
    store.machines = [.local, MachineChoice(id: "peer", label: "Peer")]
    await store.loadSessions()
    await store.loadSessionCount()
    #expect(store.sessions.count == 200)
    #expect(store.sessionTotal == nil)
    #expect(store.sessionCountLabel == "200+")
    #expect(store.listError == nil)
}

@MainActor @Test func cancelledCountCanRetryTheSameCriteria() async throws {
    let fixture = try CountFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    try fixture.write("hold")
    let pending = Task { await store.loadSessionCount() }
    try await fixture.waitUntilStarted()
    pending.cancel()
    await pending.value
    #expect(store.sessionTotal == nil)
    try fixture.remove("hold")
    await store.loadSessionCount()
    #expect(store.sessionTotal == 1200)
    #expect(try fixture.read("requests").split(separator: "\n").count == 2)
}

@MainActor @Test func explicitRefreshUpdatesTheCountWithoutChangingCriteria() async throws {
    let fixture = try CountFixture()
    defer { fixture.cleanUp() }
    let catalog = ProjectCatalog(client: fixture.client, cacheDirectory: fixture.directory.appendingPathComponent("cache"))
    let store = Store(client: fixture.client, projectCatalog: catalog)
    await store.loadSessionCount()
    #expect(store.sessionTotal == 1200)
    try fixture.write("response-local", #"{"total":1400}"#)
    await store.refresh()
    #expect(store.sessionTotal == 1400)
    #expect(try fixture.read("requests").split(separator: "\n").count == 2)
}
