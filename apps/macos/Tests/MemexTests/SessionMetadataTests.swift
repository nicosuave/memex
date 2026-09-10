import Foundation
import Testing
@testable import Memex

private struct MetadataFixture {
    let directory: URL
    let client: MemexClient
    init() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let executable = directory.appendingPathComponent("cli")
        try #"""
        #!/bin/sh
        base=$(dirname "$0")
        printf '%s\n' "$@" > "$base/arguments"
        touch "$base/started"
        while [ -e "$base/hold" ]; do sleep 0.01; done
        if [ "$3" = session ] && [ -f "$base/opening" ]; then cat "$base/opening"; else cat "$base/result"; fi
        """#.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        try #"[{"source":"codex","session_id":"old","source_path":"/old file","project":"p","resume_cmd":"custom-resume old","cwd":"/tmp/p"}]"#
            .write(to: directory.appendingPathComponent("result"), atomically: true, encoding: .utf8)
        client = MemexClient(executable: executable)
    }
    func cleanup() { try? FileManager.default.removeItem(at: directory) }
    var hit: Session {
        Session(source: "codex", sessionID: "old", sourcePath: "/old file", project: "p",
                label: "Search title", snippet: "matched text", searchRecordID: "anchor")
    }
}

@MainActor @Test func searchMetadataUsesExactIdentityAndPreservesReaderAnchor() async throws {
    let fixture = try MetadataFixture()
    defer { fixture.cleanup() }
    let store = Store(client: fixture.client)
    store.sessions = [fixture.hit]
    store.selectedID = fixture.hit.id
    let readerID = store.readerRequestID
    await store.loadSelectedSessionMetadata()
    let selected = try #require(store.selected)
    #expect(selected.resumeCommand == "custom-resume old")
    #expect(selected.cwd == "/tmp/p")
    #expect(selected.snippet == "matched text")
    #expect(selected.searchRecordID == "anchor")
    #expect(selected.label == "Search title")
    #expect(store.readerRequestID == readerID)
    #expect(store.sessionMetadataError == nil)
    let args = try String(contentsOf: fixture.directory.appendingPathComponent("arguments"), encoding: .utf8)
    #expect(args.contains("--session-id=old\n--source-path=/old file\n--origin\nall\n--limit\n1"))
}

@MainActor @Test func metadataRejectsWrongIdentity() async throws {
    let fixture = try MetadataFixture()
    defer { fixture.cleanup() }
    let store = Store(client: fixture.client)
    var hit = fixture.hit
    hit.machine = nil
    store.sessions = [hit]
    store.selectedID = hit.id
    try #"[{"source":"claude","session_id":"old","source_path":"/old file","project":"p","resume_cmd":"wrong"}]"#
        .write(to: fixture.directory.appendingPathComponent("result"), atomically: true, encoding: .utf8)
    await store.loadSelectedSessionMetadata()
    #expect(store.selected?.resumeCommand == nil)
    #expect(store.sessionMetadataError != nil)
    #expect(!store.loadingSessionMetadata)
}

@MainActor @Test func metadataResponseCannotUpdateANewSelection() async throws {
    let fixture = try MetadataFixture()
    defer { fixture.cleanup() }
    let hold = fixture.directory.appendingPathComponent("hold")
    try Data().write(to: hold)
    let store = Store(client: fixture.client)
    store.sessions = [fixture.hit]
    store.selectedID = fixture.hit.id
    let pending = Task { await store.loadSelectedSessionMetadata() }
    let deadline = Date().addingTimeInterval(3)
    while !FileManager.default.fileExists(atPath: fixture.directory.appendingPathComponent("started").path), Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    #expect(store.loadingSessionMetadata)
    store.selectedID = nil
    await store.loadSelectedSessionMetadata()
    try FileManager.default.removeItem(at: hold)
    await pending.value
    #expect(store.sessions.first?.resumeCommand == nil)
    #expect(!store.loadingSessionMetadata)
    #expect(store.sessionMetadataError == nil)
}

@MainActor @Test func remoteSearchLoadsOpeningMessageTitleWithoutReplacingMatch() async throws {
    let fixture = try MetadataFixture()
    defer { fixture.cleanup() }
    var hit = fixture.hit
    hit.machine = "nicbook-atm"
    hit.label = nil
    try #"[{"source":"codex","session_id":"old","source_path":"/old file","project":"p","machine":"nicbook-atm","label":"Investigate equality deletes"}]"#
        .write(to: fixture.directory.appendingPathComponent("result"), atomically: true, encoding: .utf8)
    let store = Store(client: fixture.client)
    store.sessions = [hit]
    store.selectedID = hit.id
    let readerID = store.readerRequestID
    await store.loadSelectedSessionMetadata()
    #expect(store.selected?.title == "Investigate equality deletes")
    #expect(store.selected?.snippet == "matched text")
    #expect(store.selected?.searchRecordID == "anchor")
    #expect(store.readerRequestID == readerID)
    #expect(store.sessionMetadataError == nil)
    let args = try String(contentsOf: fixture.directory.appendingPathComponent("arguments"), encoding: .utf8)
    #expect(args.contains("--machine\nnicbook-atm\n"))
    try FileManager.default.removeItem(at: fixture.directory.appendingPathComponent("started"))
    await store.loadSelectedSessionMetadata()
    #expect(!FileManager.default.fileExists(atPath: fixture.directory.appendingPathComponent("started").path))
}

@MainActor @Test func untitledRecentSubagentUsesOpeningContextName() async throws {
    let fixture = try MetadataFixture()
    defer { fixture.cleanup() }
    var hit = fixture.hit
    hit.machine = "nicbook-atm"
    hit.label = nil
    hit.searchRecordID = nil
    try #"[{"source":"codex","session_id":"old","source_path":"/old file","project":"p","machine":"nicbook-atm"}]"#
        .write(to: fixture.directory.appendingPathComponent("result"), atomically: true, encoding: .utf8)
    try #"[{"record_id":"context","record":{"role":"developer","text":"<context_window>\nAgent name: /root/activity_backfill\n</context_window>"}},{"record_id":"user","record":{"role":"user","text":"<recommended_plugins>plugins</recommended_plugins><environment_context>env</environment_context>"}}]"#
        .write(to: fixture.directory.appendingPathComponent("opening"), atomically: true, encoding: .utf8)
    let store = Store(client: fixture.client)
    store.sessions = [hit]
    store.selectedID = hit.id
    let readerID = store.readerRequestID
    await store.loadSelectedSessionMetadata()
    #expect(store.selected?.title == "Activity backfill")
    #expect(store.readerRequestID == readerID)
    #expect(store.sessionMetadataError == nil)
    let args = try String(contentsOf: fixture.directory.appendingPathComponent("arguments"), encoding: .utf8)
    #expect(args.contains("--offset\n0\n--limit\n16\n"))
}
