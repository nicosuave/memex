import Foundation
import Testing
@testable import Memex

private struct ReaderFixture {
    let directory: URL
    let client: MemexClient
    init() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let executable = directory.appendingPathComponent("cli")
        let script = #"""
        #!/bin/sh
        offset=0; limit=60; full=0; budget=0; page_info=0; session=''
        printf '%s\n' "$*" >> "$(dirname "$0")/requests"
        while [ "$#" -gt 0 ]; do
          case "$1" in
            --offset) shift; offset="$1";;
            --limit) shift; limit="$1";;
            --full) full=1;;
            --page-info)
              if [ -f "$(dirname "$0")/legacy" ]; then echo "unexpected argument '--page-info'" >&2; exit 2; fi
              page_info=1;;
            --max-chars) shift; budget="$1";;
            --) shift; session="$1"; break;;
          esac
          shift
        done
        if [ "$session" = slow ]; then
          touch "$(dirname "$0")/started"
          attempts=0
          while [ ! -f "$(dirname "$0")/release" ]; do
            sleep 0.02
            attempts=$((attempts + 1))
            [ "$attempts" -lt 3000 ] || exit 4
          done
        fi
        # Model a shared character budget returning fewer IDs than the limit.
        if [ "$full" = 0 ]; then
          if [ "$limit" = 1 ]; then [ "$budget" = 1 ] || exit 2
          else [ "$budget" = 262144 ] || exit 3; limit=300
          fi
        fi
        awk -v offset="$offset" -v limit="$limit" -v full="$full" -v page_info="$page_info" -v session="$session" 'BEGIN {
          total=620; end=offset+limit; if(end>total)end=total;
          printf "[";
          for(i=offset;i<end;i++) {
            if(i>offset)printf ",";
            printf "{\"record_id\":\"%s-r%d\",\"record\":{\"role\":\"assistant\",\"text\":\"%s\"}}",session,i,full ? "complete text" : "";
          }
          if(!full || page_info) {
            if(end>offset)printf ",";
            printf "{\"type\":\"page\",\"total\":620,\"next_offset\":%s}",(end<total ? end : "null");
          }
          printf "]";
        }'
        """#
        try script.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        client = MemexClient(executable: executable)
    }
    func cleanUp() { try? FileManager.default.removeItem(at: directory) }
    func session(_ id: String, anchor: String? = nil, count: Int? = nil) -> Session {
        Session(source: "codex", sessionID: id, sourcePath: "/\(id)", project: "memex", searchRecordID: anchor, messageCount: count)
    }
}

@MainActor @Test func readerUsesListCountToLoadCompleteLatestPageInOneRequest() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let session = try JSONDecoder().decode(Session.self, from: Data(#"{"source":"codex","session_id":"one","source_path":"/one","project":"memex","message_count":620}"#.utf8))
    let store = Store(client: fixture.client)
    store.sessions = [session]
    store.selectedID = session.id
    await store.loadRecords()
    #expect(store.readerError == nil)
    #expect(store.recordsOffset == 560)
    #expect(store.records.count == 60)
    #expect(store.records.last?.id == "one-r619")
    #expect(store.records.allSatisfy { $0.record.text == "complete text" })
    #expect(!store.hasMoreRecords)
    let requests = try String(contentsOf: fixture.directory.appendingPathComponent("requests"), encoding: .utf8).split(separator: "\n")
    #expect(requests.count == 1)
    #expect(requests[0].contains("--page-info"))
    #expect(!requests[0].contains("--max-chars"))
}

@Test(arguments: [0, 100, 1000]) func initialPageCorrectsStaleListCount(count: Int) async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let page = try await fixture.client.initialRecords(for: fixture.session("one", count: count), anchor: nil)
    #expect(page.offset == 560)
    #expect(page.total == 620)
    #expect(page.records.last?.id == "one-r619")
    let requests = try String(contentsOf: fixture.directory.appendingPathComponent("requests"), encoding: .utf8).split(separator: "\n")
    #expect(requests.count == 2)
    #expect(requests.allSatisfy { $0.contains("--page-info") })
}

@Test func initialPageSupportsOlderCLIWithoutPageInfo() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    try Data().write(to: fixture.directory.appendingPathComponent("legacy"))
    let page = try await fixture.client.initialRecords(for: fixture.session("one", count: 620), anchor: nil)
    #expect(page.offset == 560)
    #expect(page.total == 620)
    #expect(page.records.count == 60)
    #expect(page.records.first?.record.text == "complete text")
}

@MainActor @Test func readerStartsAtNewestAndPrependsWithoutOverlapThenRestoresWindow() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    let first = fixture.session("one"), second = fixture.session("two")
    store.sessions = [first, second]
    store.selectedID = first.id
    await store.loadRecords()
    #expect(store.readerError == nil)
    #expect(store.recordsOffset == 560)
    #expect(store.records.first?.id == "one-r560")
    #expect(store.records.last?.id == "one-r619")
    #expect(store.readerStartsAtEnd)
    #expect(store.hasEarlierRecords && !store.hasMoreRecords)
    await store.loadEarlierRecords()
    #expect(store.recordsOffset == 500)
    #expect(store.records.count == 120)
    #expect(Set(store.records.map(\.id)).count == 120)
    let loaded = store.records
    store.selectedID = second.id
    #expect(store.loadedReaderKey != store.readerPositionKey)
    await store.loadRecords()
    #expect(store.readerError == nil)
    store.selectedID = first.id
    await store.loadRecords()
    #expect(store.readerError == nil)
    #expect(store.records == loaded)
    #expect(store.recordsOffset == 500)
    #expect(store.loadedReaderKey == store.readerPositionKey)
}

@MainActor @Test func readerFindsSearchAnchorBeyondMetadataPageAndKeepsBrowseSeparate() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    let session = fixture.session("one", anchor: "one-r540")
    store.sessions = [session]
    store.selectedID = session.id
    await store.loadRecords()
    #expect(store.readerError == nil)
    let browseKey = store.readerPositionKey
    store.query = "needle"
    #expect(store.readerPositionKey != browseKey)
    await store.loadRecords()
    #expect(store.readerError == nil)
    #expect(store.readerAnchorID == "one-r540")
    #expect(!store.readerStartsAtEnd)
    #expect(store.recordsOffset == 510)
    #expect(store.records.contains { $0.id == "one-r540" })
    #expect(store.hasEarlierRecords && store.hasMoreRecords)
    await store.loadMoreRecords()
    #expect(store.records.last?.id == "one-r619")
    #expect(!store.hasMoreRecords)
    store.query = ""
    await store.loadRecords()
    #expect(store.readerError == nil)
    #expect(store.recordsOffset == 560)
    #expect(store.records.count == 60)
}

@MainActor @Test func readerIgnoresOutdatedSelectionRequests() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    let slow = fixture.session("slow"), fast = fixture.session("fast")
    store.sessions = [slow, fast]
    store.selectedID = slow.id
    let pending = Task { await store.loadRecords() }
    let deadline = Date().addingTimeInterval(3)
    while !FileManager.default.fileExists(atPath: fixture.directory.appendingPathComponent("started").path), Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    store.selectedID = fast.id
    await store.loadRecords()
    #expect(store.readerError == nil)
    try Data().write(to: fixture.directory.appendingPathComponent("release"))
    await pending.value
    #expect(store.records.first?.id == "fast-r560")
    #expect(store.loadedReaderKey == store.readerPositionKey)
    #expect(!store.loadingRecords)
}

@Test func searchCarriesRecordIdentityIntoKnownSession() throws {
    let hit = try JSONDecoder().decode(SearchHit.self, from: Data(#"{"source":"codex","session_id":"s","source_path":"/s","project":"p","record_id":"matched"}"#.utf8))
    let known = Session(source: "codex", sessionID: "s", sourcePath: "/s", project: "p", label: "Title")
    let session = hit.session(known: [known.id: known])
    #expect(session.searchRecordID == "matched")
    #expect(session.title == "Title")
}

@MainActor @Test func readerCancellationDoesNotPublishPartialWindow() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    let session = fixture.session("slow")
    store.sessions = [session]
    store.selectedID = session.id
    let pending = Task { await store.loadRecords() }
    let deadline = Date().addingTimeInterval(3)
    while !FileManager.default.fileExists(atPath: fixture.directory.appendingPathComponent("started").path), Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    pending.cancel()
    await pending.value
    #expect(store.records.isEmpty)
    #expect(store.loadedReaderKey == nil)
    #expect(store.readerError == nil)
    #expect(!store.loadingRecords)
}

@MainActor @Test func changedSearchAnchorLoadsItsOwnWindowForSameQuery() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    store.sessions = [fixture.session("one", anchor: "one-r540")]
    store.selectedID = store.sessions[0].id
    store.query = "needle"
    await store.loadRecords()
    #expect(store.readerError == nil)
    let previousKey = store.readerPositionKey
    store.sessions = [fixture.session("one", anchor: "one-r100")]
    #expect(store.readerPositionKey != previousKey)
    await store.loadRecords()
    #expect(store.readerError == nil)
    #expect(store.recordsOffset == 70)
    #expect(store.records.contains { $0.id == "one-r100" })
}

@MainActor @Test func findRevealLoadsTheKnownMatchWindowAndKeepsCurrentWindowForVisibleHits() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    let session = fixture.session("one")
    store.sessions = [session]
    store.selectedID = session.id
    await store.loadRecords()
    #expect(store.recordsOffset == 560)
    await store.revealRecord("one-r100", offset: 100)
    #expect(store.readerError == nil)
    #expect(store.recordsOffset == 70)
    #expect(store.records.contains { $0.id == "one-r100" })
    #expect(store.hasEarlierRecords && store.hasMoreRecords)
    await store.revealRecord("one-r110", offset: 110)
    #expect(store.recordsOffset == 70)
}

@MainActor @Test func findRevealWithKnownOffsetLoadsInOneRequest() async throws {
    let fixture = try ReaderFixture()
    defer { fixture.cleanUp() }
    let store = Store(client: fixture.client)
    let session = fixture.session("one")
    store.sessions = [session]
    store.selectedID = session.id
    await store.revealRecord("one-r100", offset: 100)
    #expect(store.readerError == nil)
    #expect(store.recordsOffset == 70)
    #expect(store.records.contains { $0.id == "one-r100" })
    #expect(store.hasEarlierRecords && store.hasMoreRecords)
    let requests = try String(contentsOf: fixture.directory.appendingPathComponent("requests"), encoding: .utf8).split(separator: "\n")
    #expect(requests.count == 1)
}
