import AppKit
import Testing
@testable import Memex

private struct PagingFixture {
    let directory: URL
    let client: MemexClient

    init() throws {
        directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let executable = directory.appendingPathComponent("cli")
        let script = #"""
        #!/bin/sh
        directory=$(dirname "$0")
        machine=local; limit=200; project=memex
        while [ "$#" -gt 0 ]; do
          case "$1" in
            --machine) shift; machine="$1";;
            --limit) shift; limit="$1";;
            --project) shift; project="$1";;
          esac
          shift
        done
        if [ "$machine" = peer ]; then
          while [ -e "$directory/hold-peer" ]; do sleep 0.01; done
          if [ -e "$directory/fail-peer" ]; then echo 'Peer unavailable' >&2; exit 2; fi
        fi
        cat "$directory/$machine-$limit-$project.json"
        """#
        try script.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        client = MemexClient(executable: executable)
        let formatter = ISO8601DateFormatter()
        for machine in ["local", "peer"] {
            for limit in [200, 400] {
                for project in ["memex", "other"] {
                    let rows: [[String: Any]] = (0..<limit).map { index in
                        let timestamp = Date(timeIntervalSince1970: 1_789_000_000 - Double(index * 2 + (machine == "peer" ? 1 : 0)))
                        return ["source": "codex", "session_id": "\(project)-\(index)", "source_path": "/\(project)/\(index)",
                                "project": project, "machine": machine, "label": "Page \(limit)",
                                "last_at": formatter.string(from: timestamp)]
                    }
                    try JSONSerialization.data(withJSONObject: rows).write(to: directory.appendingPathComponent("\(machine)-\(limit)-\(project).json"))
                }
            }
        }
    }
    func mark(_ name: String) throws { try Data().write(to: directory.appendingPathComponent(name)) }
    func remove(_ name: String) throws { try FileManager.default.removeItem(at: directory.appendingPathComponent(name)) }
    func cleanUp() { try? FileManager.default.removeItem(at: directory) }
}

@MainActor private func waitForPage(_ store: Store, project: String = "memex") async throws {
    let deadline = Date().addingTimeInterval(10)
    while !store.sessions.contains(where: { $0.machineID == "local" && $0.label == "Page 400" && $0.project == project }), Date() < deadline {
        try await Task.sleep(for: .milliseconds(10))
    }
    try #require(store.sessions.contains { $0.machineID == "local" && $0.label == "Page 400" && $0.project == project })
}

@MainActor @Suite(.serialized) struct StorePagingTests {
    @Test func delayedPeerPagePreservesVisibleRemoteRowAndNativeScrollAnchor() async throws {
        let fixture = try PagingFixture()
        defer { fixture.cleanUp() }
        let store = Store(client: fixture.client)
        store.machines = [.local, MachineChoice(id: "peer", label: "Peer")]
        await store.loadSessions()
        let controller = ConversationListController()
        let window = NSWindow(contentRect: NSRect(x: -10000, y: -10000, width: 300, height: 600), styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.alphaValue = 0
        window.contentViewController = controller
        window.setContentSize(NSSize(width: 300, height: 600))
        defer { window.close() }
        controller.update(sessions: store.sessions, selectedID: store.sessions.first?.id, select: { _ in }, loadMore: { _ in })
        window.orderBack(nil)
        await Task.yield()
        window.contentView?.layoutSubtreeIfNeeded()
        let anchorIndex = try #require(store.sessions.indices.first { $0 > 150 && store.sessions[$0].machineID == "peer" })
        let anchorID = store.sessions[anchorIndex].id
        let anchorY = controller.table.rect(ofRow: anchorIndex).minY + 7
        controller.scrollView.contentView.scroll(to: NSPoint(x: 0, y: anchorY))
        controller.scrollView.reflectScrolledClipView(controller.scrollView.contentView)
        try fixture.mark("hold-peer")
        store.loadMoreSessionsIfNeeded(visibleID: try #require(store.sessions.last?.id))
        let pending = Task { await store.loadSessions() }
        defer { pending.cancel() }
        try await waitForPage(store)
        #expect(store.loadingSessions)
        #expect(store.sessions.contains { $0.id == anchorID })
        controller.update(sessions: store.sessions, selectedID: store.sessions.first?.id, select: { _ in }, loadMore: { _ in })
        controller.table.layoutSubtreeIfNeeded()
        #expect(abs(controller.scrollView.contentView.bounds.minY - anchorY) < 1)
        try fixture.remove("hold-peer")
        await pending.value
        controller.update(sessions: store.sessions, selectedID: store.sessions.first?.id, select: { _ in }, loadMore: { _ in })
        controller.table.layoutSubtreeIfNeeded()
        #expect(store.sessions.count == 400)
        #expect(store.sessions.contains { $0.id == anchorID })
        #expect(abs(controller.scrollView.contentView.bounds.minY - anchorY) < 1)
    }

    @Test func failedPeerPageKeepsLoadedRowsAndReportsFailure() async throws {
        let fixture = try PagingFixture()
        defer { fixture.cleanUp() }
        let store = Store(client: fixture.client)
        store.machines = [.local, MachineChoice(id: "peer", label: "Peer")]
        await store.loadSessions()
        let previousPeerIDs = Set(store.sessions.filter { $0.machineID == "peer" }.map(\.id))
        try fixture.mark("fail-peer")
        store.loadMoreSessionsIfNeeded(visibleID: try #require(store.sessions.last?.id))
        await store.loadSessions()
        #expect(previousPeerIDs.isSubset(of: Set(store.sessions.map(\.id))))
        #expect(store.listError?.contains("Peer unavailable") == true)
    }

    @Test func changedProjectAndMachineDoNotReuseOtherCriteriaRows() async throws {
        let fixture = try PagingFixture()
        defer { fixture.cleanUp() }
        let store = Store(client: fixture.client)
        store.machines = [.local, MachineChoice(id: "peer", label: "Peer")]
        await store.loadSessions()
        try fixture.mark("hold-peer")
        store.scope = .project("other")
        store.sessionLimit = 400
        let pending = Task { await store.loadSessions() }
        defer { pending.cancel() }
        try await waitForPage(store, project: "other")
        #expect(store.sessions.allSatisfy { $0.project == "other" && $0.machineID == "local" })
        try fixture.remove("hold-peer")
        await pending.value
        #expect(store.sessions.contains { $0.machineID == "peer" })
        store.machineSelection = .machine("local")
        await store.loadSessions()
        #expect(store.sessions.allSatisfy { $0.machineID == "local" && $0.project == "other" })
    }
}
