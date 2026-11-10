import AppKit
import SwiftUI
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
            for limit in [200, 400, 600] {
                for project in ["memex", "other"] {
                    let rows: [[String: Any]] = (0..<min(limit, 450)).map { index in
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

private struct PagingHome: View {
    @Bindable var store: Store
    var body: some View {
        HomeView(store: store).task(id: store.requestID) { await store.loadSessions() }
    }
}

@MainActor @Suite(.serialized) struct StorePagingTests {
    @Test func homeScrollLoadsPagesAndStopsAtTheEnd() async throws {
        let fixture = try PagingFixture()
        defer { fixture.cleanUp() }
        let store = Store(client: fixture.client)
        store.machines = [.local]
        await store.loadSessions()
        let window = NSWindow(contentRect: NSRect(x: -10000, y: -10000, width: 1000, height: 700),
            styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.alphaValue = 0
        window.contentViewController = NSHostingController(rootView: PagingHome(store: store))
        window.setContentSize(NSSize(width: 1000, height: 700))
        window.orderBack(nil)
        defer { window.close() }
        func scrollView(in view: NSView) -> NSScrollView? {
            if let scroll = view as? NSScrollView { return scroll }
            return view.subviews.lazy.compactMap { scrollView(in: $0) }.first
        }
        window.contentView?.layoutSubtreeIfNeeded()
        let content = try #require(window.contentView)
        let scroll = try #require(scrollView(in: content))
        #expect(scroll.contentView.bounds.height > 0)
        for expectedCount in [400, 450] {
            let deadline = Date().addingTimeInterval(15)
            while (store.sessions.count < expectedCount || store.loadingSessions), Date() < deadline {
                window.contentView?.layoutSubtreeIfNeeded()
                let bottom = max(0, (scroll.documentView?.bounds.height ?? 0) - scroll.contentView.bounds.height)
                scroll.contentView.scroll(to: NSPoint(x: 0, y: bottom))
                scroll.reflectScrolledClipView(scroll.contentView)
                try await Task.sleep(for: .milliseconds(50))
            }
            // The next page may finish before the scroll loop observes this one.
            #expect(store.sessions.count >= expectedCount)
        }
        #expect(store.sessions.count == 450)
        #expect(!store.hasMoreSessions)
        #expect(store.sessionLimit == 600)
        #expect(Set(store.sessions.map(\.id)).count == 450)
    }

    @Test func delayedPeerPagePreservesVisibleRemoteRow() async throws {
        let fixture = try PagingFixture()
        defer { fixture.cleanUp() }
        let store = Store(client: fixture.client)
        store.machines = [.local, MachineChoice(id: "peer", label: "Peer")]
        await store.loadSessions()
        let anchorIndex = try #require(store.sessions.indices.first { $0 > 150 && store.sessions[$0].machineID == "peer" })
        let anchorID = store.sessions[anchorIndex].id
        try fixture.mark("hold-peer")
        store.loadMoreSessionsIfNeeded(visibleID: try #require(store.sessions.last?.id))
        let pending = Task { await store.loadSessions() }
        defer { pending.cancel() }
        try await waitForPage(store)
        #expect(store.loadingSessions)
        #expect(store.sessions.contains { $0.id == anchorID })
        try fixture.remove("hold-peer")
        await pending.value
        #expect(store.sessions.count == 400)
        #expect(store.sessions.contains { $0.id == anchorID })
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
