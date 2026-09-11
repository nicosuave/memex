import AppKit
import SwiftUI
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct HomeTests {
    @Test func automaticRefreshWaitsForStalenessAndVisibilityAndRetainsResults() async throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let executable = directory.appendingPathComponent("cli")
        let script = #"""
        #!/bin/sh
        fixture_dir=${0%/*}
        case "$3" in
          machines) printf '%s' '[{"id":"local","label":"This Mac"}]';;
          projects) printf '%s' '[]';;
          sessions)
            case "$*" in
              *--count*) printf '%s' '{"total":1}';;
              *)
                label=Before
                if [ -f "$fixture_dir/refresh" ]; then
                  touch "$fixture_dir/started"
                  attempts=0
                  while [ ! -f "$fixture_dir/release" ] && [ "$attempts" -lt 200 ]; do
                    sleep 0.01
                    attempts=$((attempts + 1))
                  done
                  [ -f "$fixture_dir/release" ] || exit 11
                  label=After
                fi
                printf '[{"source":"codex","session_id":"one","source_path":"/fixture","project":"memex","label":"%s","message_count":0}]' "$label";;
            esac;;
        esac
        """#
        try script.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        let client = MemexClient(executable: executable, root: directory.path)
        let store = Store(client: client, projectCatalog: ProjectCatalog(client: client, cacheDirectory: directory))
        await store.loadSessions()
        #expect(store.sessions.first?.title == "Before")
        let original = store.sessionCountRequestID
        await store.refreshHomeIfStale(isVisible: true)
        #expect(store.sessionCountRequestID == original)
        let stale = Date().addingTimeInterval(120)
        await store.refreshHomeIfStale(isVisible: false, now: stale)
        store.scope = .all
        await store.refreshHomeIfStale(isVisible: true, now: stale)
        store.scope = .home
        store.loadingSessions = true
        await store.refreshHomeIfStale(isVisible: true, now: stale)
        store.loadingSessions = false
        #expect(store.sessionCountRequestID == original)

        try Data().write(to: directory.appendingPathComponent("refresh"))
        store.loadingHomeActivity = true
        let activity = store.homeActivityRequestID
        let refresh = Task { await store.refreshHomeIfStale(isVisible: true, now: stale) }
        let deadline = Date().addingTimeInterval(3)
        while !FileManager.default.fileExists(atPath: directory.appendingPathComponent("started").path), Date() < deadline {
            try await Task.sleep(for: .milliseconds(10))
        }
        #expect(store.loadingSessions)
        #expect(store.sessions.first?.title == "Before")
        let active = store.sessionCountRequestID
        #expect(active != original)
        await store.refreshHomeIfStale(isVisible: true, now: stale.addingTimeInterval(120))
        #expect(store.sessionCountRequestID == active)
        try Data().write(to: directory.appendingPathComponent("release"))
        await refresh.value
        #expect(store.sessions.first?.title == "After")
        #expect(store.homeActivityRequestID == activity) // Returning Home must not restart an active token scan.
        store.loadingHomeActivity = false
        await store.refreshHomeIfStale(isVisible: true, now: stale.addingTimeInterval(1))
        #expect(store.sessionCountRequestID == active)
        await store.refreshHomeIfStale(isVisible: true, now: stale.addingTimeInterval(61))
        #expect(store.sessionCountRequestID != active)
        #expect(store.homeActivityRequestID != activity)
    }

    @Test func switchingHomeAndBrowserDoesNotRestartCatalogRequests() async throws {
        _ = NSApplication.shared
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let executable = directory.appendingPathComponent("cli")
        let script = #"""
        #!/bin/sh
        printf '%s\n' "$3" >> "$(dirname "$0")/requests"
        case "$3" in
          machines) printf '%s' '[{"id":"local","label":"This Mac"}]';;
          projects) printf '%s' '[]';;
          sessions)
            case "$*" in
              *--count*) printf '%s' '{"total":1}';;
              *) printf '%s' '[{"source":"codex","session_id":"one","source_path":"/fixture","project":"memex","label":"A named conversation","message_count":0}]';;
            esac;;
          session) printf '%s' '[{"type":"page","total":0}]';;
          activity) printf '%s' '{"points":[],"token_usage_enabled":true,"partial":false}';;
        esac
        """#
        try script.write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        let client = MemexClient(executable: executable, root: directory.path)
        let store = Store(client: client, projectCatalog: ProjectCatalog(client: client, cacheDirectory: directory))
        let window = NSWindow(contentRect: NSRect(x: -10000, y: -10000, width: 1000, height: 700),
            styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.alphaValue = 0
        window.contentViewController = NSHostingController(rootView: BrowserContent(store: store).reader)
        window.orderBack(nil)
        defer { window.close() }
        let deadline = Date().addingTimeInterval(5)
        while (store.sessions.isEmpty || store.loadingMachines || store.loadingProjects || store.homeActivityCache?.complete != true), Date() < deadline {
            try await Task.sleep(for: .milliseconds(20))
        }
        #expect(store.sessions.count == 1)
        for scope in [Store.Scope.all, .home, .all] {
            store.scope = scope
            try await Task.sleep(for: .milliseconds(200))
        }
        let requests = try String(contentsOf: directory.appendingPathComponent("requests"), encoding: .utf8)
            .split(separator: "\n")
        #expect(requests.filter { $0 == "machines" }.count == 1)
        #expect(requests.filter { $0 == "projects" }.count == 1)
        #expect(requests.filter { $0 == "sessions" }.count == 2) // list and count
        #expect(requests.filter { $0 == "activity" }.count == 1) // Returning to fresh Home reuses its chart.
    }

    @Test func launchStartsAtHomeAndOpeningResultPreservesCriteria() {
        let store = Store()
        #expect(store.scope == .home)
        #expect(store.selectedID == nil)
        store.homeProject = "memex"
        store.query = "navigation"
        let session = Session(source: "codex", sessionID: "home", sourcePath: "/fixture", project: "memex")
        store.sessions = [session]
        let criteria = store.requestID
        store.openConversation(session)
        #expect(store.scope == .project("memex"))
        #expect(store.selected == session)
        #expect(store.requestID == criteria)
        store.scope = .home
        #expect(store.selectedID == nil)
        #expect(store.query == "navigation")
        store.homeProject = nil
        store.openConversation(session)
        #expect(store.scope == .all)
    }

    @Test func homeCollapsesListAndRestoresBrowserColumnsAndToolbar() async throws {
        _ = NSApplication.shared
        let store = Store()
        let controller = BrowserColumnsController(store: store, sidebar: Text("Sidebar"),
            conversations: Text("Conversations"), reader: Text("Content"))
        let window = NSWindow(contentRect: NSRect(x: -10000, y: -10000, width: 1380, height: 700),
            styleMask: [.titled, .closable, .resizable], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.alphaValue = 0
        window.contentViewController = controller
        controller.splitView.autosaveName = nil
        window.orderBack(nil)
        defer { window.close() }
        for scope in [Store.Scope.home, .all, .project("memex"), .home, .all] {
            store.scope = scope
            try await Task.sleep(for: .milliseconds(100))
            window.contentView?.layoutSubtreeIfNeeded()
            #expect(controller.splitViewItems[1].isCollapsed == (scope == .home))
            let toolbar = try #require(window.toolbar)
            let separators = toolbar.items.compactMap { $0 as? NSTrackingSeparatorToolbarItem }
            #expect(separators.count == (scope == .home ? 1 : 2))
            #expect(toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.search } == (scope != .home))
            #expect(toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.title } == (scope != .home))
            #expect(toolbar.items.contains { $0.itemIdentifier == BrowserToolbarController.refresh } == (scope != .home))
            #expect(controller.readerHost.view.frame.width > 0)
        }
    }
}
