import Foundation
import AppKit
import SwiftUI
import Testing
@testable import Memex

private let projectFixture = [
    ProjectSummary(project: "zeta", sessionCount: 300, lastAt: "2026-01-01T00:00:00Z"),
    ProjectSummary(project: "alpha", sessionCount: 2, lastAt: "2026-02-02T00:00:00.123Z"),
    ProjectSummary(project: "beta", sessionCount: 300, lastAt: "2026-02-01T00:00:00Z"),
    ProjectSummary(project: "undated", sessionCount: 1, lastAt: nil),
]

@Test func projectCacheSurvivesRestartAndPersistsSortWithFullCounts() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let client = MemexClient(root: "/fixture-a")
    let first = ProjectCatalog(client: client, cacheDirectory: directory, fetch: { projectFixture })
    #expect(await first.loadCache() == nil)
    let fresh = try await first.refresh()
    #expect(fresh[.recent].map(\.project) == ["alpha", "beta", "zeta", "undated"])
    #expect(fresh[.conversations].map(\.project) == ["beta", "zeta", "alpha", "undated"])
    #expect(fresh[.name].map(\.project) == ["alpha", "beta", "undated", "zeta"])
    #expect(fresh[.conversations][0].sessionCount == 300)
    #expect(fresh.cacheWarning == nil)
    await first.setSort(.conversations)
    let restarted = ProjectCatalog(client: client, cacheDirectory: directory, fetch: { throw ClientError(message: "offline") })
    let cached = try #require(await restarted.loadCache())
    #expect(cached.sort == .conversations)
    #expect(cached[.conversations] == fresh[.conversations])
    #expect(cached.updatedAt == fresh.updatedAt)
    let selectedEarly = ProjectCatalog(client: client, cacheDirectory: directory, fetch: { projectFixture })
    await selectedEarly.setSort(.name)
    #expect(await selectedEarly.loadCache()?.sort == .name)
    do { _ = try await restarted.refresh(); Issue.record("Expected refresh failure") } catch {}
    #expect(await restarted.loadCache()?.updatedAt == fresh.updatedAt)
    let otherRoot = ProjectCatalog(client: MemexClient(root: "/fixture-b"), cacheDirectory: directory)
    #expect(await otherRoot.loadCache() == nil)
    let files = try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)
    try Data("invalid json".utf8).write(to: #require(files.first))
    #expect(await restarted.loadCache() == nil)
}

@MainActor @Test func cachedProjectsPublishBeforeRefreshAndKeepWorkingOnFailure() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let client = MemexClient(root: "/fixture-startup")
    let primer = ProjectCatalog(client: client, cacheDirectory: directory, fetch: { projectFixture })
    _ = try await primer.refresh()
    let gate = ProjectFetchGate()
    let catalog = ProjectCatalog(client: client, cacheDirectory: directory, fetch: {
        #expect(!isMainThread())
        return try await gate.fetch()
    })
    let store = Store(client: client, projectCatalog: catalog)
    store.setProjectSort(.name)
    let startup = Task { await store.loadProjects() }
    await gate.waitUntilStarted()
    #expect(store.loadingProjects)
    #expect(store.projects.map(\.project) == ["alpha", "beta", "undated", "zeta"])
    // The main actor can service a user action while the subprocess is pending.
    store.setProjectSort(.conversations)
    #expect(store.projects.first?.sessionCount == 300)
    await store.loadProjects()
    #expect(await gate.requestCount == 1)
    await gate.finish(.failure(ClientError(message: "fixture refresh failed")))
    await startup.value
    #expect(!store.loadingProjects)
    #expect(store.projects.count == 4)
    #expect(store.projectsError == "fixture refresh failed")
    // Session pagination and filtering cannot replace or truncate this catalog.
    store.catalog = [Session(source: "codex", sessionID: "s", sourcePath: "/s", project: "unrelated")]
    #expect(store.projects.count == 4)
}

@MainActor @Test func refreshReplacesCachedProjectsAndPreservesCurrentSort() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let gate = ProjectFetchGate()
    let catalog = ProjectCatalog(client: MemexClient(root: "/fixture-new"), cacheDirectory: directory,
                                 fetch: { try await gate.fetch() })
    let store = Store(projectCatalog: catalog)
    let startup = Task { await store.loadProjects() }
    await gate.waitUntilStarted()
    store.setProjectSort(.name)
    await gate.finish(.success(projectFixture))
    await startup.value
    #expect(store.projectSort == .name)
    #expect(store.projects.map(\.project) == ["alpha", "beta", "undated", "zeta"])
    #expect(store.projectsError == nil)
    #expect(store.projectsUpdatedAt != nil)
}

@Test func projectClientRequestsAggregateJSONAndDecodesCounts() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let executable = directory.appendingPathComponent("fixture-cli")
    let script = #"""
    #!/bin/sh
    [ "$*" = '--no-update-check --non-interactive projects --format json --machine local --root /fixture' ] || exit 3
    printf '[{"project":"memex","session_count":1234,"last_at":null}]'
    """#
    try script.write(to: executable, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
    let projects = try await MemexClient(executable: executable, root: "/fixture").projects()
    #expect(projects == [ProjectSummary(project: "memex", sessionCount: 1234, lastAt: nil)])
}

private actor ProjectFetchGate {
    private(set) var requestCount = 0
    private var started: CheckedContinuation<Void, Never>?
    private var pending: CheckedContinuation<[ProjectSummary], any Error>?
    func fetch() async throws -> [ProjectSummary] {
        requestCount += 1
        return try await withCheckedThrowingContinuation { continuation in
            pending = continuation
            started?.resume(); started = nil
        }
    }
    func waitUntilStarted() async {
        if requestCount > 0 { return }
        await withCheckedContinuation { started = $0 }
    }
    func finish(_ result: Result<[ProjectSummary], any Error>) {
        pending?.resume(with: result); pending = nil
    }
}

private func isMainThread() -> Bool { Thread.isMainThread }

@MainActor @Test func fullProjectSidebarScrollsAndSortsInAHiddenWindow() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let rows = (0..<1_603).map {
        ProjectSummary(project: "Project \($0)", sessionCount: $0 + 1, lastAt: nil)
    }
    let client = MemexClient(executable: URL(fileURLWithPath: "/usr/bin/true"), root: "/fixture-sidebar")
    let catalog = ProjectCatalog(client: client, cacheDirectory: directory, fetch: { rows })
    let store = Store(client: client, projectCatalog: catalog)
    await store.loadProjects()
    _ = NSApplication.shared
    let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 1100, height: 700),
        styleMask: [.titled, .resizable], backing: .buffered, defer: false)
    window.isReleasedWhenClosed = false
    let host = NSHostingView(rootView: BrowserView(store: store))
    window.contentView = host
    defer { window.close() }
    let heartbeat = ProjectHeartbeat()
    let timer = Timer.scheduledTimer(withTimeInterval: 0.01, repeats: true) { _ in
        MainActor.assumeIsolated { heartbeat.count += 1 }
    }
    defer { timer.invalidate() }
    func pump() {
        host.layoutSubtreeIfNeeded()
        window.displayIfNeeded()
        let deadline = Date().addingTimeInterval(0.1)
        while Date() < deadline { RunLoop.main.run(mode: .default, before: deadline) }
    }
    pump()
    func tables(_ view: NSView) -> [NSTableView] {
        ((view as? NSTableView).map { [$0] } ?? []) + view.subviews.flatMap(tables)
    }
    let sidebar = try #require(tables(host).first { $0.numberOfRows > 1_600 })
    let scroll = try #require(sidebar.enclosingScrollView)
    let scrollFrame = scroll.convert(scroll.bounds, to: host)
    let footerSpace = host.isFlipped ? host.bounds.maxY - scrollFrame.maxY : scrollFrame.minY - host.bounds.minY
    #expect(footerSpace >= 30)
    sidebar.scrollRowToVisible(sidebar.numberOfRows - 1)
    pump()
    store.setProjectSort(.conversations)
    pump()
    #expect(store.projects.first?.sessionCount == 1_603)
    sidebar.scrollRowToVisible(0)
    pump()
    #expect(heartbeat.count >= 5)
    #expect(!window.isVisible)
}

@MainActor private final class ProjectHeartbeat { var count = 0 }

@Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_LIVE_TESTS"] == "1"))
func liveProjectCatalogMatchesRepositorySessions() async throws {
    let client = MemexClient()
    let projects = try await client.projects()
    let project = try #require(projects.first)
    let sessions = try await client.sessions(limit: 5, project: project.project)
    #expect(sessions.count == min(5, project.sessionCount))
    #expect(sessions.allSatisfy { $0.projectName == project.project })
}

@Test func machineProjectCachesAggregateWithoutMixingScopes() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let client = MemexClient(root: "/fixture-machines")
    let catalog = ProjectCatalog(client: client, cacheDirectory: directory, machineFetch: { machine in
        [ProjectSummary(project: "shared", sessionCount: machine == "local" ? 2 : 7,
            lastAt: machine == "local" ? "2026-01-01T00:00:00Z" : "2026-02-01T00:00:00Z")]
    })
    _ = try await catalog.refresh(machine: "local")
    _ = try await catalog.refresh(machine: "nicbook-atm")
    #expect(await catalog.combinedSnapshot(machines: ["local"])?.recent.first?.sessionCount == 2)
    #expect(await catalog.combinedSnapshot(machines: ["nicbook-atm"])?.recent.first?.sessionCount == 7)
    let all = try #require(await catalog.combinedSnapshot(machines: ["local", "nicbook-atm"]))
    #expect(all.recent.first?.sessionCount == 9)
    #expect(all.recent.first?.lastAt == "2026-02-01T00:00:00Z")
    let restarted = ProjectCatalog(client: client, cacheDirectory: directory)
    #expect(await restarted.loadCaches(machines: ["nicbook-atm"])?.recent.first?.sessionCount == 7)
    #expect(await restarted.loadCaches(machines: ["local", "nicbook-atm"])?.recent.first?.sessionCount == 9)
}

@MainActor @Test func switchingMachineDoesNotWaitForOrPublishTheOldRequest() async {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let local = ProjectFetchGate()
    let remote = ProjectFetchGate()
    let catalog = ProjectCatalog(client: MemexClient(root: "/fixture-switch"), cacheDirectory: directory, machineFetch: { machine in
        try await (machine == "local" ? local : remote).fetch()
    })
    let store = Store(projectCatalog: catalog)
    store.machines = [.local, MachineChoice(id: "nicbook-atm", label: "nicbook-atm")]
    store.machineSelection = .machine("nicbook-atm")
    let old = Task { await store.loadProjects() }
    await remote.waitUntilStarted()
    store.machineSelection = .machine("local")
    let current = Task { await store.loadProjects() }
    await local.waitUntilStarted()
    await local.finish(.success([ProjectSummary(project: "local project", sessionCount: 2, lastAt: nil)]))
    await current.value
    #expect(store.projects.map(\.project) == ["local project"])
    #expect(!store.loadingProjects)
    await remote.finish(.success([ProjectSummary(project: "remote project", sessionCount: 7, lastAt: nil)]))
    await old.value
    #expect(store.projects.map(\.project) == ["local project"])
}
