import AppKit
import Foundation
import SwiftUI
import Testing
@testable import Memex

private struct WorkspaceFixture {
    let root: URL
    init() throws {
        root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-workspace-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        try git(["init", "-q"])
        try git(["config", "user.name", "Fixture"])
        try git(["config", "user.email", "fixture@example.invalid"])
    }
    func git(_ args: [String]) throws {
        _ = try CommandRun().execute(executable: URL(fileURLWithPath: "/usr/bin/git"), arguments: ["-C", root.path] + args, timeout: 5)
    }
    func write(_ path: String, _ text: String) throws {
        try text.write(to: root.appendingPathComponent(path), atomically: true, encoding: .utf8)
    }
    func clean() { try? FileManager.default.removeItem(at: root) }
}

@MainActor @Test func workspaceTabsKeepTheSelectedDiffAndNativeReadingPosition() async throws {
    let fixture = try WorkspaceFixture()
    defer { fixture.clean() }
    try fixture.write("a.txt", "Another file\n")
    try fixture.write("b.txt", (0..<200).map { "line \($0)" }.joined(separator: "\n"))
    let session = Session(source: "codex", sessionID: "workspace-tabs", sourcePath: "/fixture/sessions/tabs.jsonl",
                          project: "fixture", cwd: fixture.root.path, machine: "local")
    let store = Store()
    store.scope = .all
    store.sessions = [session]
    store.selectedID = session.id
    store.reviewWorkspaceChange("b.txt")
    let host = NSHostingView(rootView: WorkspacePanelView(store: store))
    let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 620, height: 560),
                          styleMask: [.titled], backing: .buffered, defer: false)
    window.isReleasedWhenClosed = false
    window.contentView = host
    defer { window.close() }

    func descendants<T: NSView>(of view: NSView, as type: T.Type) -> [T] {
        ((view as? T).map { [$0] } ?? []) + view.subviews.flatMap { descendants(of: $0, as: type) }
    }
    func diffView() -> NSTextView? {
        descendants(of: host, as: NSTextView.self).first { $0.string.contains("+line 199") }
    }
    let deadline = Date().addingTimeInterval(5)
    while diffView() == nil, Date() < deadline {
        host.layoutSubtreeIfNeeded()
        try await Task.sleep(for: .milliseconds(20))
    }
    let diff = try #require(diffView())
    let scroll = try #require(diff.enclosingScrollView)
    diff.setSelectedRange(NSRange(location: 20, length: 8))
    scroll.contentView.scroll(to: NSPoint(x: 0, y: 240))
    scroll.reflectScrolledClipView(scroll.contentView)
    let origin = scroll.contentView.bounds.origin
    let selection = diff.selectedRange()
    #expect(origin.y > 0)
    let browser = store.workspaceBrowser.session(for: session.id)
    browser.addressText = "localhost:4000/unfinished"
    for panel in [Store.WorkspacePanel.tools, .browser, .tools, .changes, .browser, .changes] {
        store.workspacePanel = panel
        try await Task.sleep(for: .milliseconds(30))
        host.layoutSubtreeIfNeeded()
        #expect(diffView() === diff)
        #expect(scroll.contentView.bounds.origin == origin)
        #expect(diff.selectedRange() == selection)
        #expect(browser.addressText == "localhost:4000/unfinished")
        #expect(browser.requestedURL == nil)
    }
    // The inspector's minimum width must fit the tabs and both native panes.
    window.setContentSize(NSSize(width: 430, height: 560))
    host.layoutSubtreeIfNeeded()
    #expect(host.bounds.width == 430)
    #expect(scroll.convert(scroll.bounds, to: host).maxX <= 431)
    #expect(browser.webView.convert(browser.webView.bounds, to: host).maxX <= 431)
    #expect(!window.isVisible)
}

@Test func workspaceChangesPreserveStagedAndUnstagedEditsAndUntrackedPaths() async throws {
    let fixture = try WorkspaceFixture()
    defer { fixture.clean() }
    try fixture.write("tracked file.txt", "original\n")
    try fixture.git(["add", "."])
    try fixture.git(["commit", "-qm", "Initial"])
    try fixture.write("tracked file.txt", "staged\n")
    try fixture.git(["add", "."])
    // Undo the staged change in the worktree: a combined HEAD diff would hide both edits.
    try fixture.write("tracked file.txt", "original\n")
    try fixture.write("untracked [file]\nname.txt", "new content\n")
    let index = fixture.root.appendingPathComponent(".git/index")
    let beforeIndex = try Data(contentsOf: index)
    let client = WorkspaceChangesClient()
    let snapshot = try #require(try await client.snapshot(directory: fixture.root))
    #expect(snapshot.files.count == 2)
    let tracked = try #require(snapshot.files.first { $0.path == "tracked file.txt" })
    #expect(tracked.indexStatus == "M")
    #expect(tracked.worktreeStatus == "M")
    let patch = try await client.diff(file: tracked, root: snapshot.root)
    #expect(patch.contains("Staged changes"))
    #expect(patch.contains("Unstaged changes"))
    #expect(patch.contains("+staged"))
    #expect(patch.contains("-staged"))
    let untracked = try #require(snapshot.files.first { $0.isUntracked })
    #expect(untracked.path == "untracked [file]\nname.txt")
    let addition = try await client.diff(file: untracked, root: snapshot.root)
    #expect(addition.contains("+new content"))
    #expect(try Data(contentsOf: index) == beforeIndex)
}

@Test func workspaceChangesShowRenameDeletionBinaryAndCleanState() async throws {
    let fixture = try WorkspaceFixture()
    defer { fixture.clean() }
    try fixture.write("old name.txt", "rename this\n")
    try fixture.write("delete.txt", "delete this\n")
    try Data([0, 1, 2]).write(to: fixture.root.appendingPathComponent("binary.dat"))
    try fixture.git(["add", "."])
    try fixture.git(["commit", "-qm", "Initial"])
    let client = WorkspaceChangesClient()
    #expect(try await client.snapshot(directory: fixture.root)?.files.isEmpty == true)
    try fixture.git(["mv", "old name.txt", "new name.txt"])
    try FileManager.default.removeItem(at: fixture.root.appendingPathComponent("delete.txt"))
    try Data([0, 1, 3]).write(to: fixture.root.appendingPathComponent("binary.dat"))
    let snapshot = try #require(try await client.snapshot(directory: fixture.root))
    let rename = try #require(snapshot.files.first { $0.path == "new name.txt" })
    #expect(rename.originalPath == "old name.txt")
    #expect(try await client.diff(file: rename, root: snapshot.root).contains("rename from old name.txt"))
    let deletion = try #require(snapshot.files.first { $0.path == "delete.txt" })
    #expect(deletion.worktreeStatus == "D")
    #expect(try await client.diff(file: deletion, root: snapshot.root).contains("-delete this"))
    let binary = try #require(snapshot.files.first { $0.path == "binary.dat" })
    #expect(try await client.diff(file: binary, root: snapshot.root).contains("Binary files"))
}

@Test func workspaceChangesSupportUnbornRepositoryAndBoundLargeDiff() async throws {
    let fixture = try WorkspaceFixture()
    defer { fixture.clean() }
    try fixture.write("first.txt", "first commit pending\n")
    try fixture.git(["add", "."])
    try fixture.write("large.txt", String(repeating: "new line\n", count: 100_000))
    let client = WorkspaceChangesClient()
    let snapshot = try #require(try await client.snapshot(directory: fixture.root))
    let staged = try #require(snapshot.files.first { $0.path == "first.txt" })
    #expect(try await client.diff(file: staged, root: snapshot.root).contains("+first commit pending"))
    let large = try #require(snapshot.files.first { $0.path == "large.txt" })
    let patch = try await client.diff(file: large, root: snapshot.root)
    #expect(patch.contains("Diff truncated at 512 KB"))
    #expect(patch.utf8.count < WorkspaceChangesClient.outputLimit + 200)
}

@Test func workspaceChangesDistinguishNonRepository() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false)
    defer { try? FileManager.default.removeItem(at: directory) }
    #expect(try await WorkspaceChangesClient().snapshot(directory: directory) == nil)
}

@Test @MainActor func workspaceSummaryLoadsWhileItsInitialContentIsEmpty() async throws {
    let fixture = try WorkspaceFixture()
    defer { fixture.clean() }
    try fixture.write("new.txt", "visible workspace change\n")
    let host = NSHostingView(rootView: WorkspaceChangeSummary(directory: fixture.root, isWorking: false, review: { _ in }))
    let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 600, height: 100),
                          styleMask: [.titled], backing: .buffered, defer: false)
    window.isReleasedWhenClosed = false
    window.contentView = host
    defer { window.close() }
    for _ in 0..<200 {
        host.layoutSubtreeIfNeeded()
        if host.fittingSize.height > 12 { break }
        try await Task.sleep(for: .milliseconds(10))
    }
    #expect(host.fittingSize.height > 12)
    #expect(!window.isVisible)
}
