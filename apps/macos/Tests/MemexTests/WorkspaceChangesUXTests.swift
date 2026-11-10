import AppKit
import Foundation
import Testing
@testable import Memex

private let workspaceUXDirectory = URL(fileURLWithPath: "/tmp/memex-workspace-ux")

private func workspaceUXSnapshot(_ paths: [String], root: URL = workspaceUXDirectory) -> WorkspaceChangesSnapshot {
    WorkspaceChangesSnapshot(root: root, files: paths.map {
        WorkspaceChange(path: $0, originalPath: nil, indexStatus: " ", worktreeStatus: "M")
    })
}

@Test func workspaceChangesUXRefreshRetainsSelectionPatchAndFailures() throws {
    var state = WorkspaceChangesState()
    let initial = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: "b.swift")
    state.finishRefresh(workspaceUXSnapshot(["a.swift", "b.swift"]), request: initial)
    let patchRequest = state.beginPatch()
    let patch = try #require(patchRequest)
    state.finishPatch("original patch", request: patch.request)

    let refresh = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    #expect(state.loading)
    #expect(state.snapshot?.files.count == 2)
    #expect(state.selectedPath == "b.swift")
    #expect(state.patch == "original patch")
    state.failRefresh("Git unavailable", request: refresh)
    #expect(!state.loading)
    #expect(state.error == "Git unavailable")
    #expect(state.snapshot?.files.count == 2)
    #expect(state.patch == "original patch")

    let retry = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    state.finishRefresh(workspaceUXSnapshot(["b.swift", "c.swift"]), request: retry)
    #expect(state.error == nil)
    #expect(state.selectedPath == "b.swift")
    #expect(state.patch == "original patch")
    let failedPatchRequest = state.beginPatch()
    let failedPatch = try #require(failedPatchRequest)
    state.failPatch("File temporarily unavailable", request: failedPatch.request)
    #expect(state.patch == "original patch")
    #expect(state.patchError == "File temporarily unavailable")
    let nextPatchRequest = state.beginPatch()
    let nextPatch = try #require(nextPatchRequest)
    state.finishPatch("changed patch", request: nextPatch.request)
    #expect(state.patch == "changed patch")
    #expect(state.patchError == nil)
}

@Test func workspaceChangesUXRejectsOldWorkspaceAndFileCompletions() throws {
    var state = WorkspaceChangesState()
    let initial = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    state.finishRefresh(workspaceUXSnapshot(["a.swift", "b.swift"]), request: initial)
    let oldPatchRequest = state.beginPatch()
    let oldPatch = try #require(oldPatchRequest)
    state.select("b.swift")
    let selectedPatchRequest = state.beginPatch()
    let selectedPatch = try #require(selectedPatchRequest)
    state.finishPatch("wrong file", request: oldPatch.request)
    #expect(state.patch == nil)
    #expect(state.loadingPatch)
    state.finishPatch("b patch", request: selectedPatch.request)
    #expect(state.patch == "b patch")

    let oldRefresh = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    let pendingPatchRequest = state.beginPatch()
    let pendingPatch = try #require(pendingPatchRequest)
    let otherDirectory = URL(fileURLWithPath: "/tmp/memex-other-workspace-ux")
    let newRefresh = state.beginRefresh(directory: otherDirectory, initialSelectedPath: "b.swift")
    #expect(state.snapshot == nil)
    #expect(state.patch == nil)
    state.finishRefresh(workspaceUXSnapshot(["a.swift"]), request: oldRefresh)
    state.failRefresh("old workspace error", request: oldRefresh)
    state.finishPatch("old workspace patch", request: pendingPatch.request)
    #expect(state.snapshot == nil)
    #expect(state.patch == nil)
    #expect(state.error == nil)
    state.finishRefresh(workspaceUXSnapshot(["b.swift"], root: otherDirectory), request: newRefresh)
    #expect(state.snapshot?.root == otherDirectory)
    #expect(state.selectedPath == "b.swift")
}

@Test func workspaceChangesUXRefreshDropsRemovedSelectionAndRejectsSupersededReads() throws {
    var state = WorkspaceChangesState()
    let initial = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    state.finishRefresh(workspaceUXSnapshot(["a.swift", "b.swift"]), request: initial)
    let oldPatchRequest = state.beginPatch()
    let oldPatch = try #require(oldPatchRequest)
    let earlier = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    let latest = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    state.finishRefresh(workspaceUXSnapshot(["b.swift"]), request: latest)
    state.finishRefresh(workspaceUXSnapshot(["a.swift"]), request: earlier)
    state.failPatch("old failure", request: oldPatch.request)
    #expect(state.selectedPath == "b.swift")
    #expect(state.snapshot?.files.map(\.path) == ["b.swift"])
    #expect(state.patch == nil)
    #expect(state.patchError == nil)
    let clean = state.beginRefresh(directory: workspaceUXDirectory, initialSelectedPath: nil)
    state.finishRefresh(workspaceUXSnapshot([]), request: clean)
    #expect(state.selectedPath == nil)
    let cleanPatch = state.beginPatch()
    #expect(cleanPatch == nil)
}

@Test @MainActor func workspaceChangesUXViewportPreservesScrollSelectionAndFilePositions() throws {
    let scroll = WorkspaceDiffText.makeScrollView()
    scroll.frame = NSRect(x: 0, y: 0, width: 500, height: 300)
    let viewport = WorkspaceDiffViewport()
    let patch = (0..<200).map { "+line \($0)" }.joined(separator: "\n")
    viewport.update(scroll, text: patch, identity: "workspace/a.swift")
    let text = try #require(scroll.documentView as? NSTextView)
    text.setSelectedRange(NSRange(location: 20, length: 8))
    scroll.contentView.scroll(to: NSPoint(x: 0, y: 240))
    let originalOrigin = scroll.contentView.bounds.origin
    let originalSelection = text.selectedRange()

    viewport.update(scroll, text: patch, identity: "workspace/a.swift")
    #expect(scroll.contentView.bounds.origin == originalOrigin)
    #expect(text.selectedRange() == originalSelection)
    viewport.update(scroll, text: patch + "\n+another line", identity: "workspace/a.swift")
    #expect(scroll.contentView.bounds.origin == originalOrigin)
    #expect(text.selectedRange() == originalSelection)

    // A newly selected file has no patch until its read completes. It must not
    // display the old file, or discard that file's remembered position.
    viewport.update(scroll, text: nil, identity: "workspace/b.swift")
    #expect(text.string.isEmpty)
    viewport.update(scroll, text: patch, identity: "workspace/b.swift")
    scroll.contentView.scroll(to: NSPoint(x: 0, y: 120))
    let secondOrigin = scroll.contentView.bounds.origin
    viewport.update(scroll, text: patch, identity: "workspace/a.swift")
    #expect(scroll.contentView.bounds.origin == originalOrigin)
    #expect(text.selectedRange() == originalSelection)
    viewport.update(scroll, text: patch, identity: "workspace/b.swift")
    #expect(scroll.contentView.bounds.origin == secondOrigin)

    viewport.update(scroll, text: patch, identity: "other-workspace/b.swift")
    #expect(scroll.contentView.bounds.origin == .zero)
    #expect(text.selectedRange() == NSRange(location: 0, length: 0))
    viewport.update(scroll, text: "+short", identity: "workspace/a.swift")
    #expect(text.selectedRange().location <= text.string.utf16.count)
    #expect(scroll.contentView.bounds.origin.y == 0)
}
