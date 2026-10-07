import AppKit
import Darwin
import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct WorkspaceTerminalTests {
    @Test func worktreeRootsShareSessionsButLinkedWorktreesDoNot() async throws {
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent("memex-terminal-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: temp, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: temp) }
        let root = temp.appendingPathComponent("repo")
        let child = root.appendingPathComponent("child")
        try FileManager.default.createDirectory(at: child, withIntermediateDirectories: true)
        func git(_ arguments: [String]) throws {
            _ = try CommandRun().execute(executable: URL(fileURLWithPath: "/usr/bin/git"),
                                         arguments: ["-C", root.path] + arguments, timeout: 5)
        }
        try git(["init", "-q"])
        try git(["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-qm", "Initial"])
        let linked = temp.appendingPathComponent("linked")
        try git(["worktree", "add", "--detach", linked.path, "HEAD"])
        let alias = temp.appendingPathComponent("alias")
        try FileManager.default.createSymbolicLink(at: alias, withDestinationURL: child)
        let store = WorkspaceTerminalStore()
        defer { store.shutdown() }
        let first = try await store.session(for: root)
        #expect(try await store.session(for: child) === first)
        #expect(try await store.session(for: alias) === first)
        let second = try await store.session(for: linked)
        #expect(second !== first)
        #expect(second.directory == linked.resolvingSymlinksInPath())
        #expect(first.terminalView == nil)
        #expect(!store.needsCloseConfirmation)
        // Non-Git directories are valid, but missing and non-directory paths
        // must never fall back to the application's working directory.
        #expect(try await store.session(for: temp).directory == temp.resolvingSymlinksInPath())
        await #expect(throws: (any Error).self) { try await store.session(for: temp.appendingPathComponent("missing")) }
        let file = temp.appendingPathComponent("file")
        try Data().write(to: file)
        await #expect(throws: (any Error).self) { try await store.session(for: file) }
    }

    @Test func nativeShellKeepsProcessEnvironmentAndScrollbackAcrossHosts() async throws {
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent("memex-terminal-shell-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: temp, withIntermediateDirectories: true)
        let secondDirectory = temp.appendingPathComponent("second")
        try FileManager.default.createDirectory(at: secondDirectory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: temp) }
        let store = WorkspaceTerminalStore()
        defer { store.shutdown() }
        let session = try await store.session(for: temp)
        let other = try await store.session(for: secondDirectory)
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 350),
                              styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        defer { window.close() }
        let firstHost = WorkspaceTerminalHost(frame: window.contentLayoutRect)
        window.contentView = firstHost
        firstHost.update(session: session, isActive: true, focusRequest: 0)
        try await waitUntil { session.isStarted }
        let view = try #require(session.terminalView)
        let surface = try #require(session.surface)
        let before = temp.appendingPathComponent("before")
        #expect(view.paste(text: "export MEMEX_TERMINAL_TEST=retained; printf '%s\\n' \"$$\" \"$PWD\" \"$MEMEX_TERMINAL_TEST\" > '\(before.path)'; printf 'MEMEX_SCROLLBACK_MARKER\\n'"))
        #expect(view.sendKey(.enter))
        try await waitUntil { FileManager.default.fileExists(atPath: before.path) }
        let initial = try String(contentsOf: before, encoding: .utf8).split(separator: "\n").map(String.init)
        try #require(initial.count == 3)
        #expect(initial[1] == session.directory.path)
        #expect(initial[2] == "retained")
        #expect(session.needsCloseConfirmation)
        firstHost.detach()
        #expect(view.window == nil)
        #expect(session.surface === surface)
        let after = temp.appendingPathComponent("after")
        #expect(view.paste(text: "printf '%s\\n' \"$$\" \"$MEMEX_TERMINAL_TEST\" > '\(after.path)'"))
        #expect(view.sendKey(.enter))
        try await waitUntil { FileManager.default.fileExists(atPath: after.path) }
        let continued = try String(contentsOf: after, encoding: .utf8).split(separator: "\n").map(String.init)
        #expect(continued == [initial[0], "retained"])
        let nextHost = WorkspaceTerminalHost(frame: window.contentLayoutRect)
        window.contentView = nextHost
        nextHost.update(session: session, isActive: true, focusRequest: 1)
        #expect(session.terminalView === view)
        #expect(session.surface === surface)
        #expect(view.performBindingAction("select_all"))
        #expect(surface.readSelection()?.contains("MEMEX_SCROLLBACK_MARKER") == true)
        nextHost.detach()
        nextHost.update(session: other, isActive: true, focusRequest: 2)
        try await waitUntil { other.isStarted }
        let secondPID = temp.appendingPathComponent("second-pid")
        let otherView = try #require(other.terminalView)
        // Interactive Bash reads !; as history expansion; whitespace keeps $! literal.
        #expect(otherView.paste(text: "sleep 60 & MEMEX_BG_PID=$! ; printf '%s\\n' \"$$\" \"$MEMEX_BG_PID\" > '\(secondPID.path)'"))
        #expect(otherView.sendKey(.enter))
        try await waitUntil(diagnostics: { other.captureHistory() }) {
            FileManager.default.fileExists(atPath: secondPID.path)
        }
        let runningPIDs = try String(contentsOf: secondPID, encoding: .utf8).split(separator: "\n").map(String.init)
        let pid = try #require(runningPIDs.first)
        try #require(runningPIDs.count == 2)
        #expect(pid != initial[0])
        #expect(view.paste(text: "exit"))
        #expect(view.sendKey(.enter))
        try await waitUntil { session.isExited }
        #expect(!session.needsCloseConfirmation)
        #expect(session.surface === surface)
        nextHost.detach()
        session.restart()
        nextHost.update(session: session, isActive: true, focusRequest: 3)
        try await waitUntil { session.isStarted }
        #expect(session.terminalView !== view)
        #expect(session.surface !== surface)
        #expect(session.needsCloseConfirmation)
        store.shutdown()
        #expect(session.terminalView == nil)
        #expect(other.terminalView == nil)
        for text in runningPIDs {
            let childPID = try #require(Int32(text))
            try await waitUntil { kill(childPID, 0) != 0 && errno == ESRCH }
        }
        #expect(!window.isVisible)
    }

    @Test func independentShellsInOneWorkspaceKeepStateAndCaptureHistory() async throws {
        let temp = FileManager.default.temporaryDirectory.appendingPathComponent("memex-terminal-group-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: temp, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: temp) }
        let store = WorkspaceTerminalStore()
        defer { store.shutdown() }
        let group = try await store.group(for: temp)
        let first = group.selected
        let second = group.add()
        #expect(first !== second)
        #expect(first.directory == second.directory)
        #expect(group.selected === second)
        #expect(try await store.group(for: temp) === group)
        group.layout = .sideBySide
        #expect(group.visible.count == 2)
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 700, height: 350),
                              styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        defer { window.close() }
        let host = WorkspaceTerminalHost(frame: window.contentLayoutRect)
        window.contentView = host
        host.update(session: first, isActive: true, focusRequest: 0)
        try await waitUntil { first.isStarted }
        let firstView = try #require(first.terminalView)
        #expect(firstView.paste(text: "export MEMEX_SPLIT=first; printf 'FIRST_SHELL_CAPTURE\\n'"))
        #expect(firstView.sendKey(.enter))
        host.detach()
        host.update(session: second, isActive: true, focusRequest: 1)
        try await waitUntil { second.isStarted }
        let secondView = try #require(second.terminalView)
        let evidence = temp.appendingPathComponent("independence")
        #expect(secondView.paste(text: "printf '%s' \"${MEMEX_SPLIT-unset}\" > '\(evidence.path)'"))
        #expect(secondView.sendKey(.enter))
        try await waitUntil { FileManager.default.fileExists(atPath: evidence.path) }
        #expect(try String(contentsOf: evidence, encoding: .utf8) == "unset")
        try await waitUntil { first.captureHistory().contains("FIRST_SHELL_CAPTURE") }
        let saved = WorkspaceTerminalGroup(directory: temp, initial: first, historyDirectory: temp.appendingPathComponent("history"))
        saved.saveHistory(from: first)
        let reloaded = WorkspaceTerminalGroup(directory: temp, initial: WorkspaceTerminalSession(directory: temp),
                                             historyDirectory: temp.appendingPathComponent("history"))
        #expect(reloaded.savedHistory.contains("FIRST_SHELL_CAPTURE"))
        #expect(!reloaded.selected.isStarted)
        #expect(reloaded.selected.isExited)
        #expect(reloaded.selected.id == first.id)
        group.remove(first)
        #expect(first.isExited)
        #expect(!second.isExited)
        #expect(second.terminalView === secondView)
    }

    private func waitUntil(diagnostics: () -> String = { "" }, sourceLocation: SourceLocation = #_sourceLocation,
                           _ predicate: () -> Bool) async throws {
        let deadline = Date().addingTimeInterval(10)
        while !predicate(), Date() < deadline { try await Task.sleep(for: .milliseconds(20)) }
        #expect(predicate(), "Terminal state did not arrive before the deadline. \(diagnostics())", sourceLocation: sourceLocation)
        guard predicate() else { throw WorkspaceChangesError(message: "Terminal test timed out.") }
    }
}
