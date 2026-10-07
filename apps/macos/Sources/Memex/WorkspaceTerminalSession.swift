import AppKit
import Darwin
import GhosttyTerminal
import Observation

@MainActor @Observable
final class WorkspaceTerminalStore {
    private(set) var sessions: [URL: WorkspaceTerminalSession] = [:]
    private(set) var workspaces: [URL: WorkspaceTerminalGroup] = [:]
    private var isShutdown = false

    func session(for directory: URL) async throws -> WorkspaceTerminalSession {
        try await group(for: directory).selected
    }

    func group(for directory: URL) async throws -> WorkspaceTerminalGroup {
        let local = try Self.validate(directory)
        let root = try await WorkspaceChangesClient().worktreeRoot(directory: local) ?? local
        guard !isShutdown else { throw WorkspaceChangesError(message: "Terminals have shut down.") }
        _ = try Self.validate(root)
        if let group = workspaces[root] { return group }
        let session = WorkspaceTerminalSession(directory: root)
        let group = WorkspaceTerminalGroup(directory: root, initial: session)
        sessions[root] = group.selected
        workspaces[root] = group
        return group
    }

    var needsCloseConfirmation: Bool { workspaces.values.contains { $0.sessions.contains { $0.needsCloseConfirmation } } }

    func shutdown() {
        isShutdown = true
        for group in workspaces.values {
            group.saveAllHistory()
            for session in group.sessions { session.close() }
        }
        sessions.removeAll()
        workspaces.removeAll()
    }

    static func validate(_ directory: URL) throws -> URL {
        guard directory.isFileURL else {
            throw WorkspaceChangesError(message: "A terminal requires a local directory.")
        }
        let canonical = directory.resolvingSymlinksInPath().standardizedFileURL
        let values = try canonical.resourceValues(forKeys: [.isDirectoryKey])
        guard values.isDirectory == true, Darwin.access(canonical.path, R_OK | X_OK) == 0 else {
            throw WorkspaceChangesError(message: "The terminal directory is not accessible: \(canonical.path)")
        }
        return canonical
    }
}

/// The session owns the native view, not its SwiftUI placement. Moving or
/// hiding a host never changes the view's configuration or frees its surface.
@MainActor @Observable
final class WorkspaceTerminalSession: TerminalSurfaceTitleDelegate, TerminalSurfaceCloseDelegate,
    TerminalSurfaceLifecycleDelegate, TerminalSurfaceClipboardConfirmationDelegate {
    let id: UUID
    let directory: URL
    private(set) var title: String
    private(set) var isExited = false
    private(set) var isStarted = false
    private(set) var error: String?
    private(set) var capturedHistory = ""
    var closeRequested = false
    @ObservationIgnored private(set) var terminalView: AppTerminalView?
    @ObservationIgnored private(set) var surface: TerminalSurface?
    @ObservationIgnored private var controller: TerminalController?
    @ObservationIgnored private var tickTimer: Timer?
    @ObservationIgnored weak var host: WorkspaceTerminalHost?
    @ObservationIgnored var didFocus: (() -> Void)?

    init(directory: URL, id: UUID = UUID(), restoredHistory: String? = nil) {
        self.id = id
        self.directory = directory
        title = directory.lastPathComponent
        if let restoredHistory {
            capturedHistory = restoredHistory
            isExited = true
        }
    }

    // The public engine wrapper does not expose Ghostty's close-confirmation
    // query. Conservatively protect every live shell, including background jobs.
    var needsCloseConfirmation: Bool { isStarted && !isExited }

    func prepareView() -> AppTerminalView? {
        if let terminalView { return terminalView }
        guard !isExited else { return nil }
        do { _ = try WorkspaceTerminalStore.validate(directory) }
        catch { self.error = error.localizedDescription; return nil }
        let controller = TerminalController()
        let view = WorkspaceAppTerminalView(frame: NSRect(x: 0, y: 0, width: 640, height: 320))
        view.didFocus = { [weak self] in self?.didFocus?() }
        view.delegate = self
        view.configuration = TerminalSurfaceOptions(workingDirectory: directory.path, waitAfterCommand: false)
        view.setSurfaceVisible(false)
        view.controller = controller
        self.controller = controller
        terminalView = view
        // The package suspends app mailbox ticks for detached views. Keep
        // draining child-exit/title/output notifications without drawing them.
        tickTimer = Timer.scheduledTimer(withTimeInterval: 0.1, repeats: true) { [weak self] _ in
            MainActor.assumeIsolated { self?.controller?.tick() }
        }
        return view
    }

    /// Called only after user confirmation, or during approved app shutdown.
    func close() {
        _ = captureHistory()
        closeRequested = false
        isExited = true
        tickTimer?.invalidate()
        tickTimer = nil
        if let view = terminalView {
            view.setSurfaceVisible(false)
            if view.window?.firstResponder === view { view.window?.makeFirstResponder(nil) }
            // Setting controller nil explicitly frees the native surface even
            // if the retained view is detached or currently has a zero size.
            view.controller = nil
            view.removeFromSuperview()
            view.delegate = nil
        }
        surface = nil
        terminalView = nil
        controller = nil
    }

    func restart() {
        guard isExited else { return }
        close()
        isExited = false
        isStarted = false
        error = nil
        title = directory.lastPathComponent
        host?.refresh()
    }

    /// Explicit capture uses Ghostty's public selection API. It changes the
    /// selection but never synthesizes shell input or accesses the clipboard.
    @discardableResult func captureHistory() -> String {
        if let view = terminalView, view.performBindingAction("select_all"), let text = surface?.readSelection() {
            capturedHistory = String(text.suffix(262_144))
            _ = view.performBindingAction("clear_selection")
        }
        return capturedHistory
    }

    func selectedText() -> String { String((surface?.readSelection() ?? "").prefix(65_536)) }

    func clearHistory() {
        _ = terminalView?.performBindingAction("clear_screen")
        capturedHistory = ""
    }

    func terminalDidAttachSurface(_ surface: TerminalSurface) {
        self.surface = surface
        isStarted = true
        error = nil
    }

    func terminalDidDetachSurface() { surface = nil }
    func terminalDidChangeTitle(_ title: String) { self.title = title }

    func terminalDidClose(processAlive: Bool) {
        if processAlive {
            closeRequested = true
        } else {
            // Keep the final screen and scrollback available until explicit
            // restart/close. Ghostty still owns the surface, but the shell exited.
            isExited = true
            tickTimer?.invalidate()
            tickTimer = nil
        }
    }

    func checkStartup() {
        guard terminalView?.window != nil, !isExited, surface == nil else { return }
        let message = controller?.lastConfigurationIssue ?? "The terminal could not start in \(directory.path)."
        if error != message { error = message }
    }

    func terminalDidRequestClipboardConfirmation(_ request: TerminalClipboardConfirmationRequest) {
        guard let view = terminalView, let window = view.window, !view.isHiddenOrHasHiddenAncestor else {
            request.respond(allow: false)
            return
        }
        let alert = NSAlert()
        switch request.kind {
        case .paste: alert.messageText = "Paste into this terminal?"
        case .osc52Read: alert.messageText = "Allow this terminal to read the clipboard?"
        case .osc52Write: alert.messageText = "Allow this terminal to replace the clipboard?"
        }
        alert.informativeText = "The terminal requested clipboard access.\n\n" + String(request.contents.prefix(1500))
        alert.addButton(withTitle: "Cancel")
        alert.addButton(withTitle: "Allow")
        alert.beginSheetModal(for: window) { response in
            request.respond(allow: response == .alertSecondButtonReturn)
        }
    }
}

/// Keep the application's drawer shortcut available even if Ghostty adds a
/// binding for it. All other keyboard, IME and paste handling stays native.
final class WorkspaceAppTerminalView: AppTerminalView {
    var didFocus: (() -> Void)?

    override func becomeFirstResponder() -> Bool {
        let accepted = super.becomeFirstResponder()
        if accepted { didFocus?() }
        return accepted
    }

    override func performKeyEquivalent(with event: NSEvent) -> Bool {
        if event.modifierFlags.intersection(.deviceIndependentFlagsMask) == .command,
           event.charactersIgnoringModifiers?.lowercased() == "j" {
            return false
        }
        return super.performKeyEquivalent(with: event)
    }
}
