import AppKit
import SwiftUI

struct WorkspaceTerminalSurface: NSViewRepresentable {
    let session: WorkspaceTerminalSession
    let isActive: Bool
    let focusRequest: Int
    var wantsFocus = true

    func makeNSView(context: Context) -> WorkspaceTerminalHost {
        let host = WorkspaceTerminalHost()
        host.update(session: session, isActive: isActive, focusRequest: focusRequest, wantsFocus: wantsFocus)
        return host
    }

    func updateNSView(_ host: WorkspaceTerminalHost, context: Context) {
        host.update(session: session, isActive: isActive, focusRequest: focusRequest, wantsFocus: wantsFocus)
    }

    static func dismantleNSView(_ host: WorkspaceTerminalHost, coordinator: ()) { host.detach() }
}

@MainActor
final class WorkspaceTerminalHost: NSView {
    private var session: WorkspaceTerminalSession?
    private var isActive = false
    private var focusRequest: Int?
    private var pendingFocus = false
    private var wantsFocus = true

    func update(session: WorkspaceTerminalSession, isActive: Bool, focusRequest: Int, wantsFocus: Bool = true) {
        if self.session !== session {
            detach()
            self.session = session
            self.focusRequest = nil
        }
        self.isActive = isActive
        if wantsFocus && !self.wantsFocus { pendingFocus = isActive }
        self.wantsFocus = wantsFocus
        if self.focusRequest != focusRequest {
            pendingFocus = isActive && wantsFocus
            self.focusRequest = focusRequest
        }
        refresh()
    }

    func refresh() {
        guard let session else { return }
        if !isActive {
            if session.host === self { hideTerminal() }
            return
        }
        guard window != nil, bounds.width > 0, bounds.height > 0 else { return }
        if let previous = session.host, previous !== self { previous.detach() }
        session.host = self
        guard let terminal = session.prepareView() else { return }
        if terminal.superview !== self {
            terminal.removeFromSuperview()
            terminal.frame = bounds
            terminal.autoresizingMask = [.width, .height]
            addSubview(terminal)
            pendingFocus = wantsFocus
        }
        terminal.isHidden = false
        terminal.setSurfaceVisible(true)
        session.checkStartup()
        if pendingFocus {
            pendingFocus = false
            DispatchQueue.main.async { [weak self, weak terminal] in
                guard let self, self.isActive, self.session?.host === self, let terminal,
                      terminal.window != nil else { return }
                terminal.acquireProgrammaticFocus()
            }
        }
    }

    private func hideTerminal() {
        guard let terminal = session?.terminalView, terminal.superview === self else { return }
        terminal.setSurfaceVisible(false)
        if terminal.window?.firstResponder === terminal { terminal.window?.makeFirstResponder(nil) }
        terminal.isHidden = true
    }

    func detach() {
        if session?.host === self {
            hideTerminal()
            session?.terminalView?.removeFromSuperview()
            session?.host = nil
        }
        session = nil
    }

    override func viewDidMoveToWindow() {
        super.viewDidMoveToWindow()
        if window == nil { hideTerminal() }
        else { refresh() }
    }

    override func layout() {
        super.layout()
        refresh()
    }
}
