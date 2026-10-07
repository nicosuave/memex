import AppKit
import SwiftUI
import Testing
@testable import Memex

#if canImport(SQACPUI)
private actor PanelFixtureRuntime: ConversationRuntime {
    let snapshot: ConversationSnapshot
    let failure: String?

    init(snapshot: ConversationSnapshot = .init(connected: true, ready: true), failure: String? = nil) {
        self.snapshot = snapshot
        self.failure = failure
    }
    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) throws {
        if let failure { throw ConversationRuntimeError(message: failure) }
        receive(.success(snapshot))
    }
    func perform(_ command: ConversationCommand) throws {
        throw ConversationRuntimeError(message: "Delivery could not be confirmed")
    }
    func disconnect() {}
}

@MainActor @Test func approvalAndRecoveryPanelsFitTheReadingLane() async throws {
    let session = Session(source: "codex", sessionID: "panel-fixture", sourcePath: "/fixture/sessions/native.jsonl",
                          project: "fixture", cwd: "/fixture", machine: "local")
    let approval = ConversationApproval(id: "request", title: "Run command", detail: """
        {
          "command": ["/bin/zsh", "-lc", "cat greeting.txt"],
          "cwd": "/tmp/disposable-workspace",
          "reason": "Read the requested project file"
        }
        """, options: [.init(id: "allow", title: "Allow once", kind: "allow_once"),
                       .init(id: "deny", title: "Decline", kind: "reject_once")])
    for mode in ["approval", "sign-in", "uncertain", "open-elsewhere"] {
        let runtime = PanelFixtureRuntime(
            snapshot: .init(connected: true, ready: true, approvals: mode == "approval" ? [approval] : []),
            failure: mode == "sign-in" ? "OAuth token expired. Sign in again." : nil)
        let conversation = LiveConversation(session: session, makeRuntime: { runtime }, resolveTarget: { session in
            InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
                workingDirectory: URL(fileURLWithPath: "/fixture"), providerHome: URL(fileURLWithPath: "/fixture"),
                executableURL: URL(fileURLWithPath: "/bin/echo"), helperURL: nil,
                storageURL: URL(fileURLWithPath: "/fixture/runtime"))
        }, checkOwnership: { _ in mode == "open-elsewhere" })
        await conversation.connect()
        conversation.draft = "Keep this draft while the request needs attention."
        if mode == "uncertain" { await conversation.send() }
        for width in [480.0, 760.0] {
            let content = VStack(spacing: 0) {
                ConversationRecoveryView(session: session, conversation: conversation)
                ConversationPendingView(conversation: conversation)
                ConversationComposer(conversation: conversation)
            }.frame(width: width).fixedSize(horizontal: false, vertical: true)
                .background(Color(NSColor.windowBackgroundColor))
            let host = NSHostingView(rootView: content)
            let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: width, height: 520),
                                  styleMask: [.titled], backing: .buffered, defer: false)
            window.isReleasedWhenClosed = false
            window.contentView = host
            host.layoutSubtreeIfNeeded()
            try await Task.sleep(for: .milliseconds(30))
            let size = host.fittingSize
            #expect(size.width <= width + 1)
            #expect(size.height > 80 && size.height < 520)
            window.setContentSize(size)
            host.layoutSubtreeIfNeeded()
            if let directory = ProcessInfo.processInfo.environment["MEMEX_PANEL_SNAPSHOTS"], width == 480,
               let bitmap = host.bitmapImageRepForCachingDisplay(in: host.bounds) {
                host.cacheDisplay(in: host.bounds, to: bitmap)
                let file = URL(fileURLWithPath: directory).appendingPathComponent("\(mode).png")
                try bitmap.representation(using: .png, properties: [:])?.write(to: file)
            }
            #expect(!window.isVisible)
            window.close()
        }
        await conversation.disconnect()
    }
}

@MainActor @Test func ownedElsewhereDisablesEditorAndRestoresSavedDraftOnUnlock() async throws {
    let session = Session(source: "codex", sessionID: "locked-composer", sourcePath: "/fixture/sessions/native.jsonl",
                          project: "fixture", cwd: "/fixture", machine: "local")
    var locked = false
    let runtime = PanelFixtureRuntime()
    let conversation = LiveConversation(session: session, makeRuntime: { runtime }, checkOwnership: { _ in locked })
    conversation.draft = "My unsent draft"
    let host = NSHostingView(rootView: ConversationComposer(conversation: conversation).frame(width: 480))
    let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 480, height: 200),
                          styleMask: [.titled], backing: .buffered, defer: false)
    window.isReleasedWhenClosed = false
    window.contentView = host
    defer { window.close() }

    func editors(in view: NSView) -> [NSTextField] {
        ((view as? NSTextField).map { [$0] } ?? []) + view.subviews.flatMap { editors(in: $0) }
    }
    for isLocked in [false, true, false] {
        locked = isLocked
        conversation.refreshOwnership()
        host.layoutSubtreeIfNeeded()
        try await Task.sleep(for: .milliseconds(30))
        host.layoutSubtreeIfNeeded()
        let editor = try #require(editors(in: host).first)
        #expect((editor.isEditable && editor.isEnabled) == !isLocked)
        #expect(editor.stringValue == (isLocked ? "" : "My unsent draft"))
        #expect(conversation.draft == "My unsent draft")
    }
}
#endif
