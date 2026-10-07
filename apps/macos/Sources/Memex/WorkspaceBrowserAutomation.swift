import AppKit
import Foundation
import WebKit

enum WorkspaceBrowserCapability: String, Codable, CaseIterable, Hashable, Sendable {
    case snapshot, click, type, scroll, evaluate, navigate, back, forward, reload, wait, key, selectTab, record, stopRecording
}

struct WorkspaceBrowserAutomationGrant: Codable, Equatable, Sendable {
    let id: UUID
    let hostID: UUID
    let conversationID: String
    let capabilities: Set<WorkspaceBrowserCapability>
}

struct WorkspaceBrowserAutomationRequest: Codable, Sendable {
    let hostID: UUID
    let grantID: UUID
    let conversationID: String
    let tabID: UUID
    let action: WorkspaceBrowserCapability
    var durationSeconds: Int? = nil
    var framesPerSecond: Int? = nil
    var url: String? = nil
    var key: String? = nil
    var timeoutMilliseconds: Int? = nil
    var selector: String? = nil
    var text: String? = nil
    var x: Double? = nil
    var y: Double? = nil
    var script: String? = nil
    var includeScreenshot: Bool? = nil
}

struct WorkspaceBrowserCapture: Codable, Sendable {
    let tabID: UUID
    let url: String
    let title: String
    let text: String
    var pngData: Data? = nil
    var recording: WorkspaceBrowserRecordingArtifact? = nil
}

struct WorkspaceBrowserHostDescriptor: Codable, Sendable {
    struct Tab: Codable, Sendable {
        let id: UUID
        let title: String
        let url: String?
    }
    let hostID: UUID
    let conversationID: String
    let grant: WorkspaceBrowserAutomationGrant
    let selectedTabID: UUID
    let tabs: [Tab]
}

struct WorkspaceBrowserAutomationError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}

/// An authenticated transport supplies requests to this registered desktop
/// host. A request cannot enumerate or control WebViews outside these groups.
/// Grants are explicit UI decisions, scoped to a chat, and never persisted.
@MainActor final class WorkspaceBrowserAutomationHost {
    let id = UUID()
    private var recordings: [String: [UUID: WorkspaceBrowserRecording]] = [:]
    private var groups: [String: WorkspaceBrowserTabs] = [:]

    func register(_ group: WorkspaceBrowserTabs) { groups[group.conversationID] = group }
    func unregister(conversationID: String) {
        revoke(conversationID: conversationID)
        groups.removeValue(forKey: conversationID)
    }

    @discardableResult func allow(conversationID: String, capabilities: Set<WorkspaceBrowserCapability>) throws -> WorkspaceBrowserAutomationGrant {
        guard let group = groups[conversationID] else { throw failure("This conversation has no registered browser host.") }
        revoke(conversationID: conversationID)
        let grant = WorkspaceBrowserAutomationGrant(id: UUID(), hostID: id, conversationID: conversationID, capabilities: capabilities)
        group.automationGrant = grant
        return grant
    }

    func revoke(conversationID: String) {
        groups[conversationID]?.automationGrant = nil
        recordings.removeValue(forKey: conversationID)?.values.forEach { $0.cancel() }
    }

    /// Call after the transport authenticates and authorizes the requesting
    /// conversation. Ungranted conversations disclose no tab metadata.
    func descriptor(conversationID: String) -> WorkspaceBrowserHostDescriptor? {
        guard let group = groups[conversationID], let grant = group.automationGrant else { return nil }
        return .init(hostID: id, conversationID: conversationID, grant: grant, selectedTabID: group.selectedID,
                     tabs: group.sessions.map { .init(id: $0.id, title: $0.title, url: $0.currentURL?.absoluteString) })
    }

    func dispatch(_ request: WorkspaceBrowserAutomationRequest) async throws -> WorkspaceBrowserCapture {
        guard request.hostID == id, let group = groups[request.conversationID],
              let grant = group.automationGrant, grant.id == request.grantID,
              grant.capabilities.contains(request.action) else {
            throw failure("Browser access is not granted for this host, conversation, and action.")
        }
        guard let session = group.sessions.first(where: { $0.id == request.tabID }) else {
            throw failure("The requested app-owned tab is no longer registered.")
        }
        guard request.action == .navigate || request.action == .selectTab || request.action == .stopRecording || (session.currentURL.map(WorkspaceBrowserAddress.allowsNavigation(to:)) ?? false) else {
            throw failure("Open an HTTP or HTTPS page in the app browser first.")
        }
        var result: String
        var recordingArtifact: WorkspaceBrowserRecordingArtifact?
        var delivered = false
        defer {
            if !delivered, let recordingArtifact { try? FileManager.default.removeItem(atPath: recordingArtifact.path) }
        }
        switch request.action {
        case .record:
            guard recordings[request.conversationID]?[session.id] == nil else { throw failure("This tab already has a recording in progress.") }
            let recording = WorkspaceBrowserRecording()
            recordings[request.conversationID, default: [:]][session.id] = recording
            defer { recordings[request.conversationID]?[session.id] = nil }
            recordingArtifact = try await recording.record(session: session, duration: request.durationSeconds ?? 3,
                fps: request.framesPerSecond ?? 3) {
                    group.automationGrant?.id == request.grantID && group.sessions.contains(where: { $0 === session })
                }
            result = "Recorded this app-owned viewport to a temporary MP4 on the desktop host."
        case .stopRecording:
            recordings[request.conversationID]?[session.id]?.cancel()
            result = "Recording cancelled; partial artifact removed."
        case .navigate:
            guard let address = request.url, let url = WorkspaceBrowserAddress.url(from: address), session.navigate(to: url) else {
                throw failure("Provide an HTTP or HTTPS destination without credentials.")
            }
            result = "Navigation requested; use wait before reading the destination."
        case .back:
            guard session.canGoBack else { throw failure("No previous page.") }
            session.goBack(); result = "Back navigation requested."
        case .forward:
            guard session.canGoForward else { throw failure("No next page.") }
            session.goForward(); result = "Forward navigation requested."
        case .reload:
            session.reload(); result = "Reload requested."
        case .selectTab:
            group.selectedID = session.id; result = "Selected tab."
        case .wait:
            let milliseconds = request.timeoutMilliseconds ?? 5000
            guard (1...10000).contains(milliseconds) else { throw failure("Wait timeout must be between 1 and 10000 milliseconds.") }
            let deadline = Date().addingTimeInterval(Double(milliseconds) / 1000)
            while true {
                try Task.checkCancellation()
                guard group.automationGrant?.id == request.grantID, group.sessions.contains(where: { $0 === session }) else {
                    throw failure("Browser access was revoked.")
                }
                let ready = try await session.runBrowserScript("return document.readyState === 'complete' && (!selector || !!document.querySelector(selector)) ? 'ready' : 'waiting';", arguments: ["selector": request.selector ?? ""])
                if !session.isLoading && ready == "ready" { break }
                guard Date() < deadline else { throw failure("Timed out waiting for the page or selector.") }
                try await Task.sleep(for: .milliseconds(50))
            }
            result = "Page and requested selector are ready."
        case .key:
            guard let key = request.key, ["Enter", "Escape", "Tab", "ArrowUp", "ArrowDown", "ArrowLeft", "ArrowRight", "Backspace", "Delete", "Home", "End"].contains(key) else {
                throw failure("Unsupported DOM key. Supported: Enter, Escape, Tab, arrows, Backspace, Delete, Home, End.")
            }
            result = try await session.runBrowserScript("""
                const e = selector ? document.querySelector(selector) : document.activeElement;
                if (!e) throw new Error('Element not found'); e.focus();
                e.dispatchEvent(new KeyboardEvent('keydown', {key,bubbles:true,cancelable:true}));
                e.dispatchEvent(new KeyboardEvent('keyup', {key,bubbles:true,cancelable:true}));
                return 'Dispatched DOM key events; untrusted events do not invoke native browser shortcuts or default editing.';
                """, arguments: ["selector": request.selector ?? "", "key": key])
        case .snapshot:
            result = try await session.pageContext(selector: request.selector)
        case .click:
            result = try await session.runBrowserScript("const e = document.querySelector(selector); if (!e) throw new Error('Element not found'); e.scrollIntoView({block:'center'}); e.click(); return 'Clicked ' + selector;", arguments: ["selector": try selector(request)])
        case .type:
            guard let text = request.text, text.utf8.count <= 65_536 else { throw failure("Provide text of at most 64 KiB.") }
            result = try await session.runBrowserScript("""
                const e = document.querySelector(selector); if (!e) throw new Error('Element not found');
                if (e.disabled || e.readOnly) throw new Error('Element is not editable');
                e.focus();
                if (e instanceof HTMLInputElement || e instanceof HTMLTextAreaElement) {
                  const proto = e instanceof HTMLInputElement ? HTMLInputElement.prototype : HTMLTextAreaElement.prototype;
                  Object.getOwnPropertyDescriptor(proto, 'value').set.call(e, text);
                } else if (e.isContentEditable) { e.textContent = text; }
                else { throw new Error('Element is not an editable input'); }
                e.dispatchEvent(new Event('input', {bubbles:true})); e.dispatchEvent(new Event('change', {bubbles:true}));
                return 'Typed into ' + selector;
                """, arguments: ["selector": try selector(request), "text": text])
        case .scroll:
            let x = request.x ?? 0, y = request.y ?? 0
            guard x.isFinite, y.isFinite, abs(x) <= 100_000, abs(y) <= 100_000 else { throw failure("Scroll distances must be finite and at most 100,000 pixels.") }
            result = try await session.runBrowserScript("window.scrollBy(x,y); return JSON.stringify({x:scrollX,y:scrollY});", arguments: ["x": x, "y": y])
        case .evaluate:
            guard let script = request.script, script.utf8.count <= 65_536 else { throw failure("Provide a script of at most 64 KiB.") }
            result = try await session.runBrowserScript(script)
        }
        // Revocation or tab closure while WebKit was evaluating also prevents
        // returning captured data to the previously authorized caller.
        guard group.automationGrant?.id == request.grantID,
              group.sessions.contains(where: { $0 === session }) else { throw failure("Browser access was revoked.") }
        result = String(result.prefix(131_072))
        let png = request.includeScreenshot == true ? try await session.screenshot() : nil
        guard group.automationGrant?.id == request.grantID, group.sessions.contains(where: { $0 === session }) else { throw failure("Browser access was revoked.") }
        delivered = true
        return WorkspaceBrowserCapture(tabID: session.id, url: session.currentURL?.absoluteString ?? "",
                                       title: session.title, text: result, pngData: png, recording: recordingArtifact)
    }

    private func selector(_ request: WorkspaceBrowserAutomationRequest) throws -> String {
        guard let selector = request.selector, !selector.isEmpty, selector.utf8.count <= 4096 else {
            throw failure("Provide a CSS selector of at most 4 KiB.")
        }
        return selector
    }
    private func failure(_ message: String) -> WorkspaceBrowserAutomationError { .init(message: message) }
}

extension WorkspaceBrowserSession {
    func runBrowserScript(_ script: String, arguments: [String: Any] = [:]) async throws -> String {
        try await withCheckedThrowingContinuation { continuation in
            let reply = WorkspaceBrowserScriptReply(continuation)
            // Page-world evaluation is intentional and separately capability
            // gated for agents. There are no privileged native JS handlers.
            webView.callAsyncJavaScript(script, arguments: arguments, in: nil, in: .page) { result in
                switch result {
                case .success(let value):
                    if let value = value as? String { reply.finish(.success(value)) }
                    else if JSONSerialization.isValidJSONObject(value),
                            let data = try? JSONSerialization.data(withJSONObject: value, options: [.sortedKeys]) {
                        reply.finish(.success(String(decoding: data, as: UTF8.self)))
                    } else { reply.finish(.success(value is NSNull ? "" : String(describing: value))) }
                case .failure(let error): reply.finish(.failure(error))
                }
            }
        }
    }

    func pageContext(selector: String? = nil) async throws -> String {
        try await runBrowserScript("""
            const element = selector ? document.querySelector(selector) : document.body;
            if (!element) throw new Error('Element not found');
            const selection = window.getSelection()?.toString() || '';
            return JSON.stringify({url:location.href,title:document.title,selector,selection:selection.slice(0,16384),
              text:(element.innerText || element.textContent || '').slice(0,65536),
              element:selector ? element.outerHTML.slice(0,16384) : null});
            """, arguments: ["selector": selector ?? ""])
    }

    func screenshot() async throws -> Data {
        guard webView.bounds.width > 0, webView.bounds.height > 0 else {
            throw WorkspaceBrowserAutomationError(message: "Show this browser tab before capturing its viewport.")
        }
        let configuration = WKSnapshotConfiguration()
        configuration.snapshotWidth = NSNumber(value: Double(min(1600, webView.bounds.width)))
        return try await withCheckedThrowingContinuation { continuation in
            webView.takeSnapshot(with: configuration) { image, error in
                if let error { continuation.resume(throwing: error); return }
                guard let data = image?.tiffRepresentation,
                      let bitmap = NSBitmapImageRep(data: data), let png = bitmap.representation(using: .png, properties: [:]) else {
                    continuation.resume(throwing: WorkspaceBrowserAutomationError(message: "WebKit could not capture this viewport.")); return
                }
                continuation.resume(returning: png)
            }
        }
    }
}

@MainActor private final class WorkspaceBrowserScriptReply {
    private var continuation: CheckedContinuation<String, any Error>?
    private var timeout: Task<Void, Never>?

    init(_ continuation: CheckedContinuation<String, any Error>) {
        self.continuation = continuation
        timeout = Task { @MainActor [weak self] in
            do { try await Task.sleep(for: .seconds(15)) } catch { return }
            self?.finish(.failure(WorkspaceBrowserAutomationError(message: "The page did not finish this browser operation within 15 seconds. Its outcome may be unknown; inspect the page before retrying.")))
        }
    }

    func finish(_ result: Result<String, any Error>) {
        guard let continuation else { return }
        self.continuation = nil
        timeout?.cancel()
        timeout = nil
        continuation.resume(with: result)
    }
}
