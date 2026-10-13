import SwiftUI
import WebKit

struct WorkspaceBrowserView: View {
    @ObservedObject var session: WorkspaceBrowserSession
    var isActive = true
    var live: LiveConversation?
    @FocusState private var addressFocused: Bool
    @State private var captureError: String?
    @State private var isCapturing = false
    @State private var pickingElement = false
    @State private var pickerTask: Task<Void, Never>?

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 8) {
                HStack(spacing: 0) {
                    Button(action: session.goBack) { Image(systemName: "chevron.left").frame(width: 26, height: 28) }
                        .disabled(!session.canGoBack).help("Back").accessibilityLabel("Back")
                    Button(action: session.goForward) { Image(systemName: "chevron.right").frame(width: 26, height: 28) }
                        .disabled(!session.canGoForward).help("Forward").accessibilityLabel("Forward")
                    Button {
                        if session.isLoading { session.stopLoading() } else { session.reload() }
                    } label: {
                        Image(systemName: session.isLoading ? "xmark" : "arrow.clockwise").frame(width: 26, height: 28)
                    }
                    .disabled(session.requestedURL == nil && session.currentURL == nil)
                    .help(session.isLoading ? "Stop loading" : "Reload")
                    .accessibilityLabel(session.isLoading ? "Stop loading" : "Reload")
                }
                .buttonStyle(.plain).font(.system(size: 11)).foregroundStyle(.secondary)
                .padding(.horizontal, 3).background(.quaternary.opacity(0.5), in: Capsule())
                TextField("Enter a URL", text: $session.addressText)
                    .textFieldStyle(.plain).font(.system(size: 12))
                    .padding(.horizontal, 12).frame(height: 28)
                    .background(.quaternary.opacity(0.5), in: Capsule())
                    .overlay(Capsule().strokeBorder(addressFocused ? Color.accentColor.opacity(0.6) : .clear, lineWidth: 1))
                    .focused($addressFocused)
                    .accessibilityLabel("Browser address")
                    .onSubmit {
                        if session.submitAddress() { addressFocused = false }
                    }
                Menu {
                    Button("Add page context to chat") { capture(screenshot: false) }
                    Button("Add viewport screenshot to chat") { capture(screenshot: true) }
                    Button("Pick element for chat") { pickElement() }
                    Divider()
                    Button("Zoom in") { session.zoom = min(3, session.zoom + 0.1) }
                    Button("Zoom out") { session.zoom = max(0.5, session.zoom - 0.1) }
                    Button("Actual size") { session.zoom = 1 }
                    Picker("Preview width", selection: $session.viewportWidth) {
                        Text("Fill pane").tag(nil as Double?)
                        Text("Phone · 390 px").tag(390.0 as Double?)
                        Text("Tablet · 768 px").tag(768.0 as Double?)
                        Text("Desktop · 1280 px").tag(1280.0 as Double?)
                    }
                    Divider()
                    Button("Open in default browser") {
                        if let url = session.currentURL { NSWorkspace.shared.open(url) }
                    }
                } label: { Image(systemName: "ellipsis") }
                .menuStyle(.borderlessButton).menuIndicator(.hidden).fixedSize()
                .disabled(isCapturing).help("Preview and context controls")
            }.padding(.horizontal, 8).padding(.vertical, 6)
                .background(.bar)
            if session.isLoading {
                ProgressView(value: session.estimatedProgress).progressViewStyle(.linear)
                    .accessibilityLabel("Loading page")
            } else { Divider() }
            if let error = session.error {
                HStack(alignment: .top, spacing: 8) {
                    Image(systemName: "exclamationmark.triangle").foregroundStyle(.orange)
                    Text(error).font(.caption).textSelection(.enabled)
                    Spacer(minLength: 0)
                    if session.requestedURL != nil {
                        Button("Retry", action: session.reload).controlSize(.small)
                    }
                    Button(action: session.dismissError) { Image(systemName: "xmark") }
                        .help("Dismiss error").accessibilityLabel("Dismiss browser error")
                }.padding(10).background(.quaternary)
            }
            if pickingElement {
                HStack {
                    Text("Click an element to add its context to chat.")
                    Spacer()
                    Button("Cancel") { cancelPicker() }
                }.font(.caption).padding(8).background(.bar)
            }
            if let captureError {
                Text(captureError).font(.caption).foregroundStyle(.secondary).padding(8).textSelection(.enabled)
            }
            GeometryReader { geometry in
                ScrollView(.horizontal) {
                    WorkspaceBrowserWebView(webView: session.webView)
                        .frame(width: session.viewportWidth ?? geometry.size.width, height: geometry.size.height)
                }
            }
                .id(ObjectIdentifier(session))
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .overlay {
                    if session.currentURL == nil && session.requestedURL == nil {
                        ContentUnavailableView("Browse", systemImage: "globe",
                            description: Text("Enter a website or a local preview address above."))
                            .allowsHitTesting(false)
                    }
                }
        }
        .onChange(of: isActive) { _, active in
            addressFocused = active && session.currentURL == nil && session.requestedURL == nil
            // Invisible WebKit content must not keep receiving keyboard input.
            if let window = session.webView.window {
                if active && !addressFocused { window.makeFirstResponder(session.webView) }
                else if !active, let responder = window.firstResponder as? NSView,
                        responder.isDescendant(of: session.webView) {
                    window.makeFirstResponder(nil)
                }
            }
        }
        .onChange(of: addressFocused) { _, focused in session.isEditingAddress = focused }
        .onChange(of: session) { previous, next in
            cancelPicker(in: previous)
            previous.isEditingAddress = false
            next.isEditingAddress = false
            addressFocused = false
        }
        .onDisappear { session.isEditingAddress = false; cancelPicker() }
    }

    private func capture(screenshot: Bool) {
        guard let live else { captureError = "Resume this conversation before adding browser context."; return }
        let capturedSession = session
        isCapturing = true
        captureError = nil
        Task { @MainActor in
            defer { isCapturing = false }
            do {
                let source = capturedSession.currentURL?.absoluteString ?? "App browser"
                let accepted: Bool
                if screenshot {
                    let png = try await capturedSession.screenshot()
                    accepted = live.appendImageContext(title: capturedSession.title, pngData: png, source: source)
                } else {
                    let text = try await capturedSession.pageContext()
                    accepted = live.appendContext(title: capturedSession.title, text: text, source: source)
                }
                if !accepted { captureError = "The composer could not accept this browser context." }
            } catch { captureError = error.localizedDescription }
        }
    }

    private func pickElement() {
        guard let live else { captureError = "Resume this conversation before adding browser context."; return }
        let capturedSession = session
        pickingElement = true
        captureError = nil
        pickerTask = Task { @MainActor in
            defer { pickingElement = false; pickerTask = nil }
            do {
                _ = try await capturedSession.runBrowserScript("""
                    if (window.__memexPickHandler) document.removeEventListener('click', window.__memexPickHandler, true);
                    window.__memexPicked = null;
                    window.__memexPickHandler = e => {
                      e.preventDefault(); e.stopImmediatePropagation();
                      const el = e.target;
                      window.__memexPicked = JSON.stringify({url:location.href,title:document.title,
                        tag:el.tagName,id:el.id,text:(el.innerText || el.textContent || '').slice(0,16384),
                        html:el.outerHTML.slice(0,16384)});
                      document.removeEventListener('click', window.__memexPickHandler, true);
                    };
                    document.addEventListener('click', window.__memexPickHandler, true);
                    return '';
                    """)
                for _ in 0..<300 {
                    try await Task.sleep(for: .milliseconds(200))
                    try Task.checkCancellation()
                    let context = try await capturedSession.runBrowserScript("return window.__memexPicked || ''; ")
                    if !context.isEmpty {
                        if !live.appendContext(title: "Browser element", text: context,
                                               source: capturedSession.currentURL?.absoluteString ?? "App browser") {
                            captureError = "The composer could not accept this element context."
                        }
                        return
                    }
                }
                captureError = "Element selection timed out. Try again."
                cancelPicker(in: capturedSession)
            } catch is CancellationError { }
            catch { captureError = error.localizedDescription; cancelPicker(in: capturedSession) }
        }
    }

    private func cancelPicker(in target: WorkspaceBrowserSession? = nil) {
        pickerTask?.cancel()
        pickingElement = false
        let target = target ?? session
        Task { @MainActor in
            _ = try? await target.runBrowserScript("if (window.__memexPickHandler) document.removeEventListener('click', window.__memexPickHandler, true); delete window.__memexPicked; delete window.__memexPickHandler; return ''; ")
        }
    }
}

/// Reattaching the retained web view must never trigger navigation. A host view
/// also handles a changed session without requiring callers to set a SwiftUI id.
struct WorkspaceBrowserWebView: NSViewRepresentable {
    let webView: WKWebView
    func makeNSView(context: Context) -> WorkspaceBrowserHost {
        let host = WorkspaceBrowserHost()
        host.attach(webView)
        return host
    }
    func updateNSView(_ host: WorkspaceBrowserHost, context: Context) { host.attach(webView) }
}

@MainActor final class WorkspaceBrowserHost: NSView {
    private(set) var webView: WKWebView?

    func attach(_ next: WKWebView) {
        guard webView !== next || next.superview !== self else { return }
        if webView?.superview === self { webView?.removeFromSuperview() }
        webView = next
        next.removeFromSuperview()
        next.frame = bounds
        next.autoresizingMask = [.width, .height]
        addSubview(next)
    }
}
