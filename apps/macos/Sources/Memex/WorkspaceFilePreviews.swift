import AppKit
import AVKit
import PDFKit
import SwiftUI
import WebKit

struct WorkspaceMarkdownPreview: NSViewRepresentable {
    let text: String
    final class Coordinator { var text: String? }
    func makeCoordinator() -> Coordinator { Coordinator() }
    func makeNSView(context: Context) -> RichContentView { RichContentView() }
    func updateNSView(_ view: RichContentView, context: Context) {
        guard context.coordinator.text != text else { return }
        context.coordinator.text = text
        view.configure(text: text, font: .systemFont(ofSize: 14), context: .init(isLocalHost: true))
    }
    func sizeThatFits(_ proposal: ProposedViewSize, nsView: RichContentView, context: Context) -> CGSize? {
        let width = max(1, proposal.width ?? 600)
        return CGSize(width: width, height: nsView.height(for: width))
    }
}

struct WorkspaceHTMLPreview: NSViewRepresentable {
    let text: String
    final class Coordinator: NSObject, WKNavigationDelegate {
        var text: String?
        func webView(_ webView: WKWebView, decidePolicyFor navigationAction: WKNavigationAction,
                     decisionHandler: @escaping @MainActor @Sendable (WKNavigationActionPolicy) -> Void) {
            // Render-only document preview. Never execute local scripts or grant
            // a web page access to the workspace/native application.
            if navigationAction.navigationType == .linkActivated {
                if let url = navigationAction.request.url, ["https", "http"].contains(url.scheme?.lowercased() ?? "") {
                    NSWorkspace.shared.open(url)
                }
                decisionHandler(.cancel)
            } else { decisionHandler(navigationAction.request.url?.scheme == "about" ? .allow : .cancel) }
        }
    }
    func makeCoordinator() -> Coordinator { Coordinator() }
    func makeNSView(context: Context) -> WKWebView {
        let configuration = WKWebViewConfiguration()
        configuration.defaultWebpagePreferences.allowsContentJavaScript = false
        let view = WKWebView(frame: .zero, configuration: configuration)
        view.navigationDelegate = context.coordinator
        return view
    }
    func updateNSView(_ view: WKWebView, context: Context) {
        guard context.coordinator.text != text else { return }
        context.coordinator.text = text
        view.loadHTMLString(text, baseURL: nil)
    }
}

struct WorkspacePDFPreview: NSViewRepresentable {
    let url: URL
    final class Coordinator { var url: URL? }
    func makeCoordinator() -> Coordinator { Coordinator() }
    func makeNSView(context: Context) -> PDFView {
        let view = PDFView()
        view.autoScales = true
        return view
    }
    func updateNSView(_ view: PDFView, context: Context) {
        guard context.coordinator.url != url else { return }
        context.coordinator.url = url
        view.document = PDFDocument(url: url)
    }
}

struct WorkspaceMediaPreview: View {
    let url: URL
    @State private var player: AVPlayer?
    var body: some View {
        VideoPlayer(player: player)
            .task(id: url) { player?.pause(); player = AVPlayer(url: url) }
            .onDisappear { player?.pause() }
    }
}

struct WorkspaceDelimitedPreview: View {
    let text: String
    let delimiter: Character
    private var rows: [[String]] { WorkspaceDelimitedRows.parse(text, delimiter: delimiter) }
    var body: some View {
        ScrollView([.horizontal, .vertical]) {
            LazyVStack(alignment: .leading, spacing: 0) {
                ForEach(Array(rows.enumerated()), id: \.offset) { index, row in
                    HStack(alignment: .top, spacing: 0) {
                        ForEach(Array(row.prefix(30).enumerated()), id: \.offset) { _, cell in
                            Text(cell).font(index == 0 ? .system(size: 12, weight: .semibold) : .system(size: 12))
                                .textSelection(.enabled).frame(width: 160, alignment: .leading).padding(6)
                                .overlay(alignment: .trailing) { Divider() }
                        }
                    }.background(index % 2 == 0 ? Color.primary.opacity(0.04) : .clear)
                }
                Text("Preview shows up to 500 rows and 30 columns. Switch to source to view or edit the complete file.")
                    .font(.caption).foregroundStyle(.secondary).padding(8)
            }
        }
    }
}

enum WorkspaceDelimitedRows {
    /// A bounded RFC-style reader used only for display; saving always preserves
    /// the source buffer. Quoted delimiters, escaped quotes and newlines survive.
    static func parse(_ text: String, delimiter: Character, limit: Int = 500) -> [[String]] {
        guard limit > 0 else { return [] }
        var rows: [[String]] = []
        var row: [String] = []
        var cell = ""
        var quoted = false
        var index = text.startIndex
        while index < text.endIndex && rows.count < limit {
            let character = text[index]
            let next = text.index(after: index)
            if character == "\"" {
                if quoted, next < text.endIndex, text[next] == "\"" {
                    cell.append("\"")
                    index = text.index(after: next)
                    continue
                } else if quoted || cell.isEmpty { quoted.toggle() }
                else { cell.append(character) }
            } else if character == delimiter && !quoted {
                row.append(cell); cell = ""
            } else if (character == "\n" || character == "\r\n" || character == "\r") && !quoted {
                row.append(cell); rows.append(row); row = []; cell = ""
            } else { cell.append(character) }
            index = next
        }
        if rows.count < limit, !row.isEmpty || !cell.isEmpty { row.append(cell); rows.append(row) }
        return rows
    }
}
