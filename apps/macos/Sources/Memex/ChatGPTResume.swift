import AppKit

// Verified against ChatGPT 26.901.51231: com.openai.codex registers `codex`,
// and its localConversation route accepts codex://threads/<conversationId>.
enum ChatGPTResume {
    static func url(for session: Session) -> URL? {
        guard session.machineID == "local", session.source == "codex",
              UUID(uuidString: session.sessionID) != nil else { return nil }
        var parts = URLComponents()
        parts.scheme = "codex"
        parts.host = "threads"
        parts.path = "/" + session.sessionID
        return parts.url
    }

    @MainActor static func open(_ session: Session) async throws {
        guard let url = url(for: session),
              let app = NSWorkspace.shared.urlForApplication(withBundleIdentifier: "com.openai.codex") else {
            throw ResumeError(message: "ChatGPT can open local Codex conversations with a valid session ID.")
        }
        _ = try await NSWorkspace.shared.open([url], withApplicationAt: app, configuration: .init())
    }
}
