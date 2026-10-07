import SwiftUI

enum ConversationRecoveryKind: Equatable {
    case archived, signIn, uncertain, reconnect

    static func resolve(session: Session, error: String?, deliveryUncertain: Bool) -> Self {
        if InAppResumeTarget.isArchived(session) { return .archived }
        let message = error?.lowercased() ?? ""
        if ["oauth", "authentication", "unauthorized", "not logged in", "sign in", "sign-in", "401"]
            .contains(where: { message.contains($0) }) { return .signIn }
        return deliveryUncertain ? .uncertain : .reconnect
    }
}

struct ConversationRecoveryView: View {
    let session: Session
    let conversation: LiveConversation?
    @State private var actionError: String?

    private var error: String? { conversation?.ownershipError ?? conversation?.error }
    private var kind: ConversationRecoveryKind {
        .resolve(session: session, error: error, deliveryUncertain: conversation?.pendingPrompt?.phase == .uncertain)
    }

    var body: some View {
        if InAppResumeTarget.isArchived(session) || error != nil {
            VStack(alignment: .leading, spacing: 6) {
                HStack(alignment: .firstTextBaseline, spacing: 12) {
                    Text(message).font(.caption).textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    if kind == .archived, ChatGPTResume.url(for: session) != nil {
                        Button("Open in Codex") { Task { await openArchived() } }
                    } else if let conversation {
                        if kind == .signIn {
                            Button("Sign in…") { Task { await signIn(conversation) } }
                        }
                        Button(kind == .uncertain ? "Reload and review" : "Retry connection") {
                            Task { await conversation.connect() }
                        }.disabled(conversation.connecting)
                    }
                }
                if let actionError { Text(actionError).font(.caption).foregroundStyle(.orange).textSelection(.enabled) }
                if let error, kind == .signIn {
                    DisclosureGroup("Details") { Text(error).font(.caption).textSelection(.enabled) }
                        .font(.caption)
                }
            }
            .buttonStyle(.borderless).foregroundStyle(.secondary)
            .frame(maxWidth: ConversationReadingLane.maximumWidth, alignment: .leading)
            .padding(.horizontal, ConversationReadingLane.minimumMargin).padding(.vertical, 8)
            .frame(maxWidth: .infinity)
        }
    }

    private var message: String {
        switch kind {
        case .archived: "This conversation is archived. Unarchive it in Codex, then refresh Memex to continue."
        case .signIn: "Sign in to \(session.source == "codex" ? "Codex" : "Claude Code"), then retry the connection. Your draft stays in Memex."
        case .uncertain: "The last send is unconfirmed. Reload and review the conversation before restoring the draft. It will not be sent again automatically."
        case .reconnect: error ?? "The connection needs attention."
        }
    }

    @MainActor private func openArchived() async {
        do { try await ChatGPTResume.open(session) }
        catch { actionError = error.localizedDescription }
    }

    @MainActor private func signIn(_ conversation: LiveConversation) async {
        do {
            let target = try InAppResumeTarget.resolve(session)
            await conversation.disconnect()
            var login = session
            login.resumeCommand = Self.signInCommand(target)
            try await ResumeLaunchPlan(session: login, destination: .terminal).run()
        } catch { actionError = error.localizedDescription }
    }

    static func signInCommand(_ target: InAppResumeTarget) -> String {
        let arguments = target.session.source == "codex" ? "login" : "auth login"
        // An empty Claude override is an unset contract. Passing an empty
        // variable to the CLI still changes its backup-file location.
        var overrides = target.environment
        let unsetClaudeHome = overrides["CLAUDE_CONFIG_DIR"] == ""
        if unsetClaudeHome { overrides.removeValue(forKey: "CLAUDE_CONFIG_DIR") }
        let environment = overrides.sorted { $0.key < $1.key }.map {
            "\($0.key)=\(ResumeLaunchPlan.shellQuote($0.value))"
        }.joined(separator: " ")
        let launcher = unsetClaudeHome ? "/usr/bin/env -u CLAUDE_CONFIG_DIR " : ""
        return "\(launcher)\(environment) \(ResumeLaunchPlan.shellQuote(target.executableURL.path)) \(arguments)"
    }
}
