import Foundation
import Testing
@testable import Memex

@Test func conversationRecoveryDistinguishesAuthenticationArchiveAndUncertainDelivery() {
    let session = Session(source: "codex", sessionID: "native", sourcePath: "/provider/sessions/native.jsonl",
                          project: "test", cwd: "/workspace", machine: "local")
    #expect(ConversationRecoveryKind.resolve(session: session, error: "OAuth token expired", deliveryUncertain: true) == .signIn)
    #expect(ConversationRecoveryKind.resolve(session: session, error: "Acknowledgement lost", deliveryUncertain: true) == .uncertain)
    #expect(ConversationRecoveryKind.resolve(session: session, error: "Process exited", deliveryUncertain: false) == .reconnect)
    let archived = Session(source: "codex", sessionID: "native", sourcePath: "/provider/archived_sessions/native.jsonl",
                           project: "test", cwd: "/workspace", machine: "local")
    #expect(InAppResumeTarget.unavailableReason(for: archived)?.contains("Unarchive") == true)
    #expect(ConversationRecoveryKind.resolve(session: archived, error: nil, deliveryUncertain: false) == .archived)
}

@MainActor @Test func signInUsesTheOriginalProviderInstallation() {
    for provider in ["codex", "claude"] {
        let session = Session(source: provider, sessionID: "native", sourcePath: "/provider/sessions/native.jsonl", project: "test")
        var target = InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
            workingDirectory: URL(fileURLWithPath: "/workspace"), providerHome: URL(fileURLWithPath: "/custom provider's home"),
            executableURL: URL(fileURLWithPath: "/custom bin/\(provider)"), helperURL: nil, storageURL: URL(fileURLWithPath: "/runtime"))
        if provider == "claude" {
            target.claudeNativeConfiguration = .resolve(providerHome: target.providerHome,
                environment: ["HOME": "/native user"])
        }
        let command = ConversationRecoveryView.signInCommand(target)
        let variable = provider == "codex" ? "CODEX_HOME" : "CLAUDE_CONFIG_DIR"
        let home = provider == "claude" ? " HOME='/native user'" : ""
        let arguments = provider == "codex" ? "login" : "auth login"
        #expect(command == "\(variable)='/custom provider'\\''s home'\(home) '/custom bin/\(provider)' \(arguments)")
    }
}

@MainActor @Test func signInPreservesClaudeDefaultAndExplicitHomeDistinction() {
    let session = Session(source: "claude", sessionID: "native", sourcePath: "/native user/.claude/projects/work/native.jsonl", project: "test")
    for explicit in [false, true] {
        var target = InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
            workingDirectory: URL(fileURLWithPath: "/workspace"), providerHome: URL(fileURLWithPath: "/native user/.claude"),
            executableURL: URL(fileURLWithPath: "/bin/claude"), helperURL: nil, storageURL: URL(fileURLWithPath: "/runtime"))
        target.claudeNativeConfiguration = .resolve(providerHome: target.providerHome, environment: [
            "HOME": "/native user", "CLAUDE_CONFIG_DIR": explicit ? "/native user/.claude" : "/another/installation"
        ])
        let prefix = explicit ? "CLAUDE_CONFIG_DIR='/native user/.claude'" : "/usr/bin/env -u CLAUDE_CONFIG_DIR"
        #expect(ConversationRecoveryView.signInCommand(target)
            == "\(prefix) HOME='/native user' '/bin/claude' auth login")
    }
}
