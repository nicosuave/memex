import Foundation
import MemexExecutionHostCore

enum ConversationOwnership {
    static func isOpenElsewhere(_ session: Session) throws -> Bool {
        guard session.machineID == "local", ["codex", "claude"].contains(session.source) else { return false }
        if session.source == "codex", UUID(uuidString: session.sessionID) == nil { return false }
        let source = URL(fileURLWithPath: session.sourcePath).standardizedFileURL
        let home = session.source == "codex"
            ? try InAppResumeTarget.providerHome(for: source, provider: "codex")
            : source.deletingLastPathComponent() // Claude's descriptor probe does not use a home path.
        return try NativeConversationOwnership.isOpenElsewhere(provider: session.source,
            nativeSessionID: session.sessionID, sourceURL: source, providerHome: home)
    }

    static func hasExternalWriter(at file: URL) throws -> Bool {
        try NativeConversationOwnership.hasExternalWriter(at: file)
    }

    static func containsExternalWriter(_ output: String, excludingPID: Int32) -> Bool {
        NativeConversationOwnership.containsExternalWriter(output, excludingPID: excludingPID)
    }
}
