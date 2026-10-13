import Foundation

/// A read-only checkpoint of the host's observed ACP view. It references the
/// exact wire observations retained by SQACPHost under history/acp-observations.
/// It never claims provider-native transcript provenance or resumes execution.
enum ConfiguredConversationHistory {
    private struct Checkpoint: Codable {
        let version: Int
        let provenance: String
        let sessionID: String
        let providerInstanceID: String
        let nativeSessionID: String
        let observedAt: Date
        let conversation: RawTranscriptJSON
    }

    static func save(conversation: RawTranscriptJSON, target: InAppResumeTarget) throws {
        guard target.configuredProvider != nil else { return }
        let expectedID = "memex-" + InAppResumeTarget.digest(target.session.id)
        guard conversation["session_id"].string == expectedID,
              conversation["key"]["native_session_id"].string == target.session.sessionID,
              conversation["key"]["namespace"].string == target.providerInstanceID else {
            throw ConversationRuntimeError(message: "The observed ACP history belongs to another provider session.")
        }
        let checkpoint = Checkpoint(version: 1, provenance: "memex_observed_acp_view", sessionID: target.session.id,
            providerInstanceID: target.providerInstanceID, nativeSessionID: target.session.sessionID,
            observedAt: Date(), conversation: conversation)
        try FileManager.default.createDirectory(at: target.storageURL, withIntermediateDirectories: true,
                                                attributes: [.posixPermissions: 0o700])
        let path = target.storageURL.appendingPathComponent("acp-observed-view.json")
        try JSONEncoder().encode(checkpoint).write(to: path, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: path.path)
    }

    static func records(for session: Session, applicationSupport: URL? = nil) throws -> [TranscriptRecord] {
        let manifest = try ConfiguredConversationManifest.read(for: session)
        let support = try applicationSupport ?? FileManager.default.url(for: .applicationSupportDirectory,
            in: .userDomainMask, appropriateFor: nil, create: false).appendingPathComponent("dev.memex.app/Resume")
        let path = support.appendingPathComponent(InAppResumeTarget.digest(session.id)).appendingPathComponent("acp-observed-view.json")
        guard FileManager.default.fileExists(atPath: path.path) else { return [] }
        let checkpoint = try JSONDecoder().decode(Checkpoint.self, from: Data(contentsOf: path))
        let providerInstanceID = manifest.provider.id + ":" + InAppResumeTarget.digest(URL(fileURLWithPath: manifest.provider.homePath).path)
        guard checkpoint.version == 1, checkpoint.provenance == "memex_observed_acp_view",
              checkpoint.sessionID == session.id, checkpoint.nativeSessionID == session.sessionID,
              checkpoint.providerInstanceID == providerInstanceID else {
            throw ConversationRuntimeError(message: "The saved ACP observation has invalid session identity.")
        }
        return ConversationProjection.snapshot(checkpoint.conversation, ready: false, canCancel: false).records
    }
}
