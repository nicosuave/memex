import Foundation
import CryptoKit

struct HostHandoffRecord: Codable {
    var id: String
    var role: String
    var phase: String
    var sourceHostID: String
    var destinationHostID: String
    var peerPublicKey: String
    var source: HostedConversation
    var destinationWorkspaceID: String
    var digest: String?
    var destination: HostedConversation?
    var error: String?
    var recoveryID: String?
    var replacedConversation: HostedConversation?
    var supersededBy: String?
    var fencesConversation: Bool { supersededBy == nil && !["aborted", "active", "restored"].contains(phase) }
}

public struct HostHandoffPackage: Codable, Sendable {
    public var operationID: String
    public var sourceHostID: String
    public var destinationHostID: String
    public var destinationWorkspaceID: String
    public var native: NativeConversationTransfer
    public var workspace: WorkspaceTransfer
}

public struct HostHandoffCertificate: Codable, Sendable {
    public var operationID: String
    public var sourceHostID: String
    public var destinationHostID: String
    public var digest: String
    public var stage: String
    public var signature: String
}

/// Signing keys belong to host identity, not retrieval machine aliases. Pairing
/// pins the public key; receipts cannot be fabricated by a confused viewer.
final class HostHandoffSigner {
    private let key: Curve25519.Signing.PrivateKey
    var publicKey: String { key.publicKey.rawRepresentation.base64EncodedString() }
    init(directory: URL) throws {
        let path = directory.appendingPathComponent("handoff-signing-key")
        if FileManager.default.fileExists(atPath: path.path) {
            key = try Curve25519.Signing.PrivateKey(rawRepresentation: Data(contentsOf: path))
        } else {
            key = Curve25519.Signing.PrivateKey()
            try key.rawRepresentation.write(to: path, options: .withoutOverwriting)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: path.path)
            let file = try FileHandle(forWritingTo: path); try file.synchronize(); try file.close()
        }
    }
    func certificate(_ record: HostHandoffRecord, stage: String) throws -> HostHandoffCertificate {
        guard let digest = record.digest else { throw HostFailure("handoff_state", "Handoff snapshot has not been captured") }
        var result = HostHandoffCertificate(operationID: record.id, sourceHostID: record.sourceHostID,
            destinationHostID: record.destinationHostID, digest: digest, stage: stage, signature: "")
        result.signature = try key.signature(for: Self.signingData(result)).base64EncodedString()
        return result
    }
    static func verify(_ certificate: HostHandoffCertificate, publicKey: String, record: HostHandoffRecord, stage: String) throws {
        guard certificate.operationID == record.id, certificate.sourceHostID == record.sourceHostID,
              certificate.destinationHostID == record.destinationHostID, certificate.digest == record.digest,
              certificate.stage == stage, let bytes = Data(base64Encoded: publicKey),
              let signature = Data(base64Encoded: certificate.signature),
              let key = try? Curve25519.Signing.PublicKey(rawRepresentation: bytes),
              key.isValidSignature(signature, for: try signingData(certificate)) else {
            throw HostFailure("handoff_identity", "Handoff receipt does not match the pinned peer, operation, and snapshot")
        }
    }
    static func digest(_ package: HostHandoffPackage) throws -> String {
        let data = try encoded(package)
        guard data.count <= 30 * 1024 * 1024 else { throw HostFailure("handoff_limit", "Combined native and workspace transfer exceeds 30 MiB") }
        return SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }
    static func encoded<T: Encodable>(_ value: T) throws -> Data {
        let encoder = JSONEncoder(); encoder.outputFormatting = [.sortedKeys]
        return try encoder.encode(value)
    }
    private static func signingData(_ certificate: HostHandoffCertificate) throws -> Data {
        var unsigned = certificate; unsigned.signature = ""
        return try encoded(unsigned)
    }
}
