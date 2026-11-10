import Foundation
import CryptoKit

@MainActor
final class RemoteWorkspaceDrafts {
    struct Draft: Codable, Equatable {
        var text: String
        var revision: String
        var original: String
    }
    static let shared = RemoteWorkspaceDrafts()
    private let directory: URL
    init(directory: URL = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        .appendingPathComponent("dev.memex.app/RemoteFileDrafts")) { self.directory = directory }
    func read(hostID: String, workspaceID: String, path: String) throws -> Draft? {
        let url = location(hostID, workspaceID, path)
        guard FileManager.default.fileExists(atPath: url.path) else { return nil }
        return try JSONDecoder().decode(Draft.self, from: Data(contentsOf: url))
    }
    func save(_ draft: Draft, hostID: String, workspaceID: String, path: String) throws {
        let url = location(hostID, workspaceID, path)
        if draft.text == draft.original {
            if FileManager.default.fileExists(atPath: url.path) { try FileManager.default.removeItem(at: url) }
            return
        }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        try JSONEncoder().encode(draft).write(to: url, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
    }
    private func location(_ host: String, _ workspace: String, _ path: String) -> URL {
        // Opaque remote identifiers are never canonicalized on the viewing Mac.
        let key = [host, workspace, path].joined(separator: "\0")
        let digest = SHA256.hash(data: Data(key.utf8)).map { String(format: "%02x", $0) }.joined()
        return directory.appendingPathComponent(digest + ".json")
    }
}
