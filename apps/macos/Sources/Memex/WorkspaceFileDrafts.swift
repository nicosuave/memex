import CryptoKit
import Foundation

/// Kept separately from workspace files. Switching chats, closing a pane or
/// relaunching must not discard an unsaved editor buffer.
@MainActor final class WorkspaceFileDrafts {
    struct Draft: Codable, Equatable {
        let original: String
        let expectedFingerprint: String
        let edited: String
    }
    static let shared = WorkspaceFileDrafts()
    private let storage: URL

    init(storage: URL = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent("Library/Application Support/dev.memex.app/FileDrafts", isDirectory: true)) {
        self.storage = storage
    }

    func read(root: URL, path: String) throws -> Draft? {
        let file = location(root: root, path: path)
        guard FileManager.default.fileExists(atPath: file.path) else { return nil }
        return try JSONDecoder().decode(Draft.self, from: Data(contentsOf: file))
    }

    func save(root: URL, path: String, original: WorkspaceFileRevision, edited: String) throws {
        let file = location(root: root, path: path)
        if original.contents == edited {
            if FileManager.default.fileExists(atPath: file.path) { try FileManager.default.removeItem(at: file) }
            return
        }
        try FileManager.default.createDirectory(at: storage, withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700])
        let draft = Draft(original: original.contents, expectedFingerprint: original.fingerprint, edited: edited)
        try JSONEncoder().encode(draft).write(to: file, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    }

    func discard(root: URL, path: String) throws {
        let file = location(root: root, path: path)
        if FileManager.default.fileExists(atPath: file.path) { try FileManager.default.removeItem(at: file) }
    }

    private func location(root: URL, path: String) -> URL {
        let key = root.standardizedFileURL.resolvingSymlinksInPath().path + "\0" + path
        let hash = SHA256.hash(data: Data(key.utf8)).map { String(format: "%02x", $0) }.joined()
        return storage.appendingPathComponent(hash + ".json")
    }
}
