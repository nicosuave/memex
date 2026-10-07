import Foundation
import CryptoKit
import Darwin

enum NativeConversationTransferValidation {
    static let limit = 8 * 1024 * 1024
    static func digest(_ data: Data) -> String { SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined() }
    static func validate(_ transfer: NativeConversationTransfer) throws {
        let c = transfer.conversation
        guard c.provider == "codex", UUID(uuidString: c.nativeSessionID) != nil,
              c.providerInstanceID.hasPrefix("codex:/"), transfer.transcript.count <= limit,
              digest(transfer.transcript) == transfer.transcriptSHA256 else {
            throw HostFailure("handoff_identity", "Native transfer provider, size, identity, or checksum is invalid")
        }
        let lines = transfer.transcript.split(separator: 10, omittingEmptySubsequences: true)
        guard !lines.isEmpty else { throw HostFailure("handoff_identity", "Native rollout is empty") }
        var found = false
        for line in lines {
            let value = try JSONDecoder().decode(HostValue.self, from: Data(line))
            if value["type"].string == "session_meta" {
                guard value["payload"]["id"].string == c.nativeSessionID,
                      value["payload"]["cwd"].string?.hasPrefix("/") == true else {
                    throw HostFailure("handoff_identity", "Native rollout metadata does not match the transferred session")
                }
                found = true
            }
        }
        guard found else { throw HostFailure("handoff_identity", "Native rollout has no verified session metadata") }
    }
    static func read(_ url: URL) throws -> Data {
        let fd = open(url.path, O_RDONLY | O_NOFOLLOW | O_CLOEXEC)
        guard fd >= 0 else { throw HostFailure("source_denied", "Native rollout cannot be opened without following a link") }
        let handle = FileHandle(fileDescriptor: fd, closeOnDealloc: true)
        defer { try? handle.close() }
        var info = stat()
        guard fstat(fd, &info) == 0, (info.st_mode & S_IFMT) == S_IFREG, info.st_nlink == 1, info.st_size <= limit else {
            throw HostFailure("handoff_limit", "Native transfer requires a regular unlinked rollout of at most 8 MiB")
        }
        let data = try handle.read(upToCount: limit + 1) ?? Data()
        guard data.count <= limit else { throw HostFailure("handoff_limit", "Native rollout grew beyond its transfer bound") }
        return data
    }
}

/// Codex's actual native writer lock fences external source writers too. The
/// catalog owner must reacquire persisted fences before serving after restart.
final class NativeHandoffWriterFence {
    private let descriptor: Int32
    init(home: URL, nativeSessionID: String) throws {
        guard let id = UUID(uuidString: nativeSessionID) else { throw HostFailure("handoff_identity", "Invalid native session UUID") }
        let directory = home.appendingPathComponent("thread-writer-locks")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        let lock = directory.appendingPathComponent(id.uuidString.lowercased() + ".lock")
        descriptor = open(lock.path, O_RDWR | O_CREAT | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard descriptor >= 0 else { throw HostFailure("handoff_ownership", "Cannot open the native writer fence") }
        guard flock(descriptor, LOCK_EX | LOCK_NB) == 0 else {
            close(descriptor)
            throw HostFailure("open_elsewhere", "The native conversation still has another writer")
        }
    }
    deinit { flock(descriptor, LOCK_UN); close(descriptor) }
}
