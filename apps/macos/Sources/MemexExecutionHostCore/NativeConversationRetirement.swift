import Foundation
import Darwin

/// A completed transfer must remain retired when the source host is stopped.
/// Preserve raw history outside Codex's native lookup roots; never point the
/// native database at the retained copy. Process locks alone are insufficient.
final class NativeConversationRetirement {
    let directory: URL
    init(directory: URL) { self.directory = directory }

    func retire(_ transfer: NativeConversationTransfer, operationID: String, home: URL) throws {
        try NativeConversationTransferValidation.validate(transfer)
        let source = try sourceURL(transfer, home: home)
        let retained = try retainedURL(transfer, operationID: operationID)
        try rejectOtherRollouts(nativeID: transfer.conversation.nativeSessionID, source: source, home: home)
        if FileManager.default.fileExists(atPath: retained.path) {
            guard try NativeConversationTransferValidation.read(retained) == transfer.transcript else {
                throw HostFailure("handoff_identity", "Retired native history does not match the transfer")
            }
        } else {
            guard try NativeConversationTransferValidation.read(source) == transfer.transcript else {
                throw HostFailure("handoff_conflict", "Source native history changed after export. Cancel the transfer and capture its newer history.")
            }
            try transfer.transcript.write(to: retained, options: .withoutOverwriting)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: retained.path)
        }
        let file = try FileHandle(forWritingTo: retained); try file.synchronize(); try file.close()
        try Self.synchronizeDirectory(retained.deletingLastPathComponent())
        try Self.synchronizeDirectory(directory)
        try Self.synchronizeDirectory(directory.deletingLastPathComponent())
        if FileManager.default.fileExists(atPath: source.path) {
            guard try NativeConversationTransferValidation.read(source) == transfer.transcript else {
                throw HostFailure("handoff_conflict", "Source native history changed before retirement; neither version was overwritten")
            }
            guard unlink(source.path) == 0 else { throw HostFailure("handoff_retirement", "Cannot retire the source native lookup file") }
        }
        // A retry after unlink but before directory sync must also prove that
        // removal durable before the host signs an ownership commitment.
        try Self.synchronizeDirectory(source.deletingLastPathComponent())
        guard !FileManager.default.fileExists(atPath: source.path) else { throw HostFailure("handoff_retirement", "Source native history is still discoverable") }
    }

    /// Only after the destination's signed abort tombstone. An existing newer
    /// source is validated and preserved instead of overwritten by old history.
    func restore(_ transfer: NativeConversationTransfer, operationID: String, home: URL) throws {
        let source = try sourceURL(transfer, home: home)
        if FileManager.default.fileExists(atPath: source.path) {
            let current = try NativeConversationTransferValidation.read(source)
            var verified = transfer; verified.transcript = current
            verified.transcriptSHA256 = NativeConversationTransferValidation.digest(current)
            try NativeConversationTransferValidation.validate(verified)
            let file = try FileHandle(forWritingTo: source); try file.synchronize(); try file.close()
            try Self.synchronizeDirectory(source.deletingLastPathComponent())
            return
        }
        let retained = try retainedURL(transfer, operationID: operationID)
        guard try NativeConversationTransferValidation.read(retained) == transfer.transcript else {
            throw HostFailure("handoff_identity", "Retained native history does not match the aborted transfer")
        }
        try transfer.transcript.write(to: source, options: .withoutOverwriting)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: source.path)
        let file = try FileHandle(forWritingTo: source); try file.synchronize(); try file.close()
        try Self.synchronizeDirectory(source.deletingLastPathComponent())
    }

    private func sourceURL(_ transfer: NativeConversationTransfer, home: URL) throws -> URL {
        guard let path = transfer.conversation.transcriptPath else { throw HostFailure("source_denied", "Missing original native rollout path") }
        let source = URL(fileURLWithPath: path).standardizedFileURL
        guard source.path == source.resolvingSymlinksInPath().path,
              source.path.hasPrefix(home.appendingPathComponent("sessions").path + "/") else {
            throw HostFailure("source_denied", "Source native rollout is outside its canonical provider home")
        }
        return source
    }
    private func retainedURL(_ transfer: NativeConversationTransfer, operationID: String) throws -> URL {
        guard UUID(uuidString: operationID) != nil, UUID(uuidString: transfer.conversation.nativeSessionID) != nil else {
            throw HostFailure("handoff_identity", "Invalid retirement operation or native identity")
        }
        let target = directory.appendingPathComponent(operationID)
        try FileManager.default.createDirectory(at: target, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        guard target.resolvingSymlinksInPath().path == target.path else { throw HostFailure("source_denied", "Retirement storage contains a symbolic link") }
        return target.appendingPathComponent(transfer.conversation.nativeSessionID + ".jsonl")
    }
    private func rejectOtherRollouts(nativeID: String, source: URL, home: URL) throws {
        let databases = try FileManager.default.contentsOfDirectory(atPath: home.path)
            .filter { $0.hasPrefix("state_") && $0.hasSuffix(".sqlite") }.map { home.appendingPathComponent($0) }
        for database in databases {
            guard database.resolvingSymlinksInPath() == database else { throw HostFailure("source_denied", "Native state database is a symbolic link") }
            let data = try CommandRun().execute(executable: URL(fileURLWithPath: "/usr/bin/sqlite3"),
                arguments: ["-readonly", database.path, "select rollout_path from threads where id = '\(nativeID)';"],
                timeout: 5, maximumOutputBytes: 65536)
            for path in String(decoding: data, as: UTF8.self).split(separator: "\n") where String(path) != source.path {
                if FileManager.default.fileExists(atPath: String(path)) {
                    throw HostFailure("handoff_identity", "The native session index points at another retained rollout; reconcile it before transferring")
                }
            }
        }
        for name in ["sessions", "archived_sessions"] {
            let root = home.appendingPathComponent(name)
            guard let entries = FileManager.default.enumerator(atPath: root.path) else { continue }
            var count = 0
            for case let path as String in entries {
                let url = root.appendingPathComponent(path)
                count += 1
                guard count <= 100000 else { throw HostFailure("handoff_limit", "Native rollout inventory exceeds the safe retirement scan bound") }
                if try url.resourceValues(forKeys: [.isSymbolicLinkKey]).isSymbolicLink == true { entries.skipDescendants(); continue }
                guard url.path != source.path else { continue }
                let named = url.lastPathComponent.lowercased().contains(nativeID.lowercased())
                if named && url.lastPathComponent.contains(".jsonl.") {
                    throw HostFailure("handoff_identity", "A compressed native rollout also names this session")
                }
                guard url.pathExtension == "jsonl" else { continue }
                let handle = try FileHandle(forReadingFrom: url)
                defer { try? handle.close() }
                let prefix = try handle.read(upToCount: 65536) ?? Data()
                let first = Data(prefix.prefix(while: { $0 != 10 }))
                let metadata = try? JSONDecoder().decode(HostValue.self, from: first)
                if named || metadata?["payload"]["id"].string == nativeID {
                    throw HostFailure("handoff_identity", "Another active or archived native rollout names this session. Reconcile the duplicate before transferring ownership.")
                }
            }
        }
    }
    static func synchronizeDirectory(_ url: URL) throws {
        let fd = open(url.path, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC)
        guard fd >= 0 else { throw HostFailure("handoff_retirement", "Cannot open native history directory for synchronization") }
        defer { close(fd) }
        guard fsync(fd) == 0 else { throw HostFailure("handoff_retirement", "Cannot synchronize native history retirement") }
    }
}
