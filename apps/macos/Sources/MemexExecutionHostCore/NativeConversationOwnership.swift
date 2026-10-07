import Darwin
import Foundation

/// Host-local native writer detection shared by the app and execution service.
public enum NativeConversationOwnership {
    /// Codex has a native writer lock. Claude has no equivalent verified lock
    /// contract here, so detect positive OS evidence of an open writer instead.
    /// This is a point-in-time check, not cross-process exclusion or a guarantee
    /// that a Claude process which closes its file between appends is idle.
    public static func isOpenElsewhere(provider: String, nativeSessionID: String,
                                       sourceURL: URL, providerHome: URL) throws -> Bool {
        if provider == "claude" { return try hasExternalWriter(at: sourceURL) }
        guard provider == "codex", let id = UUID(uuidString: nativeSessionID) else { return false }
        let home = providerHome.resolvingSymlinksInPath()
        let lock = home.appendingPathComponent("thread-writer-locks")
            .appendingPathComponent(id.uuidString.lowercased() + ".lock")
        let descriptor = open(lock.path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW)
        guard descriptor >= 0 else {
            if errno == ENOENT { return false }
            throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
        }
        defer { close(descriptor) }
        if flock(descriptor, LOCK_SH | LOCK_NB) == 0 {
            _ = flock(descriptor, LOCK_UN)
            return false
        }
        if errno == EWOULDBLOCK { return true }
        throw POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }

    public static func hasExternalWriter(at file: URL) throws -> Bool {
        guard FileManager.default.fileExists(atPath: file.path) else { return false }
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/sbin/lsof")
        process.arguments = ["-nP", "-w", "-Fpa", "--", file.path]
        let output = Pipe()
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        try process.run()
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        guard process.terminationReason == .exit, [0, 1].contains(process.terminationStatus) else {
            throw NativeConversationOwnershipError.inspectionFailed
        }
        return containsExternalWriter(String(decoding: data, as: UTF8.self), excludingPID: getpid())
    }

    public static func containsExternalWriter(_ output: String, excludingPID: Int32) -> Bool {
        var pid: Int32?
        for line in output.split(separator: "\n") {
            if line.first == "p" { pid = Int32(line.dropFirst()) }
            if let pid, pid != excludingPID, line == "aw" || line == "au" { return true }
        }
        return false
    }

}

private enum NativeConversationOwnershipError: LocalizedError {
    case inspectionFailed
    var errorDescription: String? {
        "The system could not inspect the native transcript's active writers."
    }
}
