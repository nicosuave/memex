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

    /// The same check for UI callers. It never blocks the caller's executor:
    /// `lsof` runs on a detached task with a deadline, and cancelling the
    /// caller terminates the inspection.
    public static func isOpenElsewhere(provider: String, nativeSessionID: String, sourceURL: URL,
                                       providerHome: URL, timeout: Duration) async throws -> Bool {
        if provider == "claude" { return try await hasExternalWriter(at: sourceURL, timeout: timeout) }
        return try await Task.detached {
            try isOpenElsewhere(provider: provider, nativeSessionID: nativeSessionID,
                                sourceURL: sourceURL, providerHome: providerHome)
        }.value
    }

    public static func hasExternalWriter(at file: URL) throws -> Bool {
        try hasExternalWriter(at: file, timeout: nil, inspection: nil)
    }

    public static func hasExternalWriter(at file: URL, timeout: Duration) async throws -> Bool {
        let inspection = WriterInspection()
        return try await withTaskCancellationHandler {
            try await Task.detached {
                try hasExternalWriter(at: file, timeout: timeout, inspection: inspection)
            }.value
        } onCancel: { inspection.stop(.cancelled) }
    }

    private static func hasExternalWriter(at file: URL, timeout: Duration?,
                                          inspection: WriterInspection?) throws -> Bool {
        guard FileManager.default.fileExists(atPath: file.path) else { return false }
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/sbin/lsof")
        process.arguments = ["-nP", "-w", "-Fpa", "--", file.path]
        let output = Pipe()
        process.standardOutput = output
        process.standardError = FileHandle.nullDevice
        if let inspection { try inspection.start(process) } else { try process.run() }
        if let inspection, let timeout {
            let (seconds, attoseconds) = timeout.components
            let delay = Double(seconds) + Double(attoseconds) / 1e18
            DispatchQueue.global().asyncAfter(deadline: .now() + delay) { inspection.stop(.expired) }
        }
        let data = output.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        switch inspection?.outcome {
        case .cancelled: throw CancellationError()
        case .expired: throw NativeConversationOwnershipError.timedOut
        case nil: break
        }
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

/// Owns one `lsof` run so its deadline or the caller's cancellation can end it.
private final class WriterInspection: @unchecked Sendable {
    enum Outcome { case cancelled, expired }
    private let lock = NSLock()
    private var process: Process?
    private var stoppedBy: Outcome?

    var outcome: Outcome? { lock.withLock { stoppedBy } }

    func start(_ process: Process) throws {
        try lock.withLock {
            if stoppedBy != nil { throw CancellationError() }
            try process.run()
            self.process = process
        }
    }

    func stop(_ reason: Outcome) {
        lock.withLock {
            guard stoppedBy == nil else { return }
            stoppedBy = reason
            if let process, process.isRunning { process.terminate() }
        }
    }
}

private enum NativeConversationOwnershipError: LocalizedError {
    case inspectionFailed, timedOut
    var errorDescription: String? {
        switch self {
        case .inspectionFailed: "The system could not inspect the native transcript's active writers."
        case .timedOut: "Inspecting the native transcript's active writers took too long."
        }
    }
}
