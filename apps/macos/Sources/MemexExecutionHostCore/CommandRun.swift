import Foundation
import Darwin

public struct ClientError: LocalizedError {
    public init(message: String) { self.message = message }
    public let message: String
    public var errorDescription: String? { message }
}

public struct ActivityScanProgress: Decodable, Sendable, Equatable {
    public let source: String
    public let done: Int
    public let total: Int
    public var isValid: Bool { !source.isEmpty && source.count <= 100 && done >= 0 && total > 0 && done <= total }
}

public typealias ActivityProgressHandler = @Sendable (ActivityScanProgress) -> Void

/// Run each request off the UI thread, with cancellation and a watchdog.
/// Files drain both streams without pipe-buffer deadlocks on large transcripts.
public final class CommandRun: @unchecked Sendable {
    public init() {}
    private let lock = NSLock()
    private var cancelled = false

    public func cancel() {
        lock.lock()
        cancelled = true
        lock.unlock()
    }

    public func execute(executable: URL, arguments: [String], timeout: TimeInterval, progress: ActivityProgressHandler? = nil,
                 inputFile: URL? = nil, maximumOutputBytes: Int? = nil) throws -> Data {
        if let maximumOutputBytes, maximumOutputBytes < 0 || maximumOutputBytes == Int.max { throw ClientError(message: "Output limit must be a nonnegative bounded integer.") }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let outputURL = directory.appendingPathComponent("stdout")
        let errorURL = directory.appendingPathComponent("stderr")
        FileManager.default.createFile(atPath: outputURL.path, contents: nil)
        FileManager.default.createFile(atPath: errorURL.path, contents: nil)
        let output = try FileHandle(forWritingTo: outputURL)
        let errors = try FileHandle(forWritingTo: errorURL)
        defer { try? output.close(); try? errors.close() }
        let progressReader = progress == nil ? nil : try FileHandle(forReadingFrom: errorURL)
        defer { try? progressReader?.close() }
        let child = Process()
        child.executableURL = executable
        child.arguments = arguments
        child.standardOutput = output
        child.standardError = errors
        let input = try inputFile.map { try FileHandle(forReadingFrom: $0) }
        defer { try? input?.close() }
        child.standardInput = input ?? FileHandle.nullDevice
        lock.lock()
        if cancelled { lock.unlock(); throw CancellationError() }
        do { try child.run() } catch { lock.unlock(); throw error }
        lock.unlock()
        let childID = child.processIdentifier
        // Foundation normally gives its child a dedicated process group. Only
        // signal that group when ownership is verified; never signal ours.
        let ownedGroup = getpgid(childID) == childID && childID > 1 && childID != getpgrp() ? childID : nil
        func stop(_ signal: Int32) {
            if let ownedGroup, ownedGroup != getpgrp() {
                kill(-ownedGroup, signal)
            } else if child.isRunning {
                kill(childID, signal)
            }
        }
        var deadline = ContinuousClock.now.advanced(by: .seconds(timeout))
        var progressBuffer = Data()
        var completedBySource: [String: Int] = [:]
        func consumeProgress() {
            guard let progressReader, let bytes = try? progressReader.read(upToCount: 65_536), !bytes.isEmpty else { return }
            progressBuffer.append(bytes)
            while let newline = progressBuffer.firstIndex(of: 10) {
                let line = Data(progressBuffer[..<newline])
                progressBuffer.removeSubrange(...newline)
                let prefix = Data("MEMEX_PROGRESS ".utf8)
                guard line.starts(with: prefix),
                      let update = try? JSONDecoder().decode(ActivityScanProgress.self, from: line.dropFirst(prefix.count)),
                      update.isValid else { continue }
                if let previous = completedBySource[update.source], update.done <= previous { continue }
                completedBySource[update.source] = update.done
                // A cold usage backfill can outlive a normal read. Keep the same
                // watchdog duration, measured since the last advancing counter.
                if update.done > 0 { deadline = ContinuousClock.now.advanced(by: .seconds(timeout)) }
                progress?(update)
            }
            if progressBuffer.count > 65_536 { progressBuffer.removeAll(keepingCapacity: true) }
        }
        var stoppingSince: Date?
        var timedOut = false
        var outputLimitExceeded = false
        func exceedsOutputLimit() -> Bool {
            guard let limit = maximumOutputBytes else { return false }
            let stdoutBytes = (try? FileManager.default.attributesOfItem(atPath: outputURL.path)[.size] as? NSNumber)?.int64Value ?? 0
            let stderrBytes = (try? FileManager.default.attributesOfItem(atPath: errorURL.path)[.size] as? NSNumber)?.int64Value ?? 0
            return stdoutBytes > Int64(limit) || stderrBytes > Int64(limit) - stdoutBytes
        }
        while child.isRunning {
            consumeProgress()
            lock.lock()
            let shouldCancel = cancelled
            lock.unlock()
            outputLimitExceeded = outputLimitExceeded || exceedsOutputLimit()
            if stoppingSince == nil && (shouldCancel || outputLimitExceeded || ContinuousClock.now >= deadline) {
                timedOut = !shouldCancel && !outputLimitExceeded
                stoppingSince = Date()
                stop(SIGTERM)
            }
            if let stoppingSince, Date().timeIntervalSince(stoppingSince) >= 1 {
                // A CLI that ignores SIGTERM must not outlive cancellation forever.
                stop(SIGKILL)
            }
            Thread.sleep(forTimeInterval: 0.025)
        }
        child.waitUntilExit()
        consumeProgress()
        lock.lock()
        let wasCancelled = cancelled
        lock.unlock()
        // The CLI can exit on SIGTERM while SSH or another descendant ignores
        // it. Clean up the owned group even after the direct child has exited.
        outputLimitExceeded = outputLimitExceeded || exceedsOutputLimit()
        if wasCancelled || timedOut || outputLimitExceeded { stop(SIGKILL) }
        if wasCancelled { throw CancellationError() }
        if outputLimitExceeded || exceedsOutputLimit() { throw ClientError(message: "Command output exceeded the \(maximumOutputBytes ?? 0) byte limit.") }
        if timedOut { throw ClientError(message: "Memex took too long to respond. Try again.") }
        guard child.terminationStatus == 0 else {
            let errorReader = try? FileHandle(forReadingFrom: errorURL)
            let errorBytes = try? errorReader?.read(upToCount: 8192)
            try? errorReader?.close()
            let message = errorBytes.map { String(decoding: $0, as: UTF8.self) } ?? "Memex could not complete the request."
            let failure = message.split(separator: "\n", omittingEmptySubsequences: false)
                .filter { !$0.hasPrefix("MEMEX_PROGRESS ") }.joined(separator: "\n")
            throw ClientError(message: String(failure.prefix(4000)))
        }
        if let maximumOutputBytes {
            let reader = try FileHandle(forReadingFrom: outputURL); defer { try? reader.close() }
            let bytes = try reader.read(upToCount: maximumOutputBytes + 1) ?? Data()
            guard bytes.count <= maximumOutputBytes else { throw ClientError(message: "Command output exceeded the \(maximumOutputBytes) byte limit.") }
            return bytes
        }
        return try Data(contentsOf: outputURL)
    }
}
