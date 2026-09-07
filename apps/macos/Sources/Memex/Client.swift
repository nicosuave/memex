import Foundation
import Darwin

struct ClientError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}

/// Run each request off the UI thread, with cancellation and a bounded lifetime.
/// Files drain both streams without pipe-buffer deadlocks on large transcripts.
final class CommandRun: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false

    func cancel() {
        lock.lock()
        cancelled = true
        lock.unlock()
    }

    func execute(executable: URL, arguments: [String], timeout: TimeInterval) throws -> Data {
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
        let child = Process()
        child.executableURL = executable
        child.arguments = arguments
        child.standardOutput = output
        child.standardError = errors
        child.standardInput = FileHandle.nullDevice
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
        let deadline = Date().addingTimeInterval(timeout)
        var stoppingSince: Date?
        var timedOut = false
        while child.isRunning {
            lock.lock()
            let shouldCancel = cancelled
            lock.unlock()
            if stoppingSince == nil && (shouldCancel || Date() >= deadline) {
                timedOut = !shouldCancel
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
        lock.lock()
        let wasCancelled = cancelled
        lock.unlock()
        // The CLI can exit on SIGTERM while SSH or another descendant ignores
        // it. Clean up the owned group even after the direct child has exited.
        if wasCancelled || timedOut { stop(SIGKILL) }
        if wasCancelled { throw CancellationError() }
        if timedOut { throw ClientError(message: "Memex took too long to respond. Try again.") }
        guard child.terminationStatus == 0 else {
            let message = (try? String(contentsOf: errorURL, encoding: .utf8)) ?? "Memex could not complete the request."
            throw ClientError(message: String(message.prefix(4000)))
        }
        return try Data(contentsOf: outputURL)
    }
}

struct MemexClient: Sendable {
    let executable: URL
    let root: String?
    static let pageSize = 60

    init(executable: URL? = nil, root: String? = nil) {
        let environment = ProcessInfo.processInfo.environment
        let bundled = Bundle.main.bundleURL.appendingPathComponent("Contents/Helpers/memex")
        let candidates: [String?] = [environment["MEMEX_CLI"], bundled.path, "/opt/homebrew/bin/memex", "/usr/local/bin/memex"]
        self.executable = executable ?? URL(fileURLWithPath: candidates.compactMap { $0 }.first {
            FileManager.default.isExecutableFile(atPath: $0)
        } ?? bundled.path)
        self.root = root ?? environment["MEMEX_ROOT"]
    }

    func run(_ arguments: [String], timeout: TimeInterval = 60) async throws -> Data {
        guard FileManager.default.isExecutableFile(atPath: executable.path) else {
            throw ClientError(message: "Memex CLI is missing. Build the app with scripts/build.sh or set MEMEX_CLI to your memex executable.")
        }
        var args = ["--no-update-check", "--non-interactive"] + arguments
        if let root {
            args.insert(contentsOf: ["--root", root], at: args.firstIndex(of: "--") ?? args.endIndex)
        }
        let command = CommandRun()
        let arguments = args
        return try await withTaskCancellationHandler {
            try await Task.detached(priority: .userInitiated) {
                try command.execute(executable: executable, arguments: arguments, timeout: timeout)
            }.value
        } onCancel: { command.cancel() }
    }

    func machines() async throws -> [MachineChoice] {
        try JSONDecoder().decode([MachineChoice].self, from: await run(["machines", "--format", "json"]))
    }

    func sessions(limit: Int, project: String? = nil, source: String? = nil, machine: String = "local",
                  since: String? = nil, origin: ConversationOrigin = .all) async throws -> [Session] {
        var args = ["sessions", "--format", "json", "--limit", String(limit)]
        args += ["--machine", machine]
        if let project { args += ["--project", project] }
        if let source { args += ["--source", source] }
        if let since { args += ["--since", since] }
        args += ["--origin", origin.argument]
        return try JSONDecoder().decode([Session].self, from: await run(args))
    }

    func sessionDetails(for session: Session) async throws -> Session {
        let args = ["sessions", "--format", "json", "--machine", session.machineID,
                    "--source", session.source, "--session-id=\(session.sessionID)",
                    "--source-path=\(session.sourcePath)", "--origin", "all", "--limit", "1"]
        let rows = try JSONDecoder().decode([Session].self, from: await run(args))
        guard rows.count == 1, let detail = rows.first, detail.id == session.id else {
            throw ClientError(message: "This conversation's resume details are unavailable. Refresh conversations and try again.")
        }
        return detail
    }

    func projects(machine: String = "local") async throws -> [ProjectSummary] {
        try JSONDecoder().decode([ProjectSummary].self, from: await run(["projects", "--format", "json", "--machine", machine]))
    }

    func search(_ query: String, project: String?, source: String?, limit: Int, machine: String = "local",
                since: String? = nil, origin: ConversationOrigin = .all) async throws -> [SearchHit] {
        var args = ["search", "--format", "json", "--machine", machine, "--mode", "lexical",
                    "--unique-session", "--limit", String(limit),
                    "--fields", "source,session_id,source_path,project,snippet,ts,machine,record_id"]
        if let project { args += ["--project", project] }
        if let source { args += ["--source", source] }
        if let since { args += ["--since", since] }
        args += ["--origin", origin.argument]
        return try JSONDecoder().decode([SearchHit].self, from: await run(args + ["--", query]))
    }

    func records(for session: Session, offset: Int, limit: Int = Self.pageSize) async throws -> [TranscriptRecord] {
        // --full preserves all content; pagination bounds the number of records.
        let args = ["session", "--machine", session.machineID, "--source-path", session.sourcePath,
                    "--offset", String(offset), "--limit", String(limit), "--full", "--format", "json",
                    "--", session.sessionID]
        return try JSONDecoder().decode([TranscriptRecord].self, from: await run(args))
    }

    /// The CLI has no ID-only session mode. Bound search scans to 256K characters
    /// and discard their bodies; a one-record total probe needs only one character.
    func recordMetadata(for session: Session, offset: Int, limit: Int) async throws -> RecordMetadataPage {
        let args = ["session", "--machine", session.machineID, "--source-path", session.sourcePath,
                    "--offset", String(offset), "--limit", String(limit), "--max-chars", limit == 1 ? "1" : "262144",
                    "--format", "json", "--", session.sessionID]
        let entries = try JSONDecoder().decode([RecordMetadataEntry].self, from: await run(args))
        guard let page = entries.last(where: { $0.type == "page" }), let total = page.total else {
            throw ClientError(message: "Memex did not return transcript pagination metadata.")
        }
        return RecordMetadataPage(ids: entries.compactMap(\.recordID), total: total, nextOffset: page.nextOffset)
    }

    func initialRecordOffset(for session: Session, anchor: String?) async throws -> (offset: Int, total: Int) {
        var page = try await recordMetadata(for: session, offset: 0, limit: anchor == nil ? 1 : 500)
        guard let anchor else { return (max(0, page.total - Self.pageSize), page.total) }
        var offset = 0
        while true {
            try Task.checkCancellation()
            if let index = page.ids.firstIndex(of: anchor) {
                return (max(0, offset + index - Self.pageSize / 2), page.total)
            }
            guard let next = page.nextOffset, next > offset else {
                return (max(0, page.total - Self.pageSize), page.total)
            }
            offset = next
            page = try await recordMetadata(for: session, offset: offset, limit: 500)
        }
    }
}

struct RecordMetadataPage: Sendable {
    let ids: [String]
    let total: Int
    let nextOffset: Int?
}

private struct RecordMetadataEntry: Decodable {
    let type: String?
    let recordID: String?
    let total: Int?
    let nextOffset: Int?
    enum CodingKeys: String, CodingKey {
        case type, total
        case recordID = "record_id", nextOffset = "next_offset"
    }
}
