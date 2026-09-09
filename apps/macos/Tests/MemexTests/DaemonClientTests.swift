import Foundation
import Darwin
import Testing
@testable import Memex

private final class SocketFixture: @unchecked Sendable {
    let root: URL
    let socketURL: URL
    let executable: URL
    private let listener: Int32
    private let lock = NSLock()
    private var stopped = false
    private var connections = 0
    private var messages: [[String: Any]] = []
    private var behavior = "normal"
    private var children = Set<Int32>()

    init() throws {
        root = URL(fileURLWithPath: "/private/tmp/mx-\(UUID().uuidString)").resolvingSymlinksInPath()
        let directory = root.appendingPathComponent("state/native")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700])
        socketURL = directory.appendingPathComponent("app.sock")
        executable = root.appendingPathComponent("cli")
        try "#!/bin/sh\nprintf used >> \"$(dirname \"$0\")/fallback\"\nprintf '[]'\n".write(to: executable, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
        listener = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard listener >= 0 else { throw ClientError(message: "socket fixture creation failed") }
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        withUnsafeMutableBytes(of: &address.sun_path) { $0.copyBytes(from: Array(socketURL.path.utf8) + [0]) }
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(listener, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard result == 0, Darwin.listen(listener, 12) == 0, fcntl(listener, F_SETFL, O_NONBLOCK) == 0 else {
            Darwin.close(listener)
            throw ClientError(message: "socket fixture bind failed")
        }
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: socketURL.path)
        Task.detached { [self] in acceptConnections() }
    }

    var client: MemexClient { MemexClient(executable: executable, root: root.path, daemonSocket: socketURL) }
    var fallbackUsed: Bool { FileManager.default.fileExists(atPath: root.appendingPathComponent("fallback").path) }
    var connectionCount: Int { lock.withLock { connections } }
    var requests: [[String: Any]] { lock.withLock { messages } }
    func setBehavior(_ value: String) { lock.withLock { behavior = value } }
    func disconnect() { lock.withLock { for child in children { _ = shutdown(child, SHUT_RDWR) } } }
    func stop() {
        lock.withLock {
            stopped = true
            for child in children { _ = shutdown(child, SHUT_RDWR) }
        }
        try? FileManager.default.removeItem(at: root)
    }
    func waitFor(_ op: String) async throws {
        let deadline = ContinuousClock.now.advanced(by: .seconds(5))
        while !requests.contains(where: { $0["op"] as? String == op }), ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(5))
        }
        try #require(requests.contains { $0["op"] as? String == op })
    }

    private func acceptConnections() {
        defer { Darwin.close(listener) }
        while !lock.withLock({ stopped }) {
            let child = Darwin.accept(listener, nil, nil)
            if child < 0 { usleep(1000); continue }
            _ = fcntl(child, F_SETFL, 0)
            var enabled: Int32 = 1
            _ = setsockopt(child, SOL_SOCKET, SO_NOSIGPIPE, &enabled, socklen_t(MemoryLayout.size(ofValue: enabled)))
            lock.withLock { connections += 1; _ = children.insert(child) }
            Task.detached { [self] in serve(child) }
        }
    }
    private func serve(_ child: Int32) {
        defer {
            lock.withLock { _ = children.remove(child); Darwin.close(child) }
        }
        while let header = read(child, size: 4) {
            let size = header.reduce(0) { ($0 << 8) | Int($1) }
            guard size > 0, size < 1024 * 1024,
                  let body = read(child, size: size),
                  let envelope = try? JSONSerialization.jsonObject(with: body) as? [String: Any],
                  let request = envelope["request"] as? [String: Any],
                  let op = request["op"] as? String else { return }
            let mode = lock.withLock { messages.append(request); return behavior }
            if mode == "hold" && op != "hello" {
                while lock.withLock({ !stopped && behavior == "hold" }) { usleep(1000) }
            }
            if mode == "hold_count" && op == "count" {
                while lock.withLock({ !stopped && behavior == "hold_count" }) { usleep(1000) }
            }
            let response: [String: Any]
            if mode == "unsupported" {
                response = ["protocol": 2, "error": ["code": "unsupported", "message": "old daemon"]]
            } else if mode == "domain" && op != "hello" {
                response = ["protocol": 1, "error": ["code": "request_failed", "message": "fixture domain error"]]
            } else {
                response = ["protocol": 1, "result": result(op, request: request, wrongRoot: mode == "wrong_root")]
            }
            guard let data = try? JSONSerialization.data(withJSONObject: response) else { return }
            var length = UInt32(data.count).bigEndian
            var frame = withUnsafeBytes(of: &length) { Data($0) }
            frame.append(data)
            // Split frames deliberately: transport must handle partial reads.
            for part in [frame.prefix(2), frame.dropFirst(2)] {
                var offset = 0
                while offset < part.count {
                    let count = part.withUnsafeBytes { Darwin.send(child, $0.baseAddress!.advanced(by: offset), part.count - offset, 0) }
                    guard count > 0 else { return }
                    offset += count
                }
            }
        }
    }
    private func result(_ op: String, request: [String: Any], wrongRoot: Bool) -> Any {
        let machine = request["machine"] as? String ?? "local"
        switch op {
        case "hello": return ["root": wrongRoot ? "/wrong-root" : root.path,
            "capabilities": ["machines", "projects", "sessions", "count", "search", "session"]]
        case "machines": return [["id": "local", "label": "This Mac"]]
        case "projects": return [["project": "memex", "session_count": 1250]]
        case "count": return ["total": 1250]
        case "search": return [["source": "codex", "session_id": "s1", "source_path": "/s1", "project": "memex", "machine": machine, "record_id": "r1"]]
        case "sessions": return [["source": "codex", "session_id": "s1", "source_path": "/s1", "project": "memex", "machine": machine]]
        case "session":
            var values: [[String: Any]] = [["record_id": "r1", "record": ["role": "user", "text": "hello"]]]
            if request["max_chars"] != nil { values.append(["type": "page", "total": 1, "next_offset": NSNull()]) }
            return values
        default: return []
        }
    }
    private func read(_ child: Int32, size: Int) -> Data? {
        var data = Data(count: size)
        var offset = 0
        while offset < size {
            let count = data.withUnsafeMutableBytes { Darwin.recv(child, $0.baseAddress!.advanced(by: offset), size - offset, 0) }
            guard count > 0 else { return nil }
            offset += count
        }
        return data
    }
}

@Suite(.serialized) struct DaemonClientTests {
    @Test func appOperationsReuseConnectionAndPreserveContracts() async throws {
        let fixture = try SocketFixture()
        defer { fixture.stop() }
        let client = fixture.client
        #expect(try await client.machines() == [.local])
        #expect(try await client.projects().first?.sessionCount == 1250)
        let rows = try await client.sessions(limit: 400, project: "memex", source: "codex", machine: "peer", since: "2026-09-01", origin: .subagent)
        let session = try #require(rows.first)
        #expect(session.machineID == "peer")
        #expect(try await client.sessionDetails(for: session).id == session.id)
        #expect(try await client.sessionCount(query: "--leading query", project: "memex", source: "codex", machine: "peer", since: "2026-09-01", origin: .subagent) == 1250)
        #expect(try await client.search("--leading query", project: "memex", source: "codex", limit: 400, machine: "peer", since: "2026-09-01", origin: .subagent).first?.recordID == "r1")
        #expect(try await client.records(for: session, offset: 0).first?.record.text == "hello")
        #expect(try await client.recordMetadata(for: session, offset: 0, limit: 1).total == 1)
        #expect(fixture.connectionCount == 1)
        #expect(fixture.requests.filter { $0["op"] as? String == "hello" }.count == 1)
        #expect(!fixture.fallbackUsed)
        let listing = try #require(fixture.requests.first { $0["op"] as? String == "sessions" })
        let filters = try #require(listing["filters"] as? [String: Any])
        #expect(filters["limit"] as? Int == 400)
        #expect(filters["origin"] as? String == "subagent")
        #expect(filters["since"] as? String == "2026-09-01")
        #expect(filters["source"] as? String == "codex")
        #expect(filters["project"] as? String == "memex")
        let count = try #require(fixture.requests.first { $0["op"] as? String == "count" })
        #expect(count["query"] as? String == "--leading query")
        let reads = fixture.requests.filter { $0["op"] as? String == "session" }
        #expect(reads[0]["max_chars"] == nil)
        #expect(reads[1]["max_chars"] as? Int == 1)
    }

    @Test func incompatibleWrongRootAndMissingSocketsFallBack() async throws {
        for behavior in ["unsupported", "wrong_root", "missing"] {
            let fixture = try SocketFixture()
            defer { fixture.stop() }
            fixture.setBehavior(behavior)
            if behavior == "missing" { try FileManager.default.removeItem(at: fixture.socketURL) }
            #expect(try await fixture.client.machines().isEmpty)
            #expect(fixture.fallbackUsed)
            #expect(!fixture.requests.contains { $0["op"] as? String == "machines" })
        }
    }

    @Test func domainErrorsAndCancellationDoNotRunCLI() async throws {
        let fixture = try SocketFixture()
        defer { fixture.stop() }
        let client = fixture.client
        fixture.setBehavior("domain")
        do { _ = try await client.machines(); Issue.record("domain error was hidden") }
        catch let error as ClientError { #expect(error.message == "fixture domain error") }
        fixture.setBehavior("hold")
        let pending = Task { try await client.projects() }
        try await fixture.waitFor("projects")
        pending.cancel()
        do { _ = try await pending.value; Issue.record("cancelled request completed") }
        catch is CancellationError { }
        #expect(!fixture.fallbackUsed)
        fixture.setBehavior("normal")
        #expect(try await client.machines() == [.local])
    }

    @Test func deadConnectionFallsBackThenReconnects() async throws {
        let fixture = try SocketFixture()
        defer { fixture.stop() }
        let client = fixture.client
        #expect(try await client.machines() == [.local])
        fixture.disconnect()
        #expect(try await client.machines().isEmpty)
        #expect(fixture.fallbackUsed)
        #expect(try await client.machines() == [.local])
        #expect(fixture.connectionCount == 2)
    }

    @Test func poolWaitUsesTheRequestDeadline() async throws {
        let fixture = try SocketFixture()
        defer { fixture.stop() }
        fixture.setBehavior("hold_count")
        let transport = DaemonClient(root: fixture.root, socket: fixture.socketURL, maximumConnections: 1)
        let pending = Task { try await transport.request(DaemonRequest(op: "count"), timeout: 2) }
        try await fixture.waitFor("count")
        let started = ContinuousClock.now
        #expect(try await transport.request(DaemonRequest(op: "machines"), timeout: 0.05) == nil)
        #expect(started.duration(to: .now) < .seconds(0.5))
        fixture.setBehavior("normal")
        #expect(try await pending.value != nil)
    }

    @Test func slowCountDoesNotBlockOtherRequestsAndTimeoutFallsBack() async throws {
        let fixture = try SocketFixture()
        defer { fixture.stop() }
        let client = fixture.client
        fixture.setBehavior("hold_count")
        let pending = Task { try await client.sessionCount() }
        try await fixture.waitFor("count")
        #expect(try await client.machines() == [.local])
        fixture.setBehavior("normal")
        #expect(try await pending.value == 1250)
        #expect(fixture.connectionCount == 2)
        fixture.setBehavior("hold")
        #expect(try await client.projects(timeout: 0.5).isEmpty)
        #expect(fixture.fallbackUsed)
    }
}

@Test(.enabled(if: ProcessInfo.processInfo.environment["MEMEX_DAEMON_TEST_CLI"] != nil))
func isolatedRustDaemonServesSwiftClientAndReconnects() async throws {
    let executable = try #require(ProcessInfo.processInfo.environment["MEMEX_DAEMON_TEST_CLI"])
    let root = URL(fileURLWithPath: "/private/tmp/memex-e2e-\(UUID().uuidString)")
    let inputs = root.appendingPathComponent("inputs")
    try FileManager.default.createDirectory(at: inputs, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: root) }
    let transcript = #"{"type":"user","uuid":"native-fixture-message","sessionId":"native-fixture","timestamp":"2026-09-07T12:00:00Z","message":{"role":"user","content":"hello persistent native connection"}}"#
    let image = #"{"type":"user","uuid":"native-fixture-image","sessionId":"native-fixture","timestamp":"2026-09-07T12:00:01Z","message":{"role":"user","content":[{"type":"text","text":"<<ImageDisplayed>>\nInspect this image"},{"type":"image","source":{"type":"url","url":"https://example.com/native-fixture.png"}}]}}"#
    let failedTool = #"{"type":"user","uuid":"native-fixture-error","sessionId":"native-fixture","timestamp":"2026-09-07T12:00:02Z","message":{"content":[{"type":"tool_result","tool_use_id":"failed-call","is_error":true,"content":"permission denied"}]}}"#
    let document = #"{"type":"user","uuid":"native-fixture-document","sessionId":"native-fixture","timestamp":"2026-09-07T12:00:03Z","message":{"content":[{"type":"document","title":"Report.pdf","source":{"type":"base64","media_type":"application/pdf","data":"JVBERi0xLjQ="}}]}}"#
    try ([transcript, image, failedTool, document].joined(separator: "\n") + "\n").write(to: inputs.appendingPathComponent("native-fixture.jsonl"), atomically: true, encoding: .utf8)
    let logURL = root.appendingPathComponent("daemon.log")
    FileManager.default.createFile(atPath: logURL.path, contents: nil)
    let log = try FileHandle(forWritingTo: logURL)
    defer { try? log.close() }
    func start() throws -> Process {
        let child = Process()
        child.executableURL = URL(fileURLWithPath: executable)
        // Only this synthetic source is enabled; no user configuration or data.
        child.arguments = ["--no-update-check", "--non-interactive", "daemon", "run", "--root", root.path,
            "--only-source", "claude", "--claude-path", inputs.path, "--no-embeddings", "--no-mcp", "--poll-interval", "3600"]
        child.standardInput = FileHandle.nullDevice
        child.standardOutput = log
        child.standardError = log
        try child.run()
        return child
    }
    var child = try start()
    defer { if child.isRunning { child.terminate(); child.waitUntilExit() } }
    let client = MemexClient(executable: root.appendingPathComponent("missing-cli"), root: root.path,
        daemonSocket: root.appendingPathComponent("state/native/app.sock"))
    func waitForSession() async throws -> Session {
        let deadline = ContinuousClock.now.advanced(by: .seconds(20))
        var lastObservation = "No client request completed"
        while child.isRunning, ContinuousClock.now < deadline {
            do {
                let rows = try await client.sessions(limit: 5)
                if let session = rows.first {
                    let hits = try await client.search("persistent", project: nil, source: nil, limit: 5)
                    if !hits.isEmpty { return session }
                    lastObservation = "Sessions returned \(rows.count) rows, but fixture search returned no matches"
                } else {
                    lastObservation = "Sessions returned no rows"
                }
            } catch {
                lastObservation = "Client request failed: \(error)"
            }
            try await Task.sleep(for: .milliseconds(50))
        }
        let socket = root.appendingPathComponent("state/native/app.sock")
        let socketExists = FileManager.default.fileExists(atPath: socket.path)
        let processStatus = child.isRunning ? "running" : "exited with status \(child.terminationStatus)"
        let daemonLog = (try? String(contentsOf: logURL, encoding: .utf8)) ?? "Log unavailable"
        throw ClientError(message: "Isolated daemon did not publish fixture. Process: \(processStatus). Socket: \(socket.path) (exists: \(socketExists)). \(lastObservation). Daemon log:\n\(daemonLog)")
    }
    let session = try await waitForSession()
    #expect(session.sessionID == "native-fixture")
    #expect(session.messageCount == 4)
    let initial = try await client.initialRecords(for: session, anchor: nil)
    #expect(initial.total == 4)
    #expect(initial.offset == 0)
    #expect(initial.records.count == 4)
    #expect(initial.records.last?.record.sourceContent != nil)
    #expect(try await client.machines() == [.local])
    #expect(try await client.projects().reduce(0) { $0 + $1.sessionCount } == 1)
    #expect(try await client.sessionCount() == 1)
    #expect(try await client.sessionCount(query: "persistent") == 1)
    #expect(try await client.search("persistent", project: nil, source: nil, limit: 1000).first?.sessionID == session.sessionID)
    #expect(try await client.sessionDetails(for: session).id == session.id)
    #expect(try await client.records(for: session, offset: 0).first?.record.text == "hello persistent native connection")
    #expect(try await client.recordMetadata(for: session, offset: 0, limit: 1).total == 4)
    let records = try await client.records(for: session, offset: 0)
    let imageRecord = try #require(records.first { $0.record.eventID == "native-fixture-image" })
    let imageMessage = imageRecord.record
    #expect(imageMessage.sourceContent != nil)
    #expect(SourceContent.blocks(imageMessage).contains(.attachment(label: "Image", source: "https://example.com/native-fixture.png", image: true)))
    #expect(SourceContent.displayText(imageMessage) == "Inspect this image")
    #expect(imageRecord.rawTranscriptBody.contains("ImageDisplayed"))
    let failure = try #require(records.first { $0.record.role == "tool_result" })
    #expect(failure.record.toolResultIsError == true)
    #expect(failure.record.toolOutput == "permission denied")
    #expect(TranscriptActivity(records: [failure]).presentation.hasFailure)
    let documentRecord = try #require(records.first { $0.record.eventID == "native-fixture-document" })
    let documentBlocks = SourceContent.blocks(documentRecord.record)
    #expect(documentBlocks.count == 1)
    if case .attachmentNotice(let label, let detail) = documentBlocks.first {
        #expect(label == "Report.pdf")
        #expect(detail.contains("application/pdf"))
    } else {
        Issue.record("Expected a visible embedded document card")
    }
    #expect(documentRecord.rawTranscriptBody.contains("JVBERi0xLjQ="))
    child.terminate()
    child.waitUntilExit()
    child = try start()
    #expect(try await waitForSession().id == session.id)
    #expect(try await client.sessionCount() == 1)
}
