import Foundation
import Network
import Testing
import MemexExecutionHostCore
@testable import Memex

struct ExecutionHostAdapterTests {
    @Test func lostAcknowledgementRetainsExactCommandUntilExplicitRetry() async throws {
        let root = try temporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let fixture = try ExecutionHTTPFixture { request, ordinal in
            if ordinal == 1 { return nil }
            return HostResponse(id: request.id, result: .object(["accepted": .bool(true)]))
        }
        defer { fixture.stop() }
        let endpoint = try await fixture.endpoint()
        let client = try ExecutionHostHTTPClient(endpoint: endpoint, token: "fixture-token", hostID: "fixture-host", outbox: root)
        let fields: [String: HostValue] = ["commandId": .string("original-command"), "issuedAt": .string("2026-10-05T12:00:00Z"),
            "conversationId": .string("original-conversation"), "text": .string("Execute once")]
        do {
            _ = try await client.call("conversation.send", params: fields, mutation: true)
            Issue.record("A closed acknowledgement connection must not report success")
        } catch let error as HostFailure { #expect(error.code == "delivery_unknown") }
        let pending = try await client.pending()
        #expect(pending.count == 1)
        let request = try #require(pending.first)
        #expect(request.params["commandId"] == .string("original-command"))
        #expect(request.params["hostId"] == .string("fixture-host"))
        #expect(request.params["issuedAt"] == fields["issuedAt"])
        let retried = try await client.call(request.method, params: request.params, mutation: true)
        #expect(retried["accepted"].bool == true)
        #expect(try await client.pending().isEmpty)
        #expect(fixture.requests.count == 2)
        #expect(fixture.requests[0].params == fixture.requests[1].params)
        #expect(fixture.authorizations == ["Bearer fixture-token", "Bearer fixture-token"])
    }

    @Test func remoteIdentityMismatchNeverResumesOrSendsToAnotherTranscript() async throws {
        let root = try temporaryRoot()
        defer { try? FileManager.default.removeItem(at: root) }
        let fixture = try ExecutionHTTPFixture { request, _ in
            HostResponse(id: request.id, result: .object(["conversation": .object([
                "nativeSessionID": .string("native-session"), "provider": .string("codex"),
                "workspaceID": .string("/remote/workspace"), "cwd": .string("/remote/workspace"),
                "transcriptPath": .string("/another-home/sessions/native-session.jsonl")
            ])]))
        }
        defer { fixture.stop() }
        let connection = ExecutionHostConnection(id: "fixture-host", name: "Fixture", machineID: "remote-machine",
            endpoint: try await fixture.endpoint())
        let session = Session(source: "codex", sessionID: "native-session", sourcePath: "/original-home/sessions/native-session.jsonl",
            project: "workspace", cwd: "/remote/workspace", machine: "remote-machine")
        let target = try RemoteConversationRuntime.target(for: session, connection: connection)
        let runtime = try RemoteConversationRuntime(connection: connection, token: "fixture-token", conversationID: "hosted-conversation", outbox: root)
        do {
            try await runtime.connect(target) { _ in }
            Issue.record("A different transcript must not resume under the requested native session identity")
        } catch let error as ConversationRuntimeError { #expect(error.message.contains("does not match")) }
        await runtime.disconnect()
        #expect(fixture.requests.map(\.method) == ["conversation.read"])
        #expect(fixture.requests.first?.params["hostId"] == .string("fixture-host"))
    }

    @MainActor @Test func desktopBrowserBridgeRequiresConversationGrantAndMatchingRequestIdentity() async throws {
        // Keep the Unix domain socket path below the Darwin sockaddr_un limit.
        let root = URL(fileURLWithPath: "/tmp/mxb-" + UUID().uuidString.prefix(8))
        let directory = root.appendingPathComponent("state/execution")
        defer { try? FileManager.default.removeItem(at: root) }
        let automation = WorkspaceBrowserAutomationHost()
        let suite = "memex-browser-bridge-" + UUID().uuidString
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let group = WorkspaceBrowserTabs(conversationID: "native-conversation", defaults: defaults)
        automation.register(group)
        defer { group.discard() }
        let bridge = WorkspaceBrowserExecutionBridge()
        try bridge.start(root: root, automation: automation)
        defer { bridge.stop() }
        func request(_ method: String, _ params: [String: HostValue]) async throws -> HostResponse {
            try await Task.detached {
                try HostSocketClient.request(HostRequest(method: method, params: params), directory: directory)
            }.value
        }
        let denied = try await request("browser.describe", ["conversationId": .string("native-conversation")])
        #expect(denied.error?.code == "browser_denied")
        let grant = try automation.allow(conversationID: "native-conversation", capabilities: [.snapshot])
        let described = try await request("browser.describe", ["conversationId": .string("native-conversation")])
        #expect(described.error == nil)
        #expect(described.result?["grant"]["id"].string == grant.id.uuidString)
        #expect(described.result?["conversationID"].string == "native-conversation")
        let another = WorkspaceBrowserAutomationRequest(hostID: automation.id, grantID: grant.id,
            conversationID: "another-conversation", tabID: group.selectedID, action: .snapshot)
        let mismatch = try await request("browser.dispatch", ["conversationId": .string("native-conversation"), "request": try .encoded(another)])
        #expect(mismatch.error?.code == "browser_scope")
        automation.revoke(conversationID: "native-conversation")
        let revoked = try await request("browser.describe", ["conversationId": .string("native-conversation")])
        #expect(revoked.error?.code == "browser_denied")
    }

    private func temporaryRoot() throws -> URL {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-adapter-" + UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }
}

/// A loopback HTTP server exercises URLSession's actual framing and disconnect
/// behavior. It receives complete POST bodies before optionally dropping an ack.
private final class ExecutionHTTPFixture: @unchecked Sendable {
    private let listener: NWListener
    private let queue = DispatchQueue(label: "memex.execution-http-fixture")
    private let lock = NSLock()
    private var boundPort: UInt16?
    private var received: [HostRequest] = []
    private var headers: [String] = []
    private let handler: @Sendable (HostRequest, Int) throws -> HostResponse?
    var requests: [HostRequest] { lock.withLock { received } }
    var authorizations: [String] { lock.withLock { headers } }

    init(handler: @escaping @Sendable (HostRequest, Int) throws -> HostResponse?) throws {
        self.handler = handler
        let parameters = NWParameters.tcp
        parameters.requiredLocalEndpoint = .hostPort(host: .ipv4(.loopback), port: .any)
        listener = try NWListener(using: parameters)
        listener.stateUpdateHandler = { [weak self] state in
            guard let self, case .ready = state else { return }
            self.lock.withLock { self.boundPort = self.listener.port?.rawValue }
        }
        listener.newConnectionHandler = { [weak self] connection in
            guard let self else { connection.cancel(); return }
            connection.start(queue: self.queue)
            self.receive(connection, accumulated: Data())
        }
        listener.start(queue: queue)
    }

    func endpoint() async throws -> URL {
        for _ in 0..<200 {
            if let port = lock.withLock({ boundPort }) { return URL(string: "http://127.0.0.1:\(port)/api/control")! }
            try await Task.sleep(for: .milliseconds(10))
        }
        throw HostFailure("fixture", "Loopback execution fixture did not bind")
    }

    func stop() { listener.cancel() }

    private func receive(_ connection: NWConnection, accumulated: Data) {
        connection.receive(minimumIncompleteLength: 1, maximumLength: 65_536) { [weak self] data, _, complete, error in
            guard let self, let data, error == nil else { connection.cancel(); return }
            let combined = accumulated + data
            guard combined.count <= 4 * 1024 * 1024 else { connection.cancel(); return }
            guard let separator = combined.range(of: Data("\r\n\r\n".utf8)) else {
                if complete { connection.cancel() } else { self.receive(connection, accumulated: combined) }
                return
            }
            let lines = String(decoding: combined[..<separator.lowerBound], as: UTF8.self).components(separatedBy: "\r\n")
            let length = lines.first { $0.lowercased().hasPrefix("content-length:") }
                .flatMap { Int($0.dropFirst("content-length:".count).trimmingCharacters(in: .whitespaces)) } ?? 0
            guard combined.count - separator.upperBound >= length else {
                if complete { connection.cancel() } else { self.receive(connection, accumulated: combined) }
                return
            }
            do {
                let body = combined.subdata(in: separator.upperBound..<(separator.upperBound + length))
                let request = try JSONDecoder().decode(HostRequest.self, from: body)
                let ordinal = self.lock.withLock {
                    self.received.append(request)
                    let authorization = lines.first { $0.lowercased().hasPrefix("authorization:") }
                        .map { String($0.dropFirst("authorization:".count)).trimmingCharacters(in: .whitespaces) } ?? ""
                    self.headers.append(authorization)
                    return self.received.count
                }
                guard let result = try self.handler(request, ordinal) else { connection.cancel(); return }
                let json = try JSONEncoder().encode(result)
                var response = Data("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: \(json.count)\r\nConnection: close\r\n\r\n".utf8)
                response.append(json)
                connection.send(content: response, completion: .contentProcessed { _ in connection.cancel() })
            } catch { connection.cancel() }
        }
    }
}
