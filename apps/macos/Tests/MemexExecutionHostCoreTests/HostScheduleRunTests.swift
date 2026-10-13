import XCTest
@testable import MemexExecutionHostCore

final class HostScheduleRunTests: XCTestCase {
    private final class Provider: ExecutionProvider {
        var providers = ["codex"]
        var created: [HostedConversation] = []
        var commands: [HostedCommand] = []
        var failCreation = false
        var failDeliveries = false
        func create(id: String, provider: String, workspaceID: String, cwd: String, title: String) throws -> HostedConversation {
            let value = HostedConversation(id: id, nativeSessionID: "native-" + id, provider: provider,
                providerInstanceID: "test", workspaceID: workspaceID, cwd: cwd, title: title, createdAt: "2026-10-05T12:00:00Z")
            created.append(value)
            if failCreation { throw HostFailure("transport", "Lost creation acknowledgement") }
            return value
        }
        func resume(_ conversation: HostedConversation) throws {}
        func read(_ conversation: HostedConversation) throws -> HostValue {
            .object(["ready": .bool(true), "running": .bool(false), "deliveries": .array(failDeliveries ? commands.map {
                .object(["commandId": .string($0.id), "status": .string("failed"), "error": .string("provider rejected")])
            } : [])])
        }
        func perform(_ command: HostedCommand) throws -> HostValue { commands.append(command); return .bool(true) }
        func isConnected(_ id: String) -> Bool { true }
    }

    private func fixture(_ body: (URL, URL, Provider) throws -> Void) throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        let workspace = root.appendingPathComponent("repo")
        try FileManager.default.createDirectory(at: workspace, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: root) }
        try body(root.appendingPathComponent("host"), workspace.resolvingSymlinksInPath(), Provider())
    }

    private func call(_ host: ExecutionHost, _ method: String, _ params: [String: HostValue] = [:], id: String = UUID().uuidString) throws -> HostValue {
        var fields = params
        fields["hostId"] = .string(host.hostID); fields["commandId"] = .string(id)
        let response = host.handle(.init(method: method, params: fields))
        if let error = response.error { throw error }
        return try XCTUnwrap(response.result)
    }

    func testFreshChatPerRunAndDurableEventDeduplication() throws {
        try fixture { directory, workspace, provider in
            var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
            _ = try call(host, "schedule.upsert", ["scheduleId": .string("event"), "text": .string("Inspect build"),
                "eventName": .string("build.finished"), "notificationPolicy": .string("all"),
                "newConversation": .object(["workspaceId": .string(workspace.path), "provider": .string("codex"), "title": .string("Build review")])])
            _ = try call(host, "schedule.event", ["eventName": .string("build.finished"), "eventId": .string("build-1")])
            try host.tick()
            XCTAssertEqual(provider.created.count, 1)
            XCTAssertEqual(provider.commands.count, 1)
            XCTAssertEqual(provider.created.first?.title, "Build review")
            host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
            _ = try call(host, "schedule.event", ["eventName": .string("build.finished"), "eventId": .string("build-1")])
            _ = try call(host, "schedule.event", ["eventName": .string("build.finished"), "eventId": .string("build-2")])
            try host.tick()
            XCTAssertEqual(provider.created.count, 2)
            XCTAssertEqual(provider.commands.count, 2)
            XCTAssertNotEqual(provider.commands[0].conversationID, provider.commands[1].conversationID)
            XCTAssertEqual(try call(host, "schedule.runs").array.count, 2)
        }
    }

    func testLostCreationAcknowledgementIsInspectableAndNeverReplayed() throws {
        try fixture { directory, workspace, provider in
            var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
            _ = try call(host, "schedule.upsert", ["scheduleId": .string("hourly"), "text": .string("Inspect"), "intervalSeconds": .number(3600),
                "newConversation": .object(["workspaceId": .string(workspace.path), "provider": .string("codex")])])
            provider.failCreation = true
            _ = try call(host, "schedule.run", ["scheduleId": .string("hourly")], id: "once")
            host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
            _ = try call(host, "schedule.run", ["scheduleId": .string("hourly")], id: "once")
            try host.tick()
            XCTAssertEqual(provider.created.count, 1)
            XCTAssertTrue(provider.commands.isEmpty)
            let run = try XCTUnwrap(call(host, "schedule.runs").array.first)
            XCTAssertEqual(run["status"].string, "uncertain")
            XCTAssertEqual(run["needsAttention"].bool, true)
            _ = try call(host, "schedule.run.read", ["runId": run["id"]])
            XCTAssertEqual(try call(host, "schedule.runs").array.first?["needsAttention"].bool, false)
        }
    }

    func testRemovedWorkspaceGrantPreventsFreshChatRun() throws {
        try fixture { directory, workspace, provider in
            var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
            _ = try call(host, "schedule.upsert", ["scheduleId": .string("hourly"), "text": .string("Inspect"), "intervalSeconds": .number(3600),
                "newConversation": .object(["workspaceId": .string(workspace.path), "provider": .string("codex")])])
            host = try ExecutionHost(directory: directory, workspaceRoots: []) { _ in provider }
            _ = try call(host, "schedule.run", ["scheduleId": .string("hourly")])
            XCTAssertTrue(provider.created.isEmpty)
            XCTAssertNotNil(try call(host, "schedule.runs").array.first?["error"].string)
        }
    }

    func testRunCompletionRequiresItsOwnNativeTurnAndHonorsNotificationPolicy() {
        var run = HostScheduleRun(id: "command", scheduleID: "s", createdAt: .now, updatedAt: .now,
                                  status: "queued", notificationPolicy: "all")
        let otherTurn: HostValue = .object(["nativeTurnId": .string("other"), "status": .string("completed")])
        var snapshot: [String: HostValue] = ["deliveries": .array([.object(["commandId": .string("command"), "status": .string("completed"), "nativeTurnId": .string("mine")])]),
            "thread": .object(["turns": .array([otherTurn])])]
        run.reconcile(queueStatus: "dispatched", snapshot: .object(snapshot), at: .now)
        XCTAssertEqual(run.status, "dispatched")
        XCTAssertFalse(run.needsAttention)
        snapshot["thread"] = .object(["turns": .array([otherTurn, .object(["nativeTurnId": .string("mine"), "status": .string("completed")])])])
        run.reconcile(queueStatus: "dispatched", snapshot: .object(snapshot), at: .now)
        XCTAssertEqual(run.status, "completed")
        XCTAssertTrue(run.needsAttention)
        run.notificationPolicy = "attention"
        XCTAssertFalse(run.needsAttention)
        run.status = "failed"
        XCTAssertTrue(run.needsAttention)
        run.notificationPolicy = "never"
        XCTAssertFalse(run.needsAttention)
    }

    func testRunsListIsBoundedNewestFirstAndFinishedReadRunsExpire() throws {
        try fixture { directory, workspace, provider in
            var current = Date(timeIntervalSince1970: 1_800_000_000)
            let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace], providerFactory: { _ in provider }, now: { current })
            provider.failDeliveries = true
            _ = try call(host, "schedule.upsert", ["scheduleId": .string("event"), "text": .string("Inspect"), "eventName": .string("build"),
                "newConversation": .object(["workspaceId": .string(workspace.path), "provider": .string("codex")])])
            for event in ["one", "two", "three"] {
                _ = try call(host, "schedule.event", ["eventName": .string("build"), "eventId": .string(event)])
            }
            try host.tick(); try host.tick()
            let runs = try call(host, "schedule.runs").array
            XCTAssertEqual(runs.map { $0["status"].string }, ["failed", "failed", "failed"])
            XCTAssertTrue(runs.first?["id"].string?.hasSuffix("three") == true)
            XCTAssertEqual(try call(host, "schedule.runs", ["limit": .number(2)]).array.map(\.["id"]), Array(runs.prefix(2)).map(\.["id"]))
            XCTAssertEqual(try call(host, "schedule.runs", ["since": .string(current.addingTimeInterval(-1).ISO8601Format())]).array.count, 3)
            XCTAssertTrue(try call(host, "schedule.runs", ["since": .string(current.addingTimeInterval(1).ISO8601Format())]).array.isEmpty)
            XCTAssertThrowsError(try call(host, "schedule.runs", ["limit": .number(0)]))
            for run in runs.dropFirst() { _ = try call(host, "schedule.run.read", ["runId": run["id"]]) }
            current = current.addingTimeInterval(31 * 24 * 60 * 60)
            _ = try call(host, "schedule.pause", ["scheduleId": .string("event")])
            // Read, finished runs age out; the unread failure stays in the inbox.
            XCTAssertEqual(try call(host, "schedule.runs").array.map(\.["id"]), [runs[0]["id"]])
        }
    }
}
