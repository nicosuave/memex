import XCTest
@testable import MemexExecutionHostCore

final class ExecutionHostTests: XCTestCase {
    private final class Provider: ExecutionProvider {
        var providers = ["codex"]
        var created: [HostedConversation] = []
        var commands: [HostedCommand] = []
        var resumed: [HostedConversation] = []
        var connected = true
        var running = false
        var rejectAfterAcceptance = false
        var sequence: Double = 1
        var permissionMode: String?
        var pendingPermission = false
        func create(id: String, provider: String, workspaceID: String, cwd: String, title: String) throws -> HostedConversation {
            let c = HostedConversation(id: id, nativeSessionID: "native-" + id, provider: provider,
                providerInstanceID: "codex:test-home", workspaceID: workspaceID, cwd: cwd,
                title: title, createdAt: "2026-10-05T12:00:00Z")
            created.append(c); return c
        }
        func resume(_ conversation: HostedConversation) throws { resumed.append(conversation); connected = true }
        func read(_ conversation: HostedConversation) throws -> HostValue {
            .object(["ready": .bool(connected), "running": .bool(running), "connected": .bool(connected),
                "controls": .object([
                    "pendingControlCommandIds": .array(pendingPermission ? [.string("pending")] : []),
                    "configOptions": .array(permissionMode.map { mode in [.object([
                        "id": .string("permission_mode"), "currentValue": .string(mode)
                    ])] } ?? [])
                ]),
                "thread": .object(["snapshotSequence": .number(sequence), "messages": .array([])])])
        }
        func perform(_ command: HostedCommand) throws -> HostValue {
            commands.append(command)
            sequence += 1
            if rejectAfterAcceptance { throw HostFailure("transport", "acknowledgement lost") }
            running = command.action != "cancel"
            return .object(["receipt": .object(["commandId": .string(command.id), "accepted": .bool(true)])])
        }
        func isConnected(_ id: String) -> Bool { connected }
    }

    private func fixture() throws -> (URL, URL) {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent("memex-host-test-" + UUID().uuidString)
        let workspace = root.appendingPathComponent("workspace")
        try FileManager.default.createDirectory(at: workspace, withIntermediateDirectories: true)
        addTeardownBlock { try? FileManager.default.removeItem(at: root) }
        return (root.appendingPathComponent("execution"), workspace.resolvingSymlinksInPath())
    }

    private func call(_ host: ExecutionHost, _ method: String, _ fields: [String: HostValue] = [:],
                      commandID: String = UUID().uuidString) -> HostResponse {
        var params = fields
        params["hostId"] = .string(host.hostID)
        params["commandId"] = .string(commandID)
        params["issuedAt"] = .string("2026-10-05T12:00:00Z")
        return host.handle(HostRequest(id: .string("request"), method: method, params: params))
    }

    private func create(_ host: ExecutionHost, _ workspace: URL) throws -> String {
        let response = call(host, "conversation.create", ["provider": .string("codex"), "workspaceId": .string(workspace.path)])
        XCTAssertNil(response.error)
        return try XCTUnwrap(response.result?["conversation"]["id"].string)
    }

    func testClaudeResumePassesSelectedNativePermissionsWithoutChangingAnActiveSession() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        provider.providers = ["claude"]
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let created = call(host, "conversation.create", ["provider": .string("claude"), "workspaceId": .string(workspace.path)])
        let id = try XCTUnwrap(created.result?["conversation"]["id"].string)
        for mode in ["auto", "default", "acceptEdits"] {
            provider.connected = false
            let result = call(host, "conversation.resume", ["conversationId": .string(id), "claudePermissionMode": .string(mode)])
            XCTAssertNil(result.error)
            XCTAssertEqual(provider.resumed.last?.claudePermissionMode, mode)
        }
        XCTAssertNil(call(host, "conversation.resume", ["conversationId": .string(id), "claudePermissionMode": .string("auto")]).error)
        XCTAssertEqual(provider.resumed.count, 3)
        provider.connected = false
        XCTAssertEqual(call(host, "conversation.resume", ["conversationId": .string(id), "claudePermissionMode": .string("invalid")]).error?.code, "invalid_params")
        XCTAssertEqual(provider.resumed.count, 3)
    }

    func testClaudeAcknowledgedPermissionsSurviveHeadlessHostReopen() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        provider.providers = ["claude"]
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let created = call(host, "conversation.create", ["provider": .string("claude"), "workspaceId": .string(workspace.path)])
        let id = try XCTUnwrap(created.result?["conversation"]["id"].string)
        provider.permissionMode = "default"
        provider.pendingPermission = true
        XCTAssertNil(call(host, "conversation.read", ["conversationId": .string(id)]).result?["conversation"]["claudePermissionMode"].string)
        provider.pendingPermission = false
        XCTAssertEqual(call(host, "conversation.read", ["conversationId": .string(id)]).result?["conversation"]["claudePermissionMode"].string, "default")
        provider.connected = false
        let reopened = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        XCTAssertNil(call(reopened, "conversation.resume", ["conversationId": .string(id)]).error)
        XCTAssertEqual(provider.resumed.last?.claudePermissionMode, "default")
    }

    func testDuplicateCommandsPreserveNativeIdentityAndExecuteOnce() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        let params: [String: HostValue] = ["conversationId": .string(id), "text": .string("hello")]
        let first = call(host, "conversation.send", params, commandID: "same-command")
        let repeated = call(host, "conversation.send", params, commandID: "same-command")
        XCTAssertEqual(first.result, repeated.result)
        XCTAssertEqual(provider.commands.count, 1)
        XCTAssertEqual(provider.commands[0].id, "same-command")
        XCTAssertEqual(provider.created[0].nativeSessionID, "native-" + id)
        let conflict = call(host, "conversation.send", ["conversationId": .string(id), "text": .string("changed")], commandID: "same-command")
        XCTAssertEqual(conflict.error?.code, "id_conflict")
        XCTAssertEqual(provider.commands.count, 1)
    }

    func testUnknownHostAndUnregisteredWorkspaceCannotExecute() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let wrongHost = host.handle(.init(method: "conversation.create", params: ["hostId": .string("other")] ))
        XCTAssertEqual(wrongHost.error?.code, "wrong_host")
        let forbidden = call(host, "conversation.create", ["provider": .string("codex"), "workspaceId": .string("/private")])
        XCTAssertEqual(forbidden.error?.code, "workspace_denied")
        XCTAssertTrue(provider.created.isEmpty)
    }

    func testDesktopControlOperationsAreUnavailable() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        let methods = ["desktop.describe", "desktop.dispatch", "desktop.panel", "desktop.preferences", "desktop.organization"]
        for method in methods {
            let response = call(host, method, ["conversationId": .string(id)])
            XCTAssertEqual(response.error?.code, "method_not_found", method)
        }
        let capabilities = try XCTUnwrap(call(host, "host.info").result?["capabilities"].array)
        XCTAssertFalse(capabilities.contains(.string("desktop.controls")))
        XCTAssertTrue(provider.commands.isEmpty)
    }

    func testRestartHoldsQueueAndNeverReplaysUncertainDelivery() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        let params: [String: HostValue] = ["conversationId": .string(id), "text": .string("only once")]
        XCTAssertNil(call(host, "conversation.queue.add", params, commandID: "uncertain-send").error)
        provider.rejectAfterAcceptance = true
        try host.tick()
        XCTAssertEqual(provider.commands.count, 1)
        provider.rejectAfterAcceptance = false
        host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        XCTAssertNil(call(host, "conversation.queue.resume", ["conversationId": .string(id)]).error)
        try host.tick()
        XCTAssertEqual(provider.commands.count, 1)
        let queue = call(host, "conversation.queue.list", ["conversationId": .string(id)]).result?.array
        XCTAssertEqual(queue?.first?["status"].string, "uncertain")
    }

    func testUndispatchedQueueHeldAfterRestartUntilExplicitResume() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        XCTAssertNil(call(host, "conversation.queue.add", ["conversationId": .string(id), "text": .string("later")]).error)
        host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        try host.tick()
        XCTAssertTrue(provider.commands.isEmpty)
        XCTAssertNil(call(host, "conversation.queue.resume", ["conversationId": .string(id)]).error)
        try host.tick()
        XCTAssertEqual(provider.commands.count, 1)
    }

    func testStopHoldsQueuedWorkEvenWhenProviderRejectsInterrupt() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        _ = call(host, "conversation.queue.add", ["conversationId": .string(id), "text": .string("later")])
        provider.rejectAfterAcceptance = true
        XCTAssertNotNil(call(host, "conversation.interrupt", ["conversationId": .string(id)]).error)
        provider.rejectAfterAcceptance = false
        try host.tick()
        XCTAssertEqual(provider.commands.map(\.action), ["cancel"])
        XCTAssertEqual(call(host, "conversation.read", ["conversationId": .string(id)]).result?["queueHeld"], .bool(true))
    }

    func testScheduleCoalescesMissedRunsAndRunNowUsesSameQueue() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        var current = Date(timeIntervalSince1970: 1_800_000_000)
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace], providerFactory: { _ in provider }, now: { current })
        let id = try create(host, workspace)
        let scheduled = call(host, "schedule.upsert", ["scheduleId": .string("hourly"), "conversationId": .string(id),
            "text": .string("check"), "intervalSeconds": .number(3600)])
        XCTAssertNil(scheduled.error)
        XCTAssertNotNil(scheduled.result?["nextRunAt"].string)
        current = current.addingTimeInterval(36_000)
        try host.tick()
        try host.tick()
        XCTAssertEqual(provider.commands.count, 1)
        XCTAssertTrue(provider.commands[0].id.hasPrefix("schedule-hourly-"))
        _ = call(host, "schedule.pause", ["scheduleId": .string("hourly"), "paused": .bool(true)])
        current = current.addingTimeInterval(36_000)
        provider.running = false
        try host.tick()
        XCTAssertEqual(provider.commands.count, 1)
        let run = call(host, "schedule.run", ["scheduleId": .string("hourly")], commandID: "manual-run")
        XCTAssertNil(run.error)
        _ = call(host, "schedule.run", ["scheduleId": .string("hourly")], commandID: "manual-run")
        try host.tick()
        XCTAssertEqual(provider.commands.count, 2)
        XCTAssertEqual(provider.commands.last?.id, "schedule-manual-manual-run")
    }

    func testHostCatalogAndReceiptsSurviveReopenWithoutRecreatingProviderSession() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let hostID = host.hostID
        let request: [String: HostValue] = ["provider": .string("codex"), "workspaceId": .string(workspace.path)]
        let first = call(host, "conversation.create", request, commandID: "create-once")
        host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        XCTAssertEqual(hostID, host.hostID)
        XCTAssertEqual(call(host, "conversation.create", request, commandID: "create-once").result, first.result)
        XCTAssertEqual(provider.created.count, 1)
    }

    func testEditingQueuedRichPromptUpdatesDeliveredTextAndPreservesAttachment() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        let attachment: HostValue = .object(["type": .string("image"), "data": .string("aGVsbG8="), "mimeType": .string("image/png")])
        XCTAssertNil(call(host, "conversation.queue.add", ["conversationId": .string(id), "text": .string("original"),
            "promptContent": .array([.object(["type": .string("text"), "text": .string("original")]), attachment])], commandID: "queued-rich").error)
        XCTAssertNil(call(host, "conversation.queue.edit", ["conversationId": .string(id), "queuedCommandId": .string("queued-rich"),
            "text": .string("revised")]).error)
        try host.tick()
        XCTAssertEqual(provider.commands.count, 1)
        XCTAssertEqual(provider.commands[0].text, "revised")
        XCTAssertEqual(provider.commands[0].promptContent?.array.first?["text"].string, "revised")
        XCTAssertEqual(provider.commands[0].promptContent?.array.last, attachment)
    }

    func testQueuePromotionCannotUseWorkspaceWhoseGrantWasRemoved() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        XCTAssertNil(call(host, "conversation.queue.add", ["conversationId": .string(id), "text": .string("later")], commandID: "queued").error)
        host = try ExecutionHost(directory: directory, workspaceRoots: []) { _ in provider }
        let response = call(host, "conversation.queue.promote", ["conversationId": .string(id), "queuedCommandId": .string("queued")])
        XCTAssertEqual(response.error?.code, "workspace_denied")
        XCTAssertTrue(provider.commands.isEmpty)
    }

    func testQueuePromotionCannotFollowReplacedWorkspaceSymlink() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        XCTAssertNil(call(host, "conversation.queue.add", ["conversationId": .string(id), "text": .string("later")], commandID: "queued").error)
        let original = workspace.deletingLastPathComponent().appendingPathComponent("original-workspace")
        let replacement = workspace.deletingLastPathComponent().appendingPathComponent("unregistered-workspace")
        try FileManager.default.moveItem(at: workspace, to: original)
        try FileManager.default.createDirectory(at: replacement, withIntermediateDirectories: true)
        try FileManager.default.createSymbolicLink(at: workspace, withDestinationURL: replacement)
        let response = call(host, "conversation.queue.promote", ["conversationId": .string(id), "queuedCommandId": .string("queued")])
        XCTAssertEqual(response.error?.code, "workspace_denied")
        XCTAssertTrue(provider.commands.isEmpty)
    }

    func testUncertainPromotionHoldsRemainingQueue() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        for command in ["promoted", "later"] {
            XCTAssertNil(call(host, "conversation.queue.add", ["conversationId": .string(id), "text": .string(command)], commandID: command).error)
        }
        provider.rejectAfterAcceptance = true
        XCTAssertNotNil(call(host, "conversation.queue.promote", ["conversationId": .string(id), "queuedCommandId": .string("promoted")]).error)
        provider.rejectAfterAcceptance = false
        try host.tick()
        XCTAssertEqual(provider.commands.map(\.id), ["promoted"])
        XCTAssertEqual(call(host, "conversation.read", ["conversationId": .string(id)]).result?["queueHeld"], .bool(true))
    }

    func testScheduleAcceptsFractionalRFC3339AndRejectsInvalidDate() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace]) { _ in provider }
        let id = try create(host, workspace)
        var parameters: [String: HostValue] = ["scheduleId": .string("daily"), "conversationId": .string(id),
            "text": .string("check"), "intervalSeconds": .number(86_400), "nextRunAt": .string("2026-10-06T12:00:00.500Z")]
        let valid = call(host, "schedule.upsert", parameters)
        XCTAssertNil(valid.error)
        XCTAssertEqual(valid.result?["nextRunAt"].string, "2026-10-06T12:00:00Z")
        parameters["nextRunAt"] = .string("tomorrow sometime")
        XCTAssertEqual(call(host, "schedule.upsert", parameters).error?.code, "invalid_params")
    }

    func testWallClockScheduleSkipsMissedRunsAndPersistsNextOccurrenceAcrossRestart() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        var current = try XCTUnwrap(ISO8601DateFormatter().date(from: "2026-10-05T08:00:00Z"))
        var host = try ExecutionHost(directory: directory, workspaceRoots: [workspace], providerFactory: { _ in provider }, now: { current })
        let id = try create(host, workspace)
        let parameters: [String: HostValue] = ["scheduleId": .string("weekday"), "conversationId": .string(id), "text": .string("morning check"),
            "wallClock": .object(["localTime": .string("09:00"), "weekdays": .array([1, 2, 3, 4, 5].map { .number(Double($0)) }), "timeZone": .string("UTC")])]
        XCTAssertEqual(call(host, "schedule.upsert", parameters).result?["nextRunAt"].string, "2026-10-05T09:00:00Z")
        current = try XCTUnwrap(ISO8601DateFormatter().date(from: "2026-10-06T08:00:00Z"))
        host = try ExecutionHost(directory: directory, workspaceRoots: [workspace], providerFactory: { _ in provider }, now: { current })
        try host.tick()
        XCTAssertTrue(provider.commands.isEmpty)
        let skipped = call(host, "schedule.list").result?.array.first
        XCTAssertEqual(skipped?["lastSkippedAt"].string, "2026-10-05T09:00:00Z")
        XCTAssertEqual(skipped?["nextRunAt"].string, "2026-10-06T09:00:00Z")
        current = try XCTUnwrap(ISO8601DateFormatter().date(from: "2026-10-06T09:00:00Z"))
        try host.tick()
        try host.tick()
        XCTAssertEqual(provider.commands.count, 1)
        XCTAssertEqual(provider.commands.first?.text, "morning check")
        XCTAssertEqual(call(host, "schedule.list").result?.array.first?["nextRunAt"].string, "2026-10-07T09:00:00Z")
    }

    func testEditingPromptKeepsDueTimeAndPauseForEitherRecurrence() throws {
        let (directory, workspace) = try fixture()
        let provider = Provider()
        var current = Date(timeIntervalSince1970: 1_800_000_000)
        let host = try ExecutionHost(directory: directory, workspaceRoots: [workspace], providerFactory: { _ in provider }, now: { current })
        let id = try create(host, workspace)
        var parameters: [String: HostValue] = ["scheduleId": .string("interval"), "conversationId": .string(id), "text": .string("original"),
            "intervalSeconds": .number(3600), "paused": .bool(true)]
        let original = call(host, "schedule.upsert", parameters).result
        current = current.addingTimeInterval(120)
        parameters["text"] = .string("revised")
        parameters.removeValue(forKey: "paused")
        let edited = call(host, "schedule.upsert", parameters).result
        XCTAssertEqual(edited?["nextRunAt"], original?["nextRunAt"])
        XCTAssertEqual(edited?["paused"], .bool(true))
        parameters.removeValue(forKey: "intervalSeconds")
        parameters["wallClock"] = .object(["localTime": .string("09:00"), "weekdays": .array([.number(1)]), "timeZone": .string("UTC")])
        let weekly = call(host, "schedule.upsert", parameters).result
        current = current.addingTimeInterval(120)
        parameters["text"] = .string("revised again")
        XCTAssertEqual(call(host, "schedule.upsert", parameters).result?["nextRunAt"], weekly?["nextRunAt"])
    }
}
