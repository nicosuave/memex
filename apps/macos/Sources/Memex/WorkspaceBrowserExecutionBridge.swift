import Foundation
import MemexExecutionHostCore

/// Local-only attachment of explicitly granted, app-owned browser tabs. Provider
/// commands never obtain authority over arbitrary system applications.
@MainActor
final class WorkspaceBrowserExecutionBridge {
    private var server: HostSocketServer?
    private var task: Task<Void, Never>?

    func start(root: URL, automation: WorkspaceBrowserAutomationHost) throws {
        guard server == nil else { return }
        let server = try HostSocketServer(directory: root.appendingPathComponent("state/execution"), endpointName: "desktop")
        self.server = server
        task = Task.detached(priority: .utility) {
            do {
                try server.run { request in
                    let reply = BrowserBridgeReply()
                    let operation = Task { @MainActor in
                        guard !Task.isCancelled else { return }
                        do {
                            guard let conversationID = request.params["conversationId"]?.string else {
                                throw HostFailure("browser_scope", "Browser request requires an exact conversation identity")
                            }
                            let result: HostValue
                            switch request.method {
                            case "browser.describe":
                                guard let descriptor = automation.descriptor(conversationID: conversationID) else {
                                    throw HostFailure("browser_denied", "Open this conversation's browser and explicitly grant agent access before using its desktop bridge")
                                }
                                result = try .encoded(descriptor)
                            case "browser.dispatch":
                                guard let value = request.params["request"] else { throw HostFailure("invalid_params", "Browser request payload is required") }
                                let action = try JSONDecoder().decode(WorkspaceBrowserAutomationRequest.self, from: JSONEncoder().encode(value))
                                guard action.conversationID == conversationID else { throw HostFailure("browser_scope", "Browser grant does not belong to the requested conversation") }
                                result = try .encoded(await automation.dispatch(action))
                            default: throw HostFailure("method_not_found", "The desktop bridge only exposes scoped browser operations")
                            }
                            reply.complete(HostResponse(id: request.id, result: result))
                        } catch let error as HostFailure { reply.complete(HostResponse(id: request.id, error: error)) }
                        catch { reply.complete(HostResponse(id: request.id, error: HostFailure("browser", error.localizedDescription))) }
                    }
                    guard let response = reply.wait(seconds: 20) else {
                        operation.cancel()
                        return HostResponse(id: request.id, error: HostFailure("delivery_unknown", "The desktop did not acknowledge the action. It may have executed; inspect the page before retrying."))
                    }
                    return response
                }
            } catch { FileHandle.standardError.write(Data("Browser execution bridge stopped: \(error.localizedDescription)\n".utf8)) }
        }
    }

    func stop() { server?.stop(); server = nil; task?.cancel(); task = nil }
}

private final class BrowserBridgeReply: @unchecked Sendable {
    private let condition = NSCondition()
    private var response: HostResponse?
    func complete(_ value: HostResponse) {
        condition.lock(); defer { condition.unlock() }
        response = value; condition.broadcast()
    }
    func wait(seconds: TimeInterval) -> HostResponse? {
        condition.lock(); defer { condition.unlock() }
        let deadline = Date().addingTimeInterval(seconds)
        while response == nil { if !condition.wait(until: deadline) { return nil } }
        return response
    }
}
