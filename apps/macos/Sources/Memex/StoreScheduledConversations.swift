import Foundation
import MemexExecutionHostCore

extension Store {
    func openScheduledConversation(hostID: String, conversationID: String) async {
        executionHostSelection = hostID
        guard let host = executionHosts.hosts.first(where: { $0.id == hostID }) else {
            executionHostError = "Pair the schedule's execution host again to open this run."
            showingExecutionHosts = true
            return
        }
        do {
            let client = try executionHosts.client(host)
            let info = try await client.call("host.info")
            guard info["hostId"].string == hostID else { throw HostFailure("wrong_host", "The schedule's execution identity changed. Pair it again explicitly.") }
            let result = try await client.call("conversation.read", params: ["conversationId": .string(conversationID)])
            let value = result["conversation"]
            guard value["id"].string == conversationID else { throw HostFailure("identity_conflict", "The host returned a different scheduled conversation") }
            openHostedConversation(try RemoteConversationRuntime.session(value, connection: host), connection: host, conversationID: conversationID)
        } catch {
            executionHostError = error.localizedDescription
            showingExecutionHosts = true
        }
    }
}
