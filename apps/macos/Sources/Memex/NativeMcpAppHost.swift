import AppKit
import SwiftUI
#if canImport(SQMcpAppsUI)
import SQMcpApps
import SQMcpAppsUI
#endif

/// Metadata is presentation, never authority. The service revalidates the native
/// call against its live connection before each resource read and tool call.
struct NativeMcpAppDescriptor: Equatable {
    let json: String
    let toolCallID: String
    let allowsInteraction: Bool

    static func decode(_ json: String, expectedCallID: String?) -> Self? {
        guard let object = try? JSONSerialization.jsonObject(with: Data(json.utf8)) as? [String: Any],
              let callID = object["toolCallID"] as? String, !callID.isEmpty,
              callID == expectedCallID,
              let server = object["server"] as? String, !server.isEmpty,
              let resource = object["resourceURI"] as? String, resource.hasPrefix("ui://")
        else { return nil }
        return Self(json: json, toolCallID: callID, allowsInteraction: object["interactionMode"] as? String == "liveSession")
    }
}

struct NativeMcpAppTransport: Equatable {
    let identity: String
    #if canImport(SQMcpAppsUI)
    let transport: AgentMcpAppTransport
    @MainActor init(identity: String,
         readResource: @escaping (AgentMcpAppProjection, String, @escaping (Result<String, Error>) -> Void) -> AgentMcpAppOperation?,
         callTool: @escaping (AgentMcpAppProjection, String, String, String?, @escaping (Result<String, Error>) -> Void) -> AgentMcpAppOperation?) {
        self.identity = identity
        transport = AgentMcpAppTransport(identity: identity, hostName: "Memex", readResource: readResource, callTool: callTool)
    }
    #endif
    static func == (lhs: Self, rhs: Self) -> Bool { lhs.identity == rhs.identity }
}

@MainActor enum NativeMcpAppHost {
    static let height: CGFloat = 360

    static func view(app: NativeMcpAppDescriptor, transport: NativeMcpAppTransport?) -> NSView {
        #if canImport(SQMcpAppsUI)
        if let transport, let projection = try? JSONDecoder().decode(AgentMcpAppProjection.self, from: Data(app.json.utf8)) {
            return NSHostingView(rootView: ScrollView {
                AgentMcpAppResultView(transport: transport.transport, app: projection, toolCallID: app.toolCallID)
                    .frame(maxWidth: .infinity)
            })
        }
        #endif
        let label = NSTextField(wrappingLabelWithString: "Interactive app unavailable for this connection. The original tool result remains below.")
        label.textColor = .secondaryLabelColor
        return label
    }
}

#if canImport(SQACPHost)
import SQACPHost
#endif

/// Crosses the runtime actor boundary with only queue-owned service identity.
/// SwiftUI and its closure transport are constructed later on the main actor.
struct NativeMcpAppConnection: Sendable, Equatable {
    let identity: String
    let sessionID: String
    #if canImport(SQACPHost)
    let service: AgentConversationService

    init(identity: String, sessionID: String, service: AgentConversationService) {
        self.identity = identity
        self.sessionID = sessionID
        self.service = service
    }
    #endif

    static func == (lhs: Self, rhs: Self) -> Bool {
        guard lhs.identity == rhs.identity, lhs.sessionID == rhs.sessionID else { return false }
        #if canImport(SQACPHost)
        return lhs.service === rhs.service
        #else
        return true
        #endif
    }

    @MainActor var transport: NativeMcpAppTransport? {
        #if canImport(SQMcpAppsUI) && canImport(SQACPHost)
        guard service.mcpAppTransportIdentity(sessionID: sessionID) == identity else { return nil }
        return NativeMcpAppTransport(identity: identity, readResource: { app, uri, completion in
            do {
                return try service.readMcpAppResource(sessionID: sessionID, lease: identity,
                    app: app, uri: uri, completion: completion)
            } catch {
                completion(.failure(error))
                return nil
            }
        }, callTool: { app, name, arguments, meta, completion in
            do {
                return try service.callMcpAppTool(sessionID: sessionID, lease: identity,
                    app: app, name: name, argumentsJSON: arguments, metaJSON: meta, completion: completion)
            } catch {
                completion(.failure(error))
                return nil
            }
        })
        #else
        return nil
        #endif
    }
}
