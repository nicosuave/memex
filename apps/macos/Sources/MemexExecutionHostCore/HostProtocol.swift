import Foundation

public enum HostValue: Codable, Equatable, Sendable {
    case null, bool(Bool), number(Double), string(String), array([HostValue]), object([String: HostValue])

    public init(from decoder: Decoder) throws {
        let c = try decoder.singleValueContainer()
        if c.decodeNil() { self = .null }
        else if let v = try? c.decode(Bool.self) { self = .bool(v) }
        else if let v = try? c.decode(Double.self) { self = .number(v) }
        else if let v = try? c.decode(String.self) { self = .string(v) }
        else if let v = try? c.decode([HostValue].self) { self = .array(v) }
        else { self = .object(try c.decode([String: HostValue].self)) }
    }
    public func encode(to encoder: Encoder) throws {
        var c = encoder.singleValueContainer()
        switch self {
        case .null: try c.encodeNil()
        case .bool(let v): try c.encode(v)
        case .number(let v): try c.encode(v)
        case .string(let v): try c.encode(v)
        case .array(let v): try c.encode(v)
        case .object(let v): try c.encode(v)
        }
    }
    public subscript(_ key: String) -> HostValue {
        if case .object(let value) = self { return value[key] ?? .null }; return .null
    }
    public var string: String? { if case .string(let v) = self { return v }; return nil }
    public var bool: Bool? { if case .bool(let v) = self { return v }; return nil }
    public var number: Double? { if case .number(let v) = self { return v }; return nil }
    public var array: [HostValue] { if case .array(let v) = self { return v }; return [] }
    public var object: [String: HostValue] { if case .object(let v) = self { return v }; return [:] }
    public static func encoded<T: Encodable>(_ value: T) throws -> HostValue {
        let encoder = JSONEncoder()
        encoder.dateEncodingStrategy = .iso8601
        return try JSONDecoder().decode(Self.self, from: encoder.encode(value))
    }
}

public struct HostRequest: Codable, Sendable {
    public var id: HostValue
    public var method: String
    public var params: [String: HostValue]
    public init(id: HostValue = .null, method: String, params: [String: HostValue] = [:]) {
        self.id = id; self.method = method; self.params = params
    }
}

public struct HostResponse: Codable, Sendable {
    public var id: HostValue
    public var result: HostValue?
    public var error: HostFailure?
    public init(id: HostValue, result: HostValue? = nil, error: HostFailure? = nil) {
        self.id = id; self.result = result; self.error = error
    }
}

public struct HostFailure: Error, Codable, LocalizedError, Sendable {
    public let code: String
    public let message: String
    public var errorDescription: String? { message }
    public init(_ code: String, _ message: String) { self.code = code; self.message = message }
}

public struct HostedConversation: Codable, Equatable, Sendable {
    public var id: String
    public var nativeSessionID: String
    public var provider: String
    public var providerInstanceID: String
    public var workspaceID: String
    public var cwd: String
    public var transcriptPath: String?
    public var title: String
    public var parentID: String?
    public var createdAt: String
    public var handoffResume: Bool? = nil
    public var claudePermissionMode: String? = nil
}

public struct HostedCommand: Codable, Equatable, Sendable {
    public var id: String
    public var issuedAt: String
    public var conversationID: String
    public var action: String
    public var text: String
    public var optionID: String?
    public var requestID: String?
    public var promptContent: HostValue?
}

public struct NativeConversationTransfer: Codable, Sendable {
    public var conversation: HostedConversation
    public var transcript: Data
    public var transcriptSHA256: String
    public init(conversation: HostedConversation, transcript: Data, transcriptSHA256: String) {
        self.conversation = conversation; self.transcript = transcript; self.transcriptSHA256 = transcriptSHA256
    }
}

/// Process execution stays in SQACPHost. Test implementations supply provider events,
/// not a second implementation of native protocol behavior.
public protocol ExecutionProvider: AnyObject {
    var providers: [String] { get }
    func create(id: String, provider: String, workspaceID: String, cwd: String, title: String) throws -> HostedConversation
    func resume(_ conversation: HostedConversation) throws
    func importConversation(id: String, provider: String, nativeSessionID: String, sourcePath: String,
                            workspaceID: String, cwd: String, title: String) throws -> HostedConversation
    func read(_ conversation: HostedConversation) throws -> HostValue
    func readChild(_ conversation: HostedConversation, childID: String) throws -> HostValue
    func perform(_ command: HostedCommand) throws -> HostValue
    func isConnected(_ id: String) -> Bool
    /// Nil means this concrete conversation has a verified native transfer path.
    func handoffUnavailableReason(_ conversation: HostedConversation) -> String?
    func detachForHandoff(_ conversation: HostedConversation) throws
    func exportForHandoff(_ conversation: HostedConversation) throws -> NativeConversationTransfer
    func retireHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws
    func restoreRetiredHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws
    /// Installs retained native history without connecting or sending any prompt.
    func adoptHandoff(_ transfer: NativeConversationTransfer, id: String, workspaceID: String, cwd: String) throws -> HostedConversation
    func activateHandoff(_ transfer: NativeConversationTransfer, conversation: HostedConversation) throws -> HostedConversation
    func relocate(_ conversation: HostedConversation, workspaceID: String, cwd: String) throws -> HostedConversation
    /// Called only after the host's explicit recovery/ownership decision.
    func releaseHandoffFence(_ conversation: HostedConversation) throws
}

extension ExecutionProvider {
    public func readChild(_ conversation: HostedConversation, childID: String) throws -> HostValue {
        throw HostFailure("capability_unavailable", "This provider does not support parent-bound child history")
    }
    public func handoffUnavailableReason(_ conversation: HostedConversation) -> String? { "This provider has no verified native-session handoff implementation" }
    public func detachForHandoff(_ conversation: HostedConversation) throws { throw HostFailure("handoff_unsupported", handoffUnavailableReason(conversation) ?? "Handoff unavailable") }
    public func exportForHandoff(_ conversation: HostedConversation) throws -> NativeConversationTransfer { throw HostFailure("handoff_unsupported", handoffUnavailableReason(conversation) ?? "Handoff unavailable") }
    public func retireHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws { throw HostFailure("handoff_unsupported", "Durable native retirement is unavailable") }
    public func restoreRetiredHandoff(_ transfer: NativeConversationTransfer, operationID: String) throws { throw HostFailure("handoff_unsupported", "Durable native retirement recovery is unavailable") }
    public func adoptHandoff(_ transfer: NativeConversationTransfer, id: String, workspaceID: String, cwd: String) throws -> HostedConversation { throw HostFailure("handoff_unsupported", "Native session import is unavailable") }
    public func activateHandoff(_ transfer: NativeConversationTransfer, conversation: HostedConversation) throws -> HostedConversation { throw HostFailure("handoff_unsupported", "Native session activation is unavailable") }
    public func relocate(_ conversation: HostedConversation, workspaceID: String, cwd: String) throws -> HostedConversation { throw HostFailure("handoff_unsupported", handoffUnavailableReason(conversation) ?? "Handoff unavailable") }
    public func releaseHandoffFence(_ conversation: HostedConversation) throws {}
    public func importConversation(id: String, provider: String, nativeSessionID: String, sourcePath: String,
                                   workspaceID: String, cwd: String, title: String) throws -> HostedConversation {
        throw HostFailure("capability_unavailable", "This provider does not support importing native sessions")
    }
}
