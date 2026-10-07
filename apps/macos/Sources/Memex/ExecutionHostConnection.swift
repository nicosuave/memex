import Foundation
import Security
import CryptoKit
import Observation
import MemexExecutionHostCore

struct ExecutionHostConnection: Codable, Identifiable, Equatable, Sendable {
    var id: String // Verified host identity, never inferred from a retrieval-machine alias.
    var name: String
    var machineID: String
    var endpoint: URL
    var ssh: ExecutionHostSSHConfiguration? = nil
    var handoffPublicKey: String? = nil
}

enum ExecutionHostCredential {
    private static let service = "dev.memex.execution-host"
    static func read(_ id: String) throws -> String {
        var result: CFTypeRef?
        let status = SecItemCopyMatching([kSecClass: kSecClassGenericPassword, kSecAttrService: service,
            kSecAttrAccount: id, kSecReturnData: true, kSecMatchLimit: kSecMatchLimitOne] as CFDictionary, &result)
        guard status == errSecSuccess, let data = result as? Data, let token = String(data: data, encoding: .utf8) else {
            throw HostFailure("pairing", "Pair this execution host again to restore its Keychain credential.")
        }
        return token
    }
    static func save(_ token: String, id: String) throws {
        let query = [kSecClass: kSecClassGenericPassword, kSecAttrService: service, kSecAttrAccount: id] as [CFString: Any]
        let data = Data(token.utf8)
        let status = SecItemUpdate(query as CFDictionary, [kSecValueData: data] as CFDictionary)
        if status == errSecItemNotFound {
            var attributes = query
            attributes[kSecValueData] = data
            attributes[kSecAttrAccessible] = kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly
            guard SecItemAdd(attributes as CFDictionary, nil) == errSecSuccess else { throw HostFailure("pairing", "Cannot store the execution credential in Keychain") }
        } else if status != errSecSuccess { throw HostFailure("pairing", "Cannot update the execution credential in Keychain") }
    }
    static func remove(_ id: String) {
        SecItemDelete([kSecClass: kSecClassGenericPassword, kSecAttrService: service, kSecAttrAccount: id] as CFDictionary)
    }
}

private final class ExecutionHTTPDelegate: NSObject, URLSessionTaskDelegate, Sendable {
    func urlSession(_ session: URLSession, task: URLSessionTask, willPerformHTTPRedirection response: HTTPURLResponse,
                    newRequest request: URLRequest, completionHandler: @escaping @Sendable (URLRequest?) -> Void) {
        completionHandler(nil) // Never forward an execution credential through a redirect.
    }
}

actor ExecutionHostHTTPClient {
    let endpoint: URL
    let hostID: String?
    private let token: String
    private let session: URLSession
    private let outbox: URL

    init(endpoint: URL, token: String, hostID: String? = nil, outbox: URL? = nil, requestTimeout: TimeInterval = 65) throws {
        self.endpoint = try Self.validatedEndpoint(endpoint)
        self.token = token
        self.hostID = hostID
        let configuration = URLSessionConfiguration.ephemeral
        configuration.timeoutIntervalForRequest = requestTimeout
        configuration.timeoutIntervalForResource = requestTimeout + 5
        session = URLSession(configuration: configuration, delegate: ExecutionHTTPDelegate(), delegateQueue: nil)
        let support = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
        self.outbox = outbox ?? support.appendingPathComponent("dev.memex.app/ExecutionOutbox/" + Self.digest(hostID ?? endpoint.absoluteString))
        try FileManager.default.createDirectory(at: self.outbox, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
    }

    static func validatedEndpoint(_ value: URL) throws -> URL {
        guard var components = URLComponents(url: value, resolvingAgainstBaseURL: false),
              let hostname = components.host?.lowercased(), components.user == nil, components.password == nil,
              components.query == nil, components.fragment == nil,
              components.scheme == "https" || (components.scheme == "http" && ["localhost", "127.0.0.1", "::1", "[::1]"].contains(hostname)) else {
            throw HostFailure("endpoint", "Use an HTTPS execution endpoint or HTTP on a loopback SSH tunnel.")
        }
        guard components.path.isEmpty || components.path == "/" || components.path == "/api/control" else {
            throw HostFailure("endpoint", "Enter the Memex server origin or its /api/control endpoint.")
        }
        components.path = "/api/control"
        guard let url = components.url else { throw HostFailure("endpoint", "Invalid execution host URL") }
        return url
    }

    func call(_ method: String, params: [String: HostValue] = [:], mutation: Bool = false) async throws -> HostValue {
        var params = params
        if let hostID { params["hostId"] = .string(hostID) }
        let commandID = params["commandId"]?.string ?? UUID().uuidString
        if mutation {
            params["commandId"] = .string(commandID)
            if params["issuedAt"] == nil { params["issuedAt"] = .string(Date().ISO8601Format()) }
        }
        let request = HostRequest(id: .string(commandID), method: method, params: params)
        let outgoing = outbox.appendingPathComponent(Self.digest(commandID) + ".json")
        if mutation {
            if FileManager.default.fileExists(atPath: outgoing.path) {
                let previous = try JSONDecoder().decode(HostRequest.self, from: Data(contentsOf: outgoing))
                guard previous.method == request.method, previous.params == request.params else {
                    throw HostFailure("id_conflict", "This command ID has a different saved outgoing request")
                }
            } else {
                try JSONEncoder().encode(request).write(to: outgoing, options: .atomic)
                try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: outgoing.path)
                let file = try FileHandle(forWritingTo: outgoing)
                try file.synchronize(); try file.close()
            }
        }
        var http = URLRequest(url: endpoint)
        http.httpMethod = "POST"
        http.httpBody = try JSONEncoder().encode(request)
        http.setValue("Bearer " + token, forHTTPHeaderField: "Authorization")
        http.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let data: Data
        let response: URLResponse
        do { (data, response) = try await session.data(for: http) }
        catch {
            throw HostFailure("delivery_unknown", mutation
                ? "Connection interrupted. The exact command \(commandID) is saved; inspect its host receipt before retrying. \(error.localizedDescription)"
                : error.localizedDescription)
        }
        guard let response = response as? HTTPURLResponse, response.statusCode == 200 else {
            throw HostFailure("connection", "Execution host HTTP \((response as? HTTPURLResponse)?.statusCode ?? 0). Verify its pairing token and gateway.")
        }
        guard data.count <= 32 * 1024 * 1024 else { throw HostFailure("response_limit", "Execution response is too large") }
        let decoded = try JSONDecoder().decode(HostResponse.self, from: data)
        guard decoded.id == request.id else { throw HostFailure("response_identity", "Execution response does not match this command") }
        if let error = decoded.error { throw error }
        guard let result = decoded.result else { throw HostFailure("response", "Execution host returned no result") }
        if mutation { try FileManager.default.removeItem(at: outgoing) }
        return result
    }

    func pending() throws -> [HostRequest] {
        try FileManager.default.contentsOfDirectory(at: outbox, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "json" }.map { try JSONDecoder().decode(HostRequest.self, from: Data(contentsOf: $0)) }
    }
    func reconcile(_ request: HostRequest) throws {
        guard let commandID = request.params["commandId"]?.string else {
            throw HostFailure("invalid_params", "The saved request has no command identity")
        }
        let outgoing = outbox.appendingPathComponent(Self.digest(commandID) + ".json")
        let saved = try JSONDecoder().decode(HostRequest.self, from: Data(contentsOf: outgoing))
        guard saved.method == request.method, saved.params == request.params else {
            throw HostFailure("id_conflict", "The saved outgoing request changed; reload it before reconciling")
        }
        let resolved = outbox.appendingPathComponent("resolved", isDirectory: true)
        try FileManager.default.createDirectory(at: resolved, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        // Keep the exact original request for inspection after the user has checked
        // the provider transcript. Reconciliation never dispatches it again.
        try FileManager.default.moveItem(at: outgoing, to: resolved.appendingPathComponent(outgoing.lastPathComponent))
    }
    private static func digest(_ value: String) -> String { SHA256.hash(data: Data(value.utf8)).map { String(format: "%02x", $0) }.joined() }
}

@MainActor @Observable
final class ExecutionHostConnections {
    static let shared = ExecutionHostConnections(defaults: .standard)
    private(set) var hosts: [ExecutionHostConnection]
    private(set) var error: String?
    private let defaults: UserDefaults?
    private static let key = "memex.execution-hosts.v1"

    init(defaults: UserDefaults? = nil) {
        self.defaults = defaults
        hosts = defaults?.data(forKey: Self.key).flatMap { try? JSONDecoder().decode([ExecutionHostConnection].self, from: $0) } ?? []
    }

    func pair(name: String, machineID: String, endpoint: URL, token: String) async throws -> ExecutionHostConnection {
        guard !machineID.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { throw HostFailure("machine", "Enter the exact Memex machine identifier") }
        let client = try ExecutionHostHTTPClient(endpoint: endpoint, token: token)
        let info = try await client.call("host.info")
        guard let id = info["hostId"].string else { throw HostFailure("pairing", "The endpoint did not identify an execution host") }
        if let existing = hosts.first(where: { $0.machineID == machineID && $0.id != id }) {
            throw HostFailure("machine", "\(machineID) is already paired with \(existing.name). Remove that pairing before changing its execution identity.")
        }
        try ExecutionHostCredential.save(token, id: id)
        let connection = ExecutionHostConnection(id: id, name: name.isEmpty ? machineID : name,
            machineID: machineID, endpoint: try ExecutionHostHTTPClient.validatedEndpoint(endpoint),
            handoffPublicKey: info["handoffPublicKey"].string)
        hosts.removeAll { $0.id == id }; hosts.append(connection)
        try save()
        return connection
    }

    func remove(_ connection: ExecutionHostConnection) throws {
        hosts.removeAll { $0.id == connection.id }
        try save()
        ExecutionHostCredential.remove(connection.id)
        if let ssh = connection.ssh { ExecutionHostSSHTunnels.shared.stop(ssh) }
    }
    func connection(for session: Session) -> ExecutionHostConnection? { hosts.first { $0.machineID == session.machineID } }
    func client(_ connection: ExecutionHostConnection) throws -> ExecutionHostHTTPClient {
        if let ssh = connection.ssh { try ExecutionHostSSHTunnels.shared.start(ssh) }
        return try ExecutionHostHTTPClient(endpoint: connection.endpoint, token: ExecutionHostCredential.read(connection.id), hostID: connection.id)
    }
    func pairSSH(_ configuration: ExecutionHostSSHConfiguration, machineID: String, token: String) async throws -> ExecutionHostConnection {
        let started = try ExecutionHostSSHTunnels.shared.start(configuration)
        do {
            // Probe the loopback gateway before pairing; never retry a mutation.
            var connected = false
            for _ in 0..<10 {
                guard ExecutionHostSSHTunnels.shared.isRunning(configuration) else {
                    throw HostFailure("ssh_connection", "SSH could not establish the tunnel. Check authentication, trusted host key, and ports with your SSH client.")
                }
                do {
                    _ = try await ExecutionHostHTTPClient(endpoint: configuration.endpoint, token: token, requestTimeout: 1).call("host.info")
                    connected = true; break
                } catch { try await Task.sleep(for: .milliseconds(250)) }
            }
            guard connected else { throw HostFailure("ssh_connection", "The SSH tunnel did not reach the configured Memex execution gateway") }
            var host = try await pair(name: configuration.hostname, machineID: machineID, endpoint: configuration.endpoint, token: token)
            host.ssh = configuration
            hosts.removeAll { $0.id == host.id }; hosts.append(host)
            try save()
            return host
        } catch {
            if started { ExecutionHostSSHTunnels.shared.stop(configuration) }
            throw error
        }
    }
    private func save() throws { defaults?.set(try JSONEncoder().encode(hosts), forKey: Self.key) }
}
