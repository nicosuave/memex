import Foundation
import Darwin

struct DaemonRequest: Encodable, Sendable {
    let op: String
    var machine: String?
    var filters: DaemonSessionFilters?
    var query: String?
    var project: String?
    var source: String?
    var since: String?
    var origin: String?
    var limit: Int?
    var sessionID: String?
    var sourcePath: String?
    var offset: Int?
    var maxChars: Int?

    enum CodingKeys: String, CodingKey {
        case op, machine, filters, query, project, source, since, origin, limit, offset
        case sessionID = "session_id", sourcePath = "source_path", maxChars = "max_chars"
    }
}

struct DaemonSessionFilters: Encodable, Sendable {
    var sessionID: String?
    var sourcePath: String?
    var project: String?
    var source: String?
    var since: String?
    var limit: Int
    var origin: String

    enum CodingKeys: String, CodingKey {
        case project, source, since, limit, origin
        case sessionID = "session_id", sourcePath = "source_path"
    }
}

private enum DaemonError: Error {
    case unavailable
    case requestFailed(String)
}

/// A small pool keeps slow remote reads from blocking local lists. Connections
/// are sequential and reusable; cancellation discards only the borrowed socket.
actor DaemonClient {
    private let path: String
    private let root: String
    private let maximumConnections: Int
    private var idle: [DaemonConnection] = []
    private var borrowed = 0

    init(root: URL, socket: URL? = nil, maximumConnections: Int = 6) {
        // Foundation standardization can rewrite /private/tmp back to /tmp;
        // use the same filesystem canonicalization as the Rust daemon.
        if let resolved = root.path.withCString({ realpath($0, nil) }) {
            self.root = String(cString: resolved)
            free(resolved)
        } else {
            self.root = root.standardizedFileURL.path
        }
        path = (socket ?? root.appendingPathComponent("state/native/app.sock")).path
        self.maximumConnections = max(1, maximumConnections)
    }

    func request(_ request: DaemonRequest, timeout: TimeInterval) async throws -> Data? {
        let deadline = ContinuousClock.now.advanced(by: .seconds(timeout))
        // Waiting for a pool slot remains cancellable; no blocking work runs on
        // the actor or UI executor. The pool is shared by copies of MemexClient.
        while borrowed >= maximumConnections {
            try Task.checkCancellation()
            if ContinuousClock.now >= deadline { return nil }
            try await Task.sleep(for: .milliseconds(10))
        }
        try Task.checkCancellation()
        if ContinuousClock.now >= deadline { return nil }
        let connection: DaemonConnection
        do { connection = try idle.popLast() ?? DaemonConnection(path: path, root: root) }
        catch { return nil }
        borrowed += 1
        defer { borrowed -= 1 }
        do {
            let data = try await withTaskCancellationHandler {
                try await Task.detached(priority: .userInitiated) {
                    try connection.request(request, deadline: deadline)
                }.value
            } onCancel: { connection.cancel() }
            try Task.checkCancellation()
            idle.append(connection)
            return data
        } catch is CancellationError {
            throw CancellationError()
        } catch DaemonError.requestFailed(let message) {
            throw ClientError(message: message)
        } catch {
            try Task.checkCancellation()
            return nil
        }
    }
}

/// One request owns this connection at a time. Only cancellation can arrive
/// concurrently; shutdown wakes I/O without closing/reusing its descriptor.
private final class DaemonConnection: @unchecked Sendable {
    private let descriptor: Int32
    private let path: String
    private let root: String
    private let lock = NSLock()
    private var cancelled = false
    private var connected = false
    private var capabilities: Set<String> = []
    private static let maximumFrame = 64 * 1024 * 1024

    init(path: String, root: String) throws {
        self.path = path
        self.root = root
        let address = sockaddr_un()
        guard path.utf8.count < MemoryLayout.size(ofValue: address.sun_path) else { throw DaemonError.unavailable }
        // Never send transcript queries to an unexpected local socket owner.
        for (entry, type) in [(path, mode_t(S_IFSOCK)), (URL(fileURLWithPath: path).deletingLastPathComponent().path, mode_t(S_IFDIR))] {
            var info = stat()
            guard entry.withCString({ lstat($0, &info) }) == 0,
                  info.st_uid == geteuid(), info.st_mode & S_IFMT == type,
                  info.st_mode & 0o077 == 0 else { throw DaemonError.unavailable }
        }
        let socket = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard socket >= 0 else { throw DaemonError.unavailable }
        var enabled: Int32 = 1
        guard fcntl(socket, F_SETFL, O_NONBLOCK) == 0,
              fcntl(socket, F_SETFD, FD_CLOEXEC) == 0,
              setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, socklen_t(MemoryLayout.size(ofValue: enabled))) == 0 else {
            Darwin.close(socket)
            throw DaemonError.unavailable
        }
        descriptor = socket
    }

    deinit { Darwin.close(descriptor) }

    func cancel() {
        lock.lock()
        cancelled = true
        _ = shutdown(descriptor, SHUT_RDWR)
        lock.unlock()
    }

    private func check(_ deadline: ContinuousClock.Instant) throws {
        lock.lock()
        let stopped = cancelled
        lock.unlock()
        if stopped { throw CancellationError() }
        if ContinuousClock.now >= deadline { throw DaemonError.unavailable }
    }

    func request(_ request: DaemonRequest, deadline: ContinuousClock.Instant) throws -> Data {
        if !connected {
            let handshakeDeadline = min(deadline, ContinuousClock.now.advanced(by: .milliseconds(350)))
            try connect(deadline: handshakeDeadline)
            let hello = try exchange(DaemonRequest(op: "hello"), deadline: handshakeDeadline)
            guard let value = try JSONSerialization.jsonObject(with: hello) as? [String: Any],
                  value["root"] as? String == root,
                  let supported = value["capabilities"] as? [String] else { throw DaemonError.unavailable }
            capabilities = Set(supported)
            connected = true
        }
        guard capabilities.contains(request.op) else { throw DaemonError.unavailable }
        return try exchange(request, deadline: deadline)
    }

    private func connect(deadline: ContinuousClock.Instant) throws {
        try check(deadline)
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        withUnsafeMutableBytes(of: &address.sun_path) { destination in
            destination.copyBytes(from: Array(path.utf8) + [0])
        }
        let result = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(descriptor, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        if result != 0 {
            guard errno == EINPROGRESS || errno == EAGAIN else { throw DaemonError.unavailable }
            try wait(Int16(POLLOUT), deadline: deadline)
            var error: Int32 = 0
            var size = socklen_t(MemoryLayout.size(ofValue: error))
            guard getsockopt(descriptor, SOL_SOCKET, SO_ERROR, &error, &size) == 0, error == 0 else { throw DaemonError.unavailable }
        }
        var user: uid_t = 0
        var group: gid_t = 0
        guard getpeereid(descriptor, &user, &group) == 0, user == geteuid() else { throw DaemonError.unavailable }
    }

    private func exchange(_ request: DaemonRequest, deadline: ContinuousClock.Instant) throws -> Data {
        struct Envelope: Encodable { let protocolVersion = 1; let request: DaemonRequest
            enum CodingKeys: String, CodingKey { case protocolVersion = "protocol", request }
        }
        let body = try JSONEncoder().encode(Envelope(request: request))
        guard body.count <= 1024 * 1024 else { throw DaemonError.unavailable }
        var length = UInt32(body.count).bigEndian
        var frame = withUnsafeBytes(of: &length) { Data($0) }
        frame.append(body)
        try write(frame, deadline: deadline)
        let header = try read(4, deadline: deadline)
        let size = header.reduce(0) { ($0 << 8) | Int($1) }
        guard size > 0, size <= Self.maximumFrame else { throw DaemonError.unavailable }
        let response = try read(size, deadline: deadline)
        guard let envelope = try JSONSerialization.jsonObject(with: response) as? [String: Any],
              envelope["protocol"] as? Int == 1 else { throw DaemonError.unavailable }
        if let error = envelope["error"] as? [String: Any] {
            guard error["code"] as? String == "request_failed", let message = error["message"] as? String else {
                throw DaemonError.unavailable
            }
            throw DaemonError.requestFailed(message)
        }
        guard let result = envelope["result"] else { throw DaemonError.unavailable }
        return try JSONSerialization.data(withJSONObject: result, options: [.fragmentsAllowed])
    }

    private func wait(_ events: Int16, deadline: ContinuousClock.Instant) throws {
        while true {
            try check(deadline)
            var item = pollfd(fd: descriptor, events: events, revents: 0)
            let result = poll(&item, 1, 25)
            if result > 0 {
                guard item.revents & events != 0 else { throw DaemonError.unavailable }
                return
            }
            if result < 0 && errno != EINTR { throw DaemonError.unavailable }
        }
    }

    private func write(_ data: Data, deadline: ContinuousClock.Instant) throws {
        var offset = 0
        while offset < data.count {
            try check(deadline)
            let count = data.withUnsafeBytes {
                Darwin.send(descriptor, $0.baseAddress!.advanced(by: offset), data.count - offset, 0)
            }
            if count > 0 { offset += count }
            else if count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) { try wait(Int16(POLLOUT), deadline: deadline) }
            else { throw DaemonError.unavailable }
        }
    }

    private func read(_ size: Int, deadline: ContinuousClock.Instant) throws -> Data {
        var data = Data(count: size)
        var offset = 0
        while offset < size {
            try check(deadline)
            let count = data.withUnsafeMutableBytes {
                Darwin.recv(descriptor, $0.baseAddress!.advanced(by: offset), size - offset, 0)
            }
            if count > 0 { offset += count }
            else if count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) { try wait(Int16(POLLIN), deadline: deadline) }
            else { throw DaemonError.unavailable }
        }
        return data
    }
}
