import Foundation
import Darwin

/// This endpoint never binds a network address. The Rust web gateway owns remote
/// bearer authentication; same-user clients use the owner-only socket directly.
public final class HostSocketServer: @unchecked Sendable {
    private let listener: Int32
    private let ownerLock: Int32
    private let socketURL: URL
    private let capacity = DispatchSemaphore(value: 8)
    private let stateLock = NSLock()
    private var stopped = false
    public let directory: URL

    public init(directory: URL, endpointName: String = "control") throws {
        guard ["control", "desktop"].contains(endpointName) else { throw HostFailure("socket", "Invalid execution endpoint name") }
        self.directory = directory
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        try ExecutionHost.requirePrivateDirectory(directory)
        let lockName = endpointName == "control" ? "owner.lock" : "desktop-owner.lock"
        let lock = Darwin.open(directory.appendingPathComponent(lockName).path, O_CREAT | O_RDWR | O_NOFOLLOW | O_CLOEXEC, 0o600)
        guard lock >= 0 else { throw HostFailure("ownership", "Cannot open execution host lock") }
        guard flock(lock, LOCK_EX | LOCK_NB) == 0 else {
            Darwin.close(lock)
            throw HostFailure("ownership", "An execution host already owns this root")
        }
        ownerLock = lock
        socketURL = directory.appendingPathComponent(endpointName + ".sock")
        listener = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard listener >= 0 else {
            Darwin.close(lock)
            throw HostFailure("socket", "Cannot create local execution socket")
        }
        _ = fcntl(listener, F_SETFD, FD_CLOEXEC)
        do {
            if endpointName == "control" { try Self.createToken(directory) }
            var address = sockaddr_un()
            address.sun_family = sa_family_t(AF_UNIX)
            let bytes = Array(socketURL.path.utf8CString)
            guard bytes.count <= MemoryLayout.size(ofValue: address.sun_path) else {
                throw HostFailure("socket_path", "Execution root is too long for a Unix socket; choose a shorter --root")
            }
            withUnsafeMutableBytes(of: &address.sun_path) { target in
                for (index, byte) in bytes.enumerated() { target[index] = UInt8(bitPattern: byte) }
            }
            address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
            var info = stat()
            if lstat(socketURL.path, &info) == 0 {
                guard info.st_mode & S_IFMT == S_IFSOCK, info.st_uid == getuid() else {
                    throw HostFailure("socket_path", "Refusing to replace a non-socket or another user's execution endpoint")
                }
                guard unlink(socketURL.path) == 0 else { throw HostFailure("socket", "Cannot remove stale execution socket") }
            }
            let result = withUnsafePointer(to: &address) { pointer in
                pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                    Darwin.bind(listener, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
                }
            }
            guard result == 0, chmod(socketURL.path, 0o600) == 0, Darwin.listen(listener, 16) == 0 else {
                throw HostFailure("socket", "Cannot bind private execution socket: \(String(cString: strerror(errno)))")
            }
        } catch {
            Darwin.close(listener); Darwin.close(ownerLock); throw error
        }
    }

    deinit {
        stop()
        unlink(socketURL.path)
        Darwin.close(ownerLock)
    }

    public func stop() {
        stateLock.withLock {
            guard !stopped else { return }
            stopped = true
            _ = Darwin.shutdown(listener, SHUT_RDWR)
            Darwin.close(listener)
        }
    }

    public func run(host: ExecutionHost) throws {
        let timer = DispatchSource.makeTimerSource(queue: DispatchQueue(label: "memex.host.scheduler"))
        timer.schedule(deadline: .now() + 1, repeating: 1)
        timer.setEventHandler {
            do { try host.tick() }
            catch { FileHandle.standardError.write(Data("Execution scheduler: \(error.localizedDescription)\n".utf8)) }
        }
        timer.resume()
        defer { timer.cancel() }
        try run(handler: { host.handle($0) })
    }

    public func run(handler: @escaping @Sendable (HostRequest) -> HostResponse) throws {
        while !stateLock.withLock({ stopped }) {
            let client = Darwin.accept(listener, nil, nil)
            if client < 0 {
                if stateLock.withLock({ stopped }) { return }
                if errno == EINTR { continue }
                throw HostFailure("socket", "Execution socket accept failed")
            }
            _ = fcntl(client, F_SETFD, FD_CLOEXEC)
            capacity.wait()
            DispatchQueue.global(qos: .userInitiated).async { [self] in
                defer { Darwin.close(client); capacity.signal() }
                var timeout = timeval(tv_sec: 65, tv_usec: 0)
                _ = setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
                _ = setsockopt(client, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
                var noPipe: Int32 = 1
                _ = setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &noPipe, socklen_t(MemoryLayout<Int32>.size))
                do {
                    let request = try Self.readRequest(client)
                    let response = handler(request)
                    var data = try JSONEncoder().encode(response)
                    guard data.count <= 32 * 1024 * 1024 else { throw HostFailure("response_limit", "Read a smaller conversation page") }
                    data.append(10)
                    try Self.write(data, to: client)
                } catch {
                    let response = HostResponse(id: .null, error: HostFailure("transport", error.localizedDescription))
                    if var data = try? JSONEncoder().encode(response) { data.append(10); try? Self.write(data, to: client) }
                }
            }
        }
    }

    private static func readRequest(_ fd: Int32) throws -> HostRequest {
        var data = Data()
        var chunk = [UInt8](repeating: 0, count: 8192)
        while data.count <= 4 * 1024 * 1024 {
            let count = Darwin.read(fd, &chunk, chunk.count)
            if count < 0 && errno == EINTR { continue }
            guard count > 0 else { throw HostFailure("transport", "Incomplete execution request") }
            if let end = chunk.prefix(count).firstIndex(of: 10) {
                data.append(contentsOf: chunk[..<end])
                guard data.count <= 4 * 1024 * 1024 else { break }
                return try JSONDecoder().decode(HostRequest.self, from: data)
            }
            data.append(contentsOf: chunk.prefix(count))
        }
        throw HostFailure("request_limit", "Execution request exceeds 4 MiB")
    }

    private static func write(_ data: Data, to fd: Int32) throws {
        try data.withUnsafeBytes { raw in
            guard let base = raw.baseAddress else { return }
            var offset = 0
            while offset < raw.count {
                let count = Darwin.write(fd, base.advanced(by: offset), raw.count - offset)
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else { throw HostFailure("transport", "Execution response connection closed") }
                offset += count
            }
        }
    }

    private static func createToken(_ directory: URL) throws {
        let path = directory.appendingPathComponent("control-token").path
        let fd = Darwin.open(path, O_CREAT | O_EXCL | O_WRONLY | O_NOFOLLOW, 0o600)
        if fd < 0 {
            guard errno == EEXIST else { throw HostFailure("authentication", "Cannot create control pairing token") }
            var info = stat()
            guard lstat(path, &info) == 0, info.st_mode & S_IFMT == S_IFREG,
                  info.st_uid == getuid(), info.st_mode & 0o077 == 0 else {
                throw HostFailure("authentication", "Control token must be a private file owned by this user")
            }
            let value = try String(contentsOfFile: path, encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines)
            guard value.utf8.count == 64, value.allSatisfy({ $0.isHexDigit }) else { throw HostFailure("authentication", "Control token file is invalid") }
            return
        }
        defer { Darwin.close(fd) }
        var bytes = [UInt8](repeating: 0, count: 32)
        arc4random_buf(&bytes, bytes.count)
        let token = bytes.map { String(format: "%02x", $0) }.joined() + "\n"
        try write(Data(token.utf8), to: fd)
        guard fsync(fd) == 0 else { throw HostFailure("authentication", "Cannot persist control pairing token") }
    }
}
