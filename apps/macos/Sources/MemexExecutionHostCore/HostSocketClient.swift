import Foundation
import Darwin

public enum HostSocketClient {
    public static func request(_ request: HostRequest, directory: URL, endpointName: String = "desktop") throws -> HostResponse {
        guard ["control", "desktop"].contains(endpointName) else { throw HostFailure("socket", "Invalid execution endpoint name") }
        try ExecutionHost.requirePrivateDirectory(directory)
        let path = directory.appendingPathComponent(endpointName + ".sock").path
        var info = stat()
        guard lstat(path, &info) == 0, info.st_uid == getuid(), info.st_mode & S_IFMT == S_IFSOCK,
              info.st_mode & 0o077 == 0 else { throw HostFailure("desktop_unavailable", "No private desktop bridge is attached to this host") }
        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { throw HostFailure("socket", "Cannot create desktop bridge connection") }
        defer { Darwin.close(fd) }
        _ = fcntl(fd, F_SETFD, FD_CLOEXEC)
        var timeout = timeval(tv_sec: 25, tv_usec: 0)
        _ = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        _ = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, socklen_t(MemoryLayout<timeval>.size))
        var noPipe: Int32 = 1
        _ = setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &noPipe, socklen_t(MemoryLayout<Int32>.size))
        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        let bytes = Array(path.utf8CString)
        guard bytes.count <= MemoryLayout.size(ofValue: address.sun_path) else { throw HostFailure("socket", "Desktop bridge socket path is too long") }
        withUnsafeMutableBytes(of: &address.sun_path) { target in
            for (index, byte) in bytes.enumerated() { target[index] = UInt8(bitPattern: byte) }
        }
        let connected = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
        }
        guard connected == 0 else { throw HostFailure("desktop_unavailable", "The desktop bridge is disconnected") }
        var data = try JSONEncoder().encode(request)
        guard data.count <= 4 * 1024 * 1024 else { throw HostFailure("request_limit", "Desktop request exceeds 4 MiB") }
        data.append(10)
        try data.withUnsafeBytes { buffer in
            guard let base = buffer.baseAddress else { return }
            var offset = 0
            while offset < buffer.count {
                let count = Darwin.write(fd, base.advanced(by: offset), buffer.count - offset)
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else { throw HostFailure("delivery_unknown", "Desktop request delivery was interrupted; do not retry automatically") }
                offset += count
            }
        }
        var response = Data()
        var chunk = [UInt8](repeating: 0, count: 8192)
        while response.count <= 32 * 1024 * 1024 {
            let count = Darwin.read(fd, &chunk, chunk.count)
            if count < 0 && errno == EINTR { continue }
            guard count > 0 else { throw HostFailure("delivery_unknown", "Desktop acknowledgement was lost; the action may have run") }
            if let end = chunk.prefix(count).firstIndex(of: 10) {
                response.append(contentsOf: chunk[..<end])
                guard response.count <= 32 * 1024 * 1024 else { break }
                return try JSONDecoder().decode(HostResponse.self, from: response)
            }
            response.append(contentsOf: chunk.prefix(count))
        }
        throw HostFailure("response_limit", "Desktop response exceeds 32 MiB")
    }
}
