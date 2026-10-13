import XCTest
import Darwin
@testable import MemexExecutionHostCore

final class ProcessSocketsTests: XCTestCase {
    private func loopbackListener() throws -> (descriptor: Int32, port: Int) {
        let descriptor = socket(AF_INET, SOCK_STREAM, 0)
        XCTAssertGreaterThanOrEqual(descriptor, 0)
        var address = sockaddr_in()
        address.sin_family = sa_family_t(AF_INET)
        address.sin_len = UInt8(MemoryLayout<sockaddr_in>.size)
        address.sin_addr.s_addr = in_addr_t(0x7f00_0001).bigEndian
        address.sin_port = 0
        let bound = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.bind(descriptor, $0, socklen_t(MemoryLayout<sockaddr_in>.size)) }
        }
        XCTAssertEqual(bound, 0)
        XCTAssertEqual(Darwin.listen(descriptor, 1), 0)
        var length = socklen_t(MemoryLayout<sockaddr_in>.size)
        _ = withUnsafeMutablePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.getsockname(descriptor, $0, &length) }
        }
        return (descriptor, Int(UInt16(bigEndian: address.sin_port)))
    }

    func testLoopbackOwnershipIdentifiesTheListeningProcessOnly() throws {
        let listener = try loopbackListener()
        defer { close(listener.descriptor) }
        XCTAssertTrue(ProcessSockets.listensOnLoopback(pid: getpid(), port: listener.port))
        XCTAssertFalse(ProcessSockets.listensOnLoopback(pid: getpid(), port: listener.port == 65535 ? 65534 : listener.port + 1))
        let other = Process()
        other.executableURL = URL(fileURLWithPath: "/bin/sleep")
        other.arguments = ["5"]
        try other.run()
        defer { other.terminate() }
        XCTAssertFalse(ProcessSockets.listensOnLoopback(pid: other.processIdentifier, port: listener.port))
    }

    func testSquattedPortIsNotAttributedToTheExpectedProcess() throws {
        // Another process owns the port first, as a local squatter would.
        let probe = try loopbackListener()
        let port = probe.port
        close(probe.descriptor)
        let squatter = Process()
        squatter.executableURL = URL(fileURLWithPath: "/usr/bin/nc")
        squatter.arguments = ["-l", "127.0.0.1", String(port)]
        squatter.standardInput = FileHandle.nullDevice
        squatter.standardOutput = FileHandle.nullDevice
        try squatter.run()
        defer { squatter.terminate() }
        let deadline = Date().addingTimeInterval(5)
        while Date() < deadline, !ProcessSockets.listensOnLoopback(pid: squatter.processIdentifier, port: port) {
            Thread.sleep(forTimeInterval: 0.05)
        }
        XCTAssertTrue(ProcessSockets.listensOnLoopback(pid: squatter.processIdentifier, port: port))
        XCTAssertFalse(ProcessSockets.listensOnLoopback(pid: getpid(), port: port))
    }

    func testUnixListenerOwnershipMatchesExactPath() throws {
        let directory = URL(fileURLWithPath: "/tmp/mxs-" + UUID().uuidString.prefix(8))
        defer { try? FileManager.default.removeItem(at: directory) }
        let server = try HostSocketServer(directory: directory)
        defer { server.stop() }
        let path = directory.appendingPathComponent("control.sock").path
        XCTAssertTrue(ProcessSockets.listensOnUnixSocket(pid: getpid(), path: path))
        XCTAssertFalse(ProcessSockets.listensOnUnixSocket(pid: getpid(), path: path + ".other"))
    }
}
