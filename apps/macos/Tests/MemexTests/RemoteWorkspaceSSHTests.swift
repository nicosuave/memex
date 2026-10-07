import XCTest
@testable import Memex

final class RemoteWorkspaceSSHTests: XCTestCase {
    func testSSHArgumentsPreserveExactHostAndDoNotRunRemoteCommand() throws {
        let configuration = ExecutionHostSSHConfiguration(hostname: "nicbook-atm", user: "nico", identityFile: "/tmp/key with spaces")
        let arguments = try configuration.arguments()
        XCTAssertEqual(Array(arguments.suffix(2)), ["--", "nicbook-atm"])
        XCTAssertTrue(arguments.contains("BatchMode=yes"))
        XCTAssertTrue(arguments.contains("StrictHostKeyChecking=yes"))
        XCTAssertTrue(arguments.contains("127.0.0.1:56363:127.0.0.1:6363"))
        XCTAssertTrue(arguments.contains("/tmp/key with spaces"))
        XCTAssertTrue(arguments.contains("-N"))
        XCTAssertNotEqual(configuration.hostname, "nicbook")
    }
    func testSSHRejectsOptionHostsAndInvalidPorts() {
        for hostname in ["-oProxyCommand=bad", "host\nother", "user@host", ""] {
            XCTAssertThrowsError(try ExecutionHostSSHConfiguration(hostname: hostname, user: "").arguments())
        }
        XCTAssertThrowsError(try ExecutionHostSSHConfiguration(hostname: "host", user: "", localPort: 80).arguments())
    }
    func testLegacyConnectionDecodesWithoutSSHTunnel() throws {
        let data = Data(#"{"id":"verified","name":"remote","machineID":"nicbook-atm","endpoint":"https://example.invalid/api/control"}"#.utf8)
        let host = try JSONDecoder().decode(ExecutionHostConnection.self, from: data)
        XCTAssertNil(host.ssh)
        XCTAssertEqual(host.machineID, "nicbook-atm")
    }
}
