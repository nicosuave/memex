import XCTest
@testable import MemexExecutionHostCore

final class CommandOutputLimitTests: XCTestCase {
    func testBoundedOutputReturnsExactBytes() throws {
        let data = try CommandRun().execute(executable: URL(fileURLWithPath: "/usr/bin/printf"),
            arguments: ["bounded output"], timeout: 5, maximumOutputBytes: 64)
        XCTAssertEqual(String(decoding: data, as: UTF8.self), "bounded output")
    }
    func testUnboundedChildIsTerminatedPromptlyAtOutputLimit() {
        let started = Date()
        XCTAssertThrowsError(try CommandRun().execute(executable: URL(fileURLWithPath: "/usr/bin/yes"),
            arguments: ["oversized output"], timeout: 20, maximumOutputBytes: 4096)) {
            XCTAssertTrue($0.localizedDescription.contains("byte limit"), $0.localizedDescription)
        }
        XCTAssertLessThan(Date().timeIntervalSince(started), 5)
    }
    func testStderrCannotBypassBound() {
        XCTAssertThrowsError(try CommandRun().execute(executable: URL(fileURLWithPath: "/bin/sh"),
            arguments: ["-c", "exec /usr/bin/yes oversized >&2"], timeout: 20, maximumOutputBytes: 4096)) {
            XCTAssertTrue($0.localizedDescription.contains("byte limit"), $0.localizedDescription)
        }
    }
}
