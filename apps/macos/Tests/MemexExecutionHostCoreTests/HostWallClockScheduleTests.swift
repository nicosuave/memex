import XCTest
@testable import MemexExecutionHostCore

final class HostWallClockScheduleTests: XCTestCase {
    private func date(_ text: String) throws -> Date { try XCTUnwrap(ISO8601DateFormatter().date(from: text)) }

    func testSpringForwardSkipsNonexistentLocalTime() throws {
        let recurrence = try HostWallClockSchedule(localTime: "02:30", weekdays: [7], timeZone: "America/Los_Angeles")
        let next = try recurrence.next(after: date("2026-03-08T08:00:00Z"))
        XCTAssertEqual(next, try date("2026-03-15T09:30:00Z"))
    }

    func testAutumnRepeatedTimeRunsOnlyItsFirstOccurrence() throws {
        let recurrence = try HostWallClockSchedule(localTime: "01:30", weekdays: [7], timeZone: "America/Los_Angeles")
        XCTAssertEqual(try recurrence.next(after: date("2026-11-01T07:00:00Z")), try date("2026-11-01T08:30:00Z"))
        XCTAssertEqual(try recurrence.next(after: date("2026-11-01T08:45:00Z")), try date("2026-11-08T09:30:00Z"))
    }

    func testISOWeekdaysAndExplicitTimezoneDetermineNextRun() throws {
        let recurrence = try HostWallClockSchedule(localTime: "09:15", weekdays: [3, 1], timeZone: "Asia/Kolkata")
        XCTAssertEqual(recurrence.weekdays, [1, 3])
        XCTAssertEqual(try recurrence.next(after: date("2026-10-04T22:00:00Z")), try date("2026-10-05T03:45:00Z"))
        XCTAssertEqual(try recurrence.next(after: date("2026-10-05T03:45:00Z")), try date("2026-10-07T03:45:00Z"))
    }

    func testInvalidRecurrencesAreRejected() {
        XCTAssertThrowsError(try HostWallClockSchedule(localTime: "25:00", weekdays: [1], timeZone: "UTC"))
        XCTAssertThrowsError(try HostWallClockSchedule(localTime: "9:00", weekdays: [1], timeZone: "UTC"))
        XCTAssertThrowsError(try HostWallClockSchedule(localTime: "09:00", weekdays: [], timeZone: "UTC"))
        XCTAssertThrowsError(try HostWallClockSchedule(localTime: "09:00", weekdays: [1, 1], timeZone: "UTC"))
        XCTAssertThrowsError(try HostWallClockSchedule(localTime: "09:00", weekdays: [0], timeZone: "UTC"))
        XCTAssertThrowsError(try HostWallClockSchedule(localTime: "09:00", weekdays: [1], timeZone: "not/a-zone"))
    }
}
