import Foundation

/// ISO weekdays: Monday=1 ... Sunday=7. A recurrence owns its timezone rather
/// than inheriting the viewing client's or execution machine's current zone.
struct HostWallClockSchedule: Codable, Equatable, Sendable {
    let localTime: String
    let weekdays: [Int]
    let timeZone: String

    init(localTime: String, weekdays: [Int], timeZone: String) throws {
        let parts = localTime.split(separator: ":", omittingEmptySubsequences: false)
        guard parts.count == 2, parts.allSatisfy({ $0.count == 2 && $0.allSatisfy(\.isNumber) }),
              let hour = Int(parts[0]), (0...23).contains(hour),
              let minute = Int(parts[1]), (0...59).contains(minute),
              !weekdays.isEmpty, Set(weekdays).count == weekdays.count,
              weekdays.allSatisfy({ (1...7).contains($0) }), TimeZone(identifier: timeZone) != nil else {
            throw HostFailure("invalid_params", "Use localTime HH:mm, unique ISO weekdays 1 (Monday) through 7 (Sunday), and a valid timezone identifier")
        }
        self.localTime = localTime
        self.weekdays = weekdays.sorted()
        self.timeZone = timeZone
    }

    func next(after instant: Date) throws -> Date {
        guard let zone = TimeZone(identifier: timeZone) else { throw HostFailure("schedule_timezone", "The stored schedule timezone is unavailable") }
        var calendar = Calendar(identifier: .gregorian)
        calendar.timeZone = zone
        let parts = localTime.split(separator: ":").compactMap { Int($0) }
        guard parts.count == 2 else { throw HostFailure("schedule_time", "The stored schedule time is invalid") }
        let beginning = calendar.startOfDay(for: instant)
        // Eight days include the next occurrence of every selected weekday.
        // strict skips nonexistent spring-forward times. first plus the same-day
        // comparison ensures an autumn repeated time executes only once.
        for offset in 0...8 {
            guard let day = calendar.date(byAdding: .day, value: offset, to: beginning) else { continue }
            let weekday = (calendar.component(.weekday, from: day) + 5) % 7 + 1
            guard weekdays.contains(weekday), let candidate = calendar.nextDate(after: day.addingTimeInterval(-1),
                matching: DateComponents(hour: parts[0], minute: parts[1], second: 0), matchingPolicy: .strict,
                repeatedTimePolicy: .first, direction: .forward), calendar.isDate(candidate, inSameDayAs: day), candidate > instant else { continue }
            return candidate
        }
        throw HostFailure("schedule_time", "No valid wall-clock occurrence was found in the next eight days")
    }
}
