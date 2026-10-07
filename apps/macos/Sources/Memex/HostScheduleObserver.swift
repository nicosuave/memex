import AppKit
import Foundation
import MemexExecutionHostCore

struct HostScheduleNotificationEvent: Equatable, Sendable {
    let hostID: String
    let runID: String
    let conversationID: String?
    let title: String
    let status: String
    var isCompletion: Bool { ["completed", "interrupted"].contains(status) }
    var body: String {
        switch status {
        case "completed": "Scheduled work completed."
        case "interrupted": "Scheduled work was interrupted."
        case "needs_input": "Scheduled work needs your input."
        case "failed": "Scheduled work failed. Open the run inbox for details."
        case "uncertain": "Scheduled delivery is uncertain. Inspect the run before retrying."
        case "held": "Scheduled work is held and needs attention."
        default: "Scheduled work needs attention."
        }
    }
}

/// Polls only paired identities while the app is running, including in the
/// background where notifications are useful. Seen receipt versions
/// are saved before notification delivery, including read/muted observations.
@MainActor final class HostScheduleObserver {
    typealias Request = @MainActor @Sendable (ExecutionHostConnection, String) async throws -> HostValue
    private let notifications: ConversationNotifications
    private let connections: ExecutionHostConnections
    private let defaults: UserDefaults?
    private let request: Request
    private let isActive: @MainActor () -> Bool
    private var seen: [String: [String: String]]
    private var task: Task<Void, Never>?
    private var polling = false
    private static let key = "memex.schedule-run-notification-seen.v1"

    init(notifications: ConversationNotifications, connections: ExecutionHostConnections = .shared,
         defaults: UserDefaults? = .standard, request: Request? = nil,
         isActive: @escaping @MainActor () -> Bool = { true }) {
        self.notifications = notifications
        self.connections = connections
        self.defaults = defaults
        self.request = request ?? { host, method in try await connections.client(host).call(method) }
        self.isActive = isActive
        seen = defaults?.data(forKey: Self.key).flatMap { try? JSONDecoder().decode([String: [String: String]].self, from: $0) } ?? [:]
    }

    func start() {
        guard task == nil else { return }
        task = Task { [weak self] in
            while !Task.isCancelled {
                guard let self else { return }
                if self.isActive() { await self.pollOnce() }
                do { try await Task.sleep(for: .seconds(15)) } catch { return }
            }
        }
    }

    func stop() { task?.cancel(); task = nil }

    func pollOnce() async {
        guard !polling, isActive(), !Task.isCancelled else { return }
        polling = true
        defer { polling = false }
        for host in connections.hosts {
            guard isActive(), !Task.isCancelled else { return }
            do {
                let runs = try await fetchRuns(host)
                guard isActive(), !Task.isCancelled, connections.hosts.contains(where: { $0 == host }) else { continue }
                if let runs { await observe(hostID: host.id, hostName: host.name, runs: runs) }
            } catch {
                // Connectivity failures are not schedule-run outcomes. Keep the
                // durable inbox authoritative and retry on the next bounded pass.
            }
        }
    }

    private func fetchRuns(_ host: ExecutionHostConnection) async throws -> [HostValue]? {
        let request = request
        return try await withThrowingTaskGroup(of: [HostValue]?.self) { group in
            group.addTask {
                let info = try await request(host, "host.info")
                guard info["hostId"].string == host.id else { throw HostFailure("wrong_host", "Schedule endpoint identity changed") }
                guard info["capabilities"].array.contains(.string("schedules.runs")) else { return nil }
                try Task.checkCancellation()
                return try await request(host, "schedule.runs").array
            }
            group.addTask {
                try await Task.sleep(for: .seconds(15))
                throw HostFailure("schedule_timeout", "Schedule status check timed out")
            }
            defer { group.cancelAll() }
            return try await group.next() ?? nil
        }
    }

    /// Public to the module for deterministic receipt/dedup tests. First sight of
    /// a host establishes a baseline; old inbox items never become startup spam.
    func observe(hostID: String, hostName: String, runs: [HostValue]) async {
        let old = seen[hostID]
        var updated = old ?? [:]
        var events: [HostScheduleNotificationEvent] = []
        for run in runs {
            guard let id = run["id"].string, let status = run["status"].string else { continue }
            let version = status + ":" + (run["updatedAt"].string ?? "")
            let prior = updated.updateValue(version, forKey: id)
            guard old != nil, prior != version,
                  run["read"].bool != true, run["needsAttention"].bool == true else { continue }
            let policy = run["notificationPolicy"].string ?? "attention"
            let needsInput = ["failed", "uncertain", "needs_input", "held"].contains(status)
            let completion = ["completed", "interrupted"].contains(status)
            guard (policy == "attention" && needsInput) || (policy == "all" && (needsInput || completion)) else { continue }
            events.append(.init(hostID: hostID, runID: id, conversationID: run["conversationID"].string,
                                title: "Scheduled work on " + hostName, status: status))
        }
        seen[hostID] = updated
        if let data = try? JSONEncoder().encode(seen) { defaults?.set(data, forKey: Self.key) }
        for event in events {
            guard isActive(), !Task.isCancelled else { return }
            _ = await notifications.receiveScheduleRun(event)
        }
    }
}
