import Foundation
import Testing
import MemexExecutionHostCore
@testable import Memex

@MainActor struct HostScheduleObserverTests {
    @Test func defaultObserverDeliversWithoutForegroundWindowAndFailureOpensInbox() async {
        let notifications = ConversationNotifications()
        notifications.preferences.mode = .sound
        var events: [HostScheduleNotificationEvent] = []
        notifications.deliverSchedule = { event, _ in events.append(event) }
        let observer = HostScheduleObserver(notifications: notifications, defaults: nil)
        await observer.observe(hostID: "host", hostName: "Host", runs: [])
        await observer.observe(hostID: "host", hostName: "Host", runs: [run()])
        #expect(events.count == 1)
        var destinations: [String] = []
        notifications.openHostedConversation = { destinations.append($0 + ":" + $1) }
        notifications.openHostSchedules = { destinations.append($0 + ":inbox") }
        notifications.openConversation = { destinations.append("local:" + $0) }
        notifications.openNotification(hostID: "host", hostConversationID: nil, conversationID: nil)
        notifications.openNotification(hostID: "host", hostConversationID: "exact", conversationID: "unrelated")
        notifications.openNotification(hostID: nil, hostConversationID: nil, conversationID: "local")
        #expect(destinations == ["host:inbox", "host:exact", "local:local"])
    }

    private func run(_ id: String = "run", status: String = "failed", version: String = "one",
                     policy: String = "attention", read: Bool = false, attention: Bool = true) -> HostValue {
        .object(["id": .string(id), "status": .string(status), "updatedAt": .string(version),
                 "notificationPolicy": .string(policy), "read": .bool(read), "needsAttention": .bool(attention),
                 "conversationID": .string("exact-conversation")])
    }

    @Test func durableDedupAndStatusChangesRespectRunPolicy() async throws {
        let suite = "schedule-notifications-" + UUID().uuidString
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let notifications = ConversationNotifications()
        notifications.preferences.mode = .notifications
        var events: [HostScheduleNotificationEvent] = []
        notifications.deliverSchedule = { event, _ in events.append(event) }
        let observer = HostScheduleObserver(notifications: notifications, defaults: defaults, isActive: { true })
        await observer.observe(hostID: "host", hostName: "Exact host", runs: [])
        await observer.observe(hostID: "host", hostName: "Exact host", runs: [run(), run()])
        #expect(events.count == 1)
        #expect(events.first?.hostID == "host")
        #expect(events.first?.conversationID == "exact-conversation")
        let restored = HostScheduleObserver(notifications: notifications, defaults: defaults, isActive: { true })
        await restored.observe(hostID: "host", hostName: "Exact host", runs: [run()])
        #expect(events.count == 1)
        await restored.observe(hostID: "host", hostName: "Exact host", runs: [run(status: "completed", version: "two")])
        #expect(events.count == 1)
        await restored.observe(hostID: "host", hostName: "Exact host", runs: [run(status: "completed", version: "three", policy: "all")])
        #expect(events.count == 2)
        #expect(events.last?.isCompletion == true)
    }

    @Test func startupReadMutedInactiveAndGlobalOptOutStayQuiet() async {
        let notifications = ConversationNotifications()
        notifications.preferences.mode = .sound
        var events: [HostScheduleNotificationEvent] = []
        notifications.deliverSchedule = { event, _ in events.append(event) }
        let observer = HostScheduleObserver(notifications: notifications, defaults: nil, isActive: { true })
        await observer.observe(hostID: "host", hostName: "Host", runs: [run("old")])
        await observer.observe(hostID: "host", hostName: "Host", runs: [run("read", read: true), run("muted", policy: "never"), run("quiet", attention: false)])
        #expect(events.isEmpty)
        notifications.preferences.mode = .off
        await observer.observe(hostID: "host", hostName: "Host", runs: [run("off")])
        notifications.preferences.mode = .sound
        await observer.observe(hostID: "host", hostName: "Host", runs: [run("off")])
        #expect(events.isEmpty)
        notifications.isHostedConversationVisible = { host, conversation in host == "host" && conversation == "exact-conversation" }
        await observer.observe(hostID: "host", hostName: "Host", runs: [run("visible")])
        #expect(events.isEmpty)
    }

    @Test func pollingVerifiesHostIdentityAndCapabilityBeforeReadingRuns() async throws {
        let suite = "schedule-observer-hosts-" + UUID().uuidString
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let hosts = ["old", "wrong", "modern"].map {
            ExecutionHostConnection(id: $0, name: $0, machineID: $0, endpoint: URL(string: "http://127.0.0.1:5432")!)
        }
        defaults.set(try JSONEncoder().encode(hosts), forKey: "memex.execution-hosts.v1")
        let connections = ExecutionHostConnections(defaults: defaults)
        var methods: [String] = []
        let observer = HostScheduleObserver(notifications: ConversationNotifications(), connections: connections, defaults: nil,
            request: { host, method in
                methods.append(host.id + ":" + method)
                if method == "schedule.runs" { return .array([]) }
                return .object(["hostId": .string(host.id == "wrong" ? "other" : host.id),
                                "capabilities": .array(host.id == "old" ? [] : [.string("schedules.runs")])])
            }, isActive: { true })
        await observer.pollOnce()
        #expect(methods == ["old:host.info", "wrong:host.info", "modern:host.info", "modern:schedule.runs"])
        let inactive = HostScheduleObserver(notifications: ConversationNotifications(), connections: connections, defaults: nil,
            request: { _, _ in Issue.record("Inactive app must not poll"); return .null }, isActive: { false })
        await inactive.pollOnce()
    }
}
