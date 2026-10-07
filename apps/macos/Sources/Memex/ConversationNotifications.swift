import AppKit
import Foundation
import Observation
import SwiftUI
import UserNotifications

struct ConversationNotificationState: Equatable, Sendable {
    let activity: ConversationActivity?
    var requestIDs: [String] = []
}

struct ConversationNotificationPreferences: Codable, Equatable, Sendable {
    enum Mode: String, Codable, CaseIterable, Identifiable {
        case off, notifications, sound, notificationsAndSound
        var id: Self { self }
        var title: String {
            switch self {
            case .off: "Off"
            case .notifications: "Notifications only"
            case .sound: "Sound only"
            case .notificationsAndSound: "Notifications and sound"
            }
        }
        var showsNotification: Bool { self == .notifications || self == .notificationsAndSound }
        var playsSound: Bool { self == .sound || self == .notificationsAndSound }
    }
    var mode = Mode.off
    var completion = true
    var input = true
    var whileViewingConversation = false
}

struct ConversationNotificationEvent: Equatable, Sendable {
    enum Kind: String, Sendable { case completion, approval, question }
    let sessionID: String
    let title: String
    let kind: Kind
    var body: String {
        switch kind {
        case .completion: "Work completed."
        case .approval: "Approval needed to continue."
        case .question: "An answer is needed to continue."
        }
    }
}

/// Receives runtime transitions even when the sidebar is hidden. Past completed
/// history is never announced when a conversation is opened or the app restarts.
@MainActor @Observable
final class ConversationNotifications: NSObject {
    var preferences: ConversationNotificationPreferences {
        didSet {
            if let data = try? JSONEncoder().encode(preferences) {
                defaults?.set(data, forKey: Self.preferencesKey)
            }
        }
    }
    private(set) var error: String?
    private(set) var requestingPermission = false
    @ObservationIgnored var openHostedConversation: ((String, String) -> Void)?
    @ObservationIgnored var openHostSchedules: ((String) -> Void)?
    @ObservationIgnored var isHostedConversationVisible: (String, String) -> Bool = { _, _ in false }
    @ObservationIgnored var deliverSchedule: (@MainActor (HostScheduleNotificationEvent, ConversationNotificationPreferences) async throws -> Void)?
    @ObservationIgnored var openConversation: ((String) -> Void)?
    @ObservationIgnored var isConversationVisible: (String) -> Bool = { _ in false }
    @ObservationIgnored private let defaults: UserDefaults?
    @ObservationIgnored private var previous: [String: ConversationNotificationState] = [:]
    @ObservationIgnored private var activeWork: Set<String> = []
    @ObservationIgnored private var notifiedRequests: [String: Set<String>] = [:]
    @ObservationIgnored private let deliver: (@MainActor (ConversationNotificationEvent, ConversationNotificationPreferences) async throws -> Void)?
    private static let preferencesKey = "conversation-notification-preferences"

    init(defaults: UserDefaults? = nil,
         deliver: (@MainActor (ConversationNotificationEvent, ConversationNotificationPreferences) async throws -> Void)? = nil) {
        self.defaults = defaults
        self.deliver = deliver
        if let data = defaults?.data(forKey: Self.preferencesKey),
           let saved = try? JSONDecoder().decode(ConversationNotificationPreferences.self, from: data) {
            preferences = saved
        } else { preferences = ConversationNotificationPreferences() }
        super.init()
    }

    /// Only the packaged app installs a notification delegate. Constructing this
    /// controller in unit tests or a history-only CLI never touches the OS center.
    func activate() {
        guard Bundle.main.bundleIdentifier != nil else { return }
        UNUserNotificationCenter.current().delegate = self
    }

    func setMode(_ mode: ConversationNotificationPreferences.Mode) async {
        guard mode.showsNotification else { preferences.mode = mode; error = nil; return }
        guard !requestingPermission else { return }
        guard Bundle.main.bundleIdentifier != nil else {
            error = "System notifications require the installed Memex app. Sound only is available here."
            return
        }
        requestingPermission = true
        defer { requestingPermission = false }
        do {
            let granted = try await UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound, .badge])
            guard granted else {
                error = "Notifications are disabled for Memex. Enable them in System Settings → Notifications, or choose Sound only."
                return
            }
            preferences.mode = mode
            error = nil
        } catch { self.error = "Could not enable notifications: \(error.localizedDescription)" }
    }

    /// Returns the accepted event for callers/tests; delivery errors remain
    /// visible in preferences instead of changing conversation execution state.
    @discardableResult
    func receive(_ session: Session, state: ConversationNotificationState) -> ConversationNotificationEvent? {
        let old = previous.updateValue(state, forKey: session.id)
        let kind: ConversationNotificationEvent.Kind
        switch state.activity {
        case .working:
            activeWork.insert(session.id)
            return nil
        case .stopping, .stopped, .failed:
            activeWork.remove(session.id)
            return nil
        case .completed:
            guard activeWork.remove(session.id) != nil else { return nil }
            kind = .completion
        case .approval, .question:
            let requestKeys = Set(state.requestIDs.map { "\(state.activity!.rawValue):\($0)" })
            if requestKeys.isEmpty {
                guard old?.activity != state.activity else { return nil }
            } else {
                let alreadyNotified = notifiedRequests[session.id, default: []]
                guard !requestKeys.isSubset(of: alreadyNotified) else { return nil }
                notifiedRequests[session.id, default: []].formUnion(requestKeys)
            }
            kind = state.activity == .approval ? .approval : .question
        default: return nil
        }
        guard preferences.mode != .off,
              kind == .completion ? preferences.completion : preferences.input,
              preferences.whileViewingConversation || !isConversationVisible(session.id) else { return nil }
        let event = ConversationNotificationEvent(sessionID: session.id, title: session.title, kind: kind)
        let settings = preferences
        Task { [weak self] in
            guard let self, self.preferences.mode != .off else { return }
            do {
                if let deliver = self.deliver { try await deliver(event, settings) }
                else { try await self.deliverSystem(event, preferences: settings) }
                self.error = nil
            } catch { self.error = "Could not deliver a conversation notification: \(error.localizedDescription)" }
        }
        return event
    }

    /// Schedule runs have their own durable receipt state. They never synthesize
    /// conversation working/completed transitions to enter the notification path.
    @discardableResult
    func receiveScheduleRun(_ event: HostScheduleNotificationEvent) async -> Bool {
        guard preferences.mode != .off,
              event.isCompletion ? preferences.completion : preferences.input,
              preferences.whileViewingConversation || event.conversationID.map({ !isHostedConversationVisible(event.hostID, $0) }) != false else { return false }
        let settings = preferences
        do {
            if let deliverSchedule { try await deliverSchedule(event, settings) }
            else if settings.mode.showsNotification {
                guard Bundle.main.bundleIdentifier != nil else { throw CocoaError(.featureUnsupported) }
                let content = UNMutableNotificationContent()
                content.title = event.title
                content.body = event.body
                content.threadIdentifier = "schedule:" + event.hostID + ":" + event.runID
                content.userInfo = ["hostID": event.hostID, "scheduleRunID": event.runID]
                if let conversationID = event.conversationID { content.userInfo["hostConversationID"] = conversationID }
                if settings.mode.playsSound { content.sound = .default }
                try await UNUserNotificationCenter.current().add(UNNotificationRequest(identifier: UUID().uuidString, content: content, trigger: nil))
            } else if settings.mode.playsSound { NSSound(named: "Glass")?.play() }
            error = nil
            return true
        } catch { self.error = "Could not deliver a schedule notification: \(error.localizedDescription)"; return false }
    }

    private func deliverSystem(_ event: ConversationNotificationEvent,
                               preferences: ConversationNotificationPreferences) async throws {
        if preferences.mode.showsNotification {
            guard Bundle.main.bundleIdentifier != nil else { throw CocoaError(.featureUnsupported) }
            let content = UNMutableNotificationContent()
            content.title = event.title
            content.body = event.body
            content.threadIdentifier = event.sessionID
            content.userInfo = ["conversationID": event.sessionID]
            if preferences.mode.playsSound { content.sound = .default }
            let request = UNNotificationRequest(identifier: UUID().uuidString, content: content, trigger: nil)
            try await UNUserNotificationCenter.current().add(request)
        } else if preferences.mode.playsSound { NSSound(named: "Glass")?.play() }
    }

    func openNotification(hostID: String?, hostConversationID: String?, conversationID: String?) {
        if let hostID {
            if let hostConversationID { openHostedConversation?(hostID, hostConversationID) }
            else { openHostSchedules?(hostID) }
        } else if let conversationID { openConversation?(conversationID) }
    }
}

extension ConversationNotifications: UNUserNotificationCenterDelegate {
    nonisolated func userNotificationCenter(_ center: UNUserNotificationCenter,
                                           didReceive response: UNNotificationResponse) async {
        let info = response.notification.request.content.userInfo
        await openNotification(hostID: info["hostID"] as? String,
            hostConversationID: info["hostConversationID"] as? String,
            conversationID: info["conversationID"] as? String)
    }

    nonisolated func userNotificationCenter(_ center: UNUserNotificationCenter,
                                           willPresent notification: UNNotification) async -> UNNotificationPresentationOptions {
        // receive() already suppresses the currently viewed conversation according
        // to preferences; work in another chat may still need an alert in-app.
        notification.request.content.sound == nil ? [.banner, .list] : [.banner, .list, .sound]
    }
}

struct ConversationNotificationSettingsView: View {
    @Bindable var notifications: ConversationNotifications
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        Form {
            Picker("Delivery", selection: Binding(get: { notifications.preferences.mode }, set: { mode in
                Task { await notifications.setMode(mode) }
            })) {
                ForEach(ConversationNotificationPreferences.Mode.allCases) { Text($0.title).tag($0) }
            }.disabled(notifications.requestingPermission)
            Toggle("Completed work", isOn: $notifications.preferences.completion)
            Toggle("Approvals and questions", isOn: $notifications.preferences.input)
            Toggle("Also notify while viewing the conversation", isOn: $notifications.preferences.whileViewingConversation)
            Text("Live conversations in Memex generate alerts showing the conversation title and event type, without transcript text.")
                .font(.caption).foregroundStyle(.secondary)
            if let error = notifications.error { Text(error).font(.caption).foregroundStyle(.red) }
            HStack { Spacer(); Button("Done") { dismiss() }.keyboardShortcut(.defaultAction) }
        }
        .formStyle(.grouped).padding().frame(width: 470)
    }
}
