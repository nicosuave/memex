import Foundation
import Testing
@testable import Memex

@Suite @MainActor struct ConversationNotificationTests {
    private let session = Session(source: "codex", sessionID: "one", sourcePath: "/one.jsonl", project: "project", label: "Task")

    private func enabled() -> ConversationNotifications {
        let controller = ConversationNotifications(deliver: { _, _ in })
        controller.preferences.mode = .notifications
        return controller
    }

    @Test func doesNotAnnouncePastHistoryOrStoppedWork() {
        let controller = enabled()
        #expect(controller.receive(session, state: .init(activity: .completed)) == nil)
        #expect(controller.receive(session, state: .init(activity: .starting)) == nil)
        #expect(controller.receive(session, state: .init(activity: .completed)) == nil)
        #expect(controller.receive(session, state: .init(activity: .working)) == nil)
        #expect(controller.receive(session, state: .init(activity: .stopping)) == nil)
        #expect(controller.receive(session, state: .init(activity: .completed)) == nil)
        #expect(controller.receive(session, state: .init(activity: .stopped)) == nil)
    }

    @Test func completionIsOncePerObservedRunIncludingAnInputPause() {
        let controller = enabled()
        _ = controller.receive(session, state: .init(activity: .working))
        #expect(controller.receive(session, state: .init(activity: .question, requestIDs: ["question"]))?.kind == .question)
        #expect(controller.receive(session, state: .init(activity: .completed))?.kind == .completion)
        #expect(controller.receive(session, state: .init(activity: .completed)) == nil)
        _ = controller.receive(session, state: .init(activity: .working))
        #expect(controller.receive(session, state: .init(activity: .completed))?.kind == .completion)
    }

    @Test func pendingRequestsDeduplicateByIdentityAcrossSnapshotRefreshes() {
        let controller = enabled()
        #expect(controller.receive(session, state: .init(activity: .approval, requestIDs: ["a"]))?.kind == .approval)
        #expect(controller.receive(session, state: .init(activity: .approval, requestIDs: ["a"])) == nil)
        _ = controller.receive(session, state: .init(activity: .working))
        #expect(controller.receive(session, state: .init(activity: .approval, requestIDs: ["a"])) == nil)
        #expect(controller.receive(session, state: .init(activity: .approval, requestIDs: ["b"]))?.kind == .approval)
        // Provider namespaces can reuse an ID between approval and question types.
        #expect(controller.receive(session, state: .init(activity: .question, requestIDs: ["b"]))?.kind == .question)
    }

    @Test func preferencesSuppressOutputWithoutReplayingOldEventsOnEnable() {
        let controller = ConversationNotifications(deliver: { _, _ in })
        #expect(controller.preferences.mode == .off)
        _ = controller.receive(session, state: .init(activity: .working))
        #expect(controller.receive(session, state: .init(activity: .completed)) == nil)
        controller.preferences.mode = .sound
        #expect(controller.receive(session, state: .init(activity: .completed)) == nil)
        controller.preferences.completion = false
        _ = controller.receive(session, state: .init(activity: .working))
        #expect(controller.receive(session, state: .init(activity: .completed)) == nil)
        controller.preferences.input = false
        #expect(controller.receive(session, state: .init(activity: .question, requestIDs: ["a"])) == nil)
        controller.preferences.input = true
        #expect(controller.receive(session, state: .init(activity: .question, requestIDs: ["a"])) == nil)
        #expect(controller.receive(session, state: .init(activity: .question, requestIDs: ["b"]))?.kind == .question)
    }

    @Test func viewingSuppressionIsPerConversationAndCanBeOverridden() {
        let controller = enabled()
        controller.isConversationVisible = { $0 == self.session.id }
        #expect(controller.receive(session, state: .init(activity: .approval, requestIDs: ["a"])) == nil)
        let other = Session(source: "claude", sessionID: "other", sourcePath: "/other", project: "project")
        #expect(controller.receive(other, state: .init(activity: .question, requestIDs: ["a"]))?.kind == .question)
        controller.preferences.whileViewingConversation = true
        #expect(controller.receive(session, state: .init(activity: .approval, requestIDs: ["b"]))?.kind == .approval)
    }

    @Test func preferencesSurviveRelaunch() throws {
        let name = "Memex-notifications-\(UUID())"
        let defaults = try #require(UserDefaults(suiteName: name))
        defer { defaults.removePersistentDomain(forName: name) }
        let controller = ConversationNotifications(defaults: defaults, deliver: { _, _ in })
        controller.preferences.mode = .sound
        controller.preferences.completion = false
        controller.preferences.whileViewingConversation = true
        let restored = ConversationNotifications(defaults: defaults, deliver: { _, _ in })
        #expect(restored.preferences == controller.preferences)
    }
}
