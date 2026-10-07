import Testing
import MemexExecutionHostCore
@testable import Memex

struct ExecutionScheduleDraftTests {
    let capabilities: Set<String> = ["schedules.runs", "schedules.new_conversation", "schedules.events", "schedules.wall_clock"]

    @Test func freshEventScheduleUsesExclusiveTargetAndTrigger() {
        var draft = ExecutionScheduleDraft()
        draft.text = "Inspect import"; draft.freshConversation = true
        draft.workspaceID = "workspace"; draft.provider = "codex"; draft.title = "Import review"
        draft.conversationID = "stale-conversation"; draft.kind = "event"; draft.eventName = "import.completed"
        draft.notificationPolicy = "never"
        #expect(draft.isValid(capabilities: capabilities))
        let value = draft.parameters(capabilities: capabilities)
        #expect(value["conversationId"] == nil)
        #expect(value["intervalSeconds"] == nil)
        #expect(value["wallClock"] == nil)
        #expect(value["newConversation"]?["workspaceId"] == .string("workspace"))
        #expect(value["eventName"] == .string("import.completed"))
        #expect(value["notificationPolicy"] == .string("never"))
        draft.freshConversation = false; draft.kind = "interval"
        let edited = draft.parameters(capabilities: capabilities)
        #expect(edited["newConversation"] == nil)
        #expect(edited["eventName"] == nil)
        #expect(edited["conversationId"] == .string("stale-conversation"))
    }

    @Test func editingReadsPersistedTargetCasingAndPolicy() {
        let saved: HostValue = .object(["id": .string("schedule"), "prompt": .string("Prompt"),
            "newConversation": .object(["workspaceID": .string("folder"), "provider": .string("claude"), "title": .string("Run")]),
            "eventName": .string("event"), "notificationPolicy": .string("all")])
        let draft = ExecutionScheduleDraft(schedule: saved)
        #expect(draft.workspaceID == "folder")
        #expect(draft.freshConversation)
        #expect(draft.kind == "event")
        #expect(draft.notificationPolicy == "all")
        #expect(!draft.isValid(capabilities: []))
    }

    @Test func legacyPayloadOmitsUnsupportedNotificationField() {
        var draft = ExecutionScheduleDraft()
        draft.conversationID = "chat"; draft.text = "Prompt"
        #expect(draft.isValid(capabilities: []))
        #expect(draft.parameters(capabilities: [])["notificationPolicy"] == nil)
    }
}
