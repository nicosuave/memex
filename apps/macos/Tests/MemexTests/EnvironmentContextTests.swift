import Testing
@testable import Memex

@Test func environmentContextIsCollapsedWithInstructionsWithoutHidingUserRequests() {
    func record(_ id: String, _ text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: "user", text: text, toolName: nil, toolInput: nil, toolOutput: nil))
    }
    let environment = "<environment_context>\n<current_date>2026-09-07</current_date>\n<timezone>America/Los_Angeles</timezone>\n<filesystem><workspace_roots>...</workspace_roots></filesystem>\n</environment_context>"
    let context = record("context", environment)
    let request = record("request", "Please fix the toolbar")
    let items = TranscriptItem.group([context, request])
    #expect(items.count == 2)
    #expect(items[0].isInstructions)
    #expect(items[0].activities[0].title == "Environment context")
    #expect(items[0].activities[0].body == environment)
    #expect(!items[1].isInstructions)
    for text in ["Explain this:\n" + environment, environment + "\nPlease fix this", "Environment Context is the heading I want", "<environment_context>unfinished"] {
        #expect(!record("user", text).record.isInstruction)
    }
}
