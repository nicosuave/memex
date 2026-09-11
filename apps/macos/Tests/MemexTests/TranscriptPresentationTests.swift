import Foundation
import Testing
@testable import Memex

@Test func olderAgentsHeadingWithoutPathStillCollapsesAndPreservesRequest() {
    let source = presentationRecord("older", "user", "# AGENTS.md instructions\n\n<INSTRUCTIONS>\nLong setup\n</INSTRUCTIONS>\nActual request")
    let projected = TranscriptPresentation.project([source])
    #expect(projected.count == 2)
    #expect(projected[0].record.contextLabel == "Project instructions")
    #expect(projected[1].record.text.trimmingCharacters(in: .whitespacesAndNewlines) == "Actual request")
    #expect(projected[0].rawTranscriptBody == source.rawTranscriptBody)
}

private func presentationRecord(_ id: String, _ role: String, _ text: String, turn: String? = nil,
                                phase: String? = nil, lifecycle: String? = nil) -> TranscriptRecord {
    TranscriptRecord(recordID: id, record: Message(role: role, text: text, toolName: nil,
        toolInput: nil, toolOutput: nil, sourceTurnID: turn, assistantPhase: phase, lifecycleEvent: lifecycle))
}

@Test func mixedInjectedContextPreservesRequestAndSourceMapping() {
    let text = "<recommended_plugins>plugins</recommended_plugins>\n# AGENTS.md instructions for /repo\n\n<INSTRUCTIONS>rules</INSTRUCTIONS><environment_context>env</environment_context>\nFix the toolbar"
    let original = presentationRecord("source", "user", text)
    let projected = TranscriptPresentation.project([original])
    #expect(projected.count == 4)
    #expect(projected.dropLast().allSatisfy { $0.record.isInstruction })
    #expect(projected.last?.record.text == "\nFix the toolbar")
    #expect(projected.last?.id == "source")
    #expect(projected.allSatisfy { $0.sourceID == "source" })
    #expect(projected.map(\.record.text).joined() == text)
    #expect(original.record.text == text)
    #expect(Set(projected.map(\.id)).count == 4)
    #expect(TranscriptPresentation.project(projected) == projected)
}

@Test func contextRecognitionDoesNotEatQuotesExamplesOrUnknownHTML() {
    for text in ["Explain <environment_context>x</environment_context>",
                 "```xml\n<environment_context>x</environment_context>\n```",
                 "> <recommended_plugins>x</recommended_plugins>",
                 "<div>hello</div>", "<environment_context>unfinished",
                 "# AGENTS.md instructions for /repo\nDiscuss this example"] {
        let original = presentationRecord("a", "user", text)
        #expect(TranscriptPresentation.project([original]) == [original])
    }
}

@Test func completedWorkRequiresExplicitCompletionAndFinalForSameTurn() {
    let commentary = presentationRecord("c", "assistant", "Working", turn: "t", phase: "commentary")
    let tool = presentationRecord("tool", "tool_use", "read", turn: "t")
    let final = presentationRecord("f", "assistant", "Done", turn: "t", phase: "final_answer")
    let completion = presentationRecord("done", "lifecycle", "", turn: "t", lifecycle: "task_complete")
    let items = TranscriptItem.group([commentary, tool, final, completion])
    #expect(items[0].isCompletedWork)
    #expect(items[0].records.map(\.id) == ["c", "tool"])
    #expect(!items[1].isCompletedWork)
    #expect(items[1].records[0].id == "f")
    #expect(items.flatMap(\.records).map(\.id) == ["c", "tool", "f"])
    for incomplete in [[commentary, tool, final], [commentary, tool, completion],
                       [commentary, tool, final, presentationRecord("x", "lifecycle", "", turn: "other", lifecycle: "task_complete")],
                       [commentary, tool, final, completion, presentationRecord("a", "lifecycle", "", turn: "t", lifecycle: "turn_aborted")]] {
        #expect(!TranscriptItem.group(incomplete).contains { $0.isCompletedWork })
    }
}

@Test func completedWorkDoesNotCrossUnclassifiedMessagesOrTurns() {
    let records = [presentationRecord("a", "assistant", "work", turn: "t", phase: "commentary"),
                   presentationRecord("u", "user", "another request"),
                   presentationRecord("b", "assistant", "work", turn: "t", phase: "commentary"),
                   presentationRecord("f", "assistant", "done", turn: "t", phase: "final_answer"),
                   presentationRecord("e", "lifecycle", "", turn: "t", lifecycle: "task_complete")]
    let items = TranscriptItem.group(records)
    #expect(items[0].isCompletedWork)
    #expect(!items[1].isCompletedWork)
    #expect(items[2].isCompletedWork)
    #expect(items.flatMap(\.records).map(\.id) == ["a", "u", "b", "f"])
}

@Test func routineTurnEventsAreHiddenWithoutHidingMessagesOrJoiningTurns() {
    let records = [presentationRecord("a", "tool_use", "first turn"),
                   presentationRecord("done", "lifecycle", "Turn completed", lifecycle: "task_complete"),
                   presentationRecord("start", "lifecycle", "Turn started", lifecycle: "task_started"),
                   presentationRecord("b", "tool_use", "second turn"),
                   presentationRecord("user", "user", "Turn completed"),
                   presentationRecord("abort", "lifecycle", "Turn interrupted", lifecycle: "turn_aborted")]
    let items = TranscriptItem.group(records)
    #expect(items.map { $0.records.map(\.id) } == [["a"], ["b"], ["user"], ["abort"]])
    #expect(TranscriptPresentation.project(records) == records)
}

@Test func rawTranscriptRetainsUnknownFieldsAndOriginalMixedRecord() throws {
    let json = #"{"record_id":"r","record":{"role":"user","text":"<environment_context>env</environment_context>request","future":{"answer":42,"flag":true}},"extra":[null,"retained"]}"#
    let record = try JSONDecoder().decode(TranscriptRecord.self, from: Data(json.utf8))
    let original = try JSONSerialization.jsonObject(with: Data(json.utf8)) as? NSDictionary
    let decoded = try JSONSerialization.jsonObject(with: Data(record.rawTranscriptBody.utf8)) as? NSDictionary
    #expect(original == decoded)
    #expect(TranscriptPresentation.project([record]).allSatisfy { $0.rawTranscriptBody == record.rawTranscriptBody })
}

@Test func recordedQuestionRepliesShowTheActualAnswerAndKeepQuestionAndRawPayload() {
    let source = #"<send_user_message_question_reply>[{"questionItemId":"q1","question":"Which environment?","answer":"Use staging"}]</send_user_message_question_reply>"#
    let original = presentationRecord("reply", "user", source)
    let projected = TranscriptPresentation.project([original])
    #expect(projected.count == 2)
    #expect(projected[0].record.isInstruction)
    #expect(projected[0].record.contextLabel == "Question")
    #expect(projected[1].record.text == "Use staging")
    #expect(projected[1].id == "reply")
    #expect(projected.allSatisfy { $0.rawTranscriptBody == original.rawTranscriptBody })
    #expect(TranscriptPresentation.project(projected) == projected)
    let quoted = presentationRecord("quoted", "user", "Explain this: " + source)
    #expect(TranscriptPresentation.project([quoted]) == [quoted])
}

private let memoryCitationFixture = """
<oai-mem-citation>
<citation_entries>
MEMORY.md:1-2|note=[prior context]
</citation_entries>
<rollout_ids>
019c6e27-e55b-73d1-87d8-4e01f1f75043
</rollout_ids>
</oai-mem-citation>
"""

@Test func assistantTransportMarkersDisappearWithoutChangingSourceOrAnswer() {
    let text = "The answer :codex-annotation{index=\"1\"} is here.\n\n" + memoryCitationFixture
    let original = presentationRecord("answer", "assistant", text)
    let projected = TranscriptPresentation.project([original])
    #expect(projected.count == 1)
    #expect(projected[0].record.text == "The answer  is here.")
    #expect(projected[0].id == original.id)
    #expect(projected[0].sourceID == original.sourceID)
    #expect(projected[0].rawTranscriptBody == original.rawTranscriptBody)
    #expect(TranscriptPresentation.project(projected) == projected)
    #expect(original.record.text == text)
}

@Test func assistantTransportProjectionPreservesLiteralCodeQuotesAndUnknownTags() {
    let annotation = ":codex-annotation{index=\"1\"}"
    for text in ["`" + annotation + "`", "```text\n" + annotation + "\n```",
                 "~~~xml\n" + memoryCitationFixture + "\n~~~",
                 "> " + annotation, "    " + annotation,
                 "\"" + annotation + "\"", "'" + annotation + "'",
                 "<div>ordinary HTML</div>", "<oai-mem-citation>unfinished",
                 memoryCitationFixture + "\nThis is an explanation of that format.",
                 ":codex-annotation{other=\"1\"}", ":codex-annotation{index=\"one\"}",
                 "<oai-mem-citation><unexpected>content</unexpected></oai-mem-citation>"] {
        let original = presentationRecord("example", "assistant", text)
        #expect(TranscriptPresentation.project([original])[0].record.text == text)
    }
    let user = presentationRecord("user", "user", "Inspect " + annotation + "\n" + memoryCitationFixture)
    #expect(TranscriptPresentation.project([user]) == [user])
}

@Test func assistantTransportProjectionHandlesMultipleAnnotationsAndMultilineCode() {
    let annotation = "::codex-annotation{index=\"12\"}"
    let text = "First" + annotation + ". Second" + annotation + "."
    #expect(TranscriptPresentation.project([presentationRecord("a", "assistant", text)])[0].record.text == "First. Second.")
    let code = "`multiline\n:codex-annotation{index=\"1\"}\nexample`"
    #expect(TranscriptPresentation.project([presentationRecord("a", "assistant", code)])[0].record.text == code)
}
