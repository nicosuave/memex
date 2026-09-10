import Foundation
import Testing
@testable import Memex

@Test func decodesCLIContracts() throws {
    let sessionJSON = #"[{"source":"codex","session_id":"session-1","source_path":"/tmp/one.jsonl","project":"memex","last_at":"2026-09-07T17:03:01Z","label":"Native app","resume_cmd":"codex resume session-1"}]"#
    let sessions = try JSONDecoder().decode([Session].self, from: Data(sessionJSON.utf8))
    #expect(sessions[0].title == "Native app")
    #expect(sessions[0].date != nil)
    #expect(sessions[0].resumeCommand == "codex resume session-1")
    let recordJSON = #"[{"record_id":"r1","record":{"role":"assistant","text":"Hello","tool_name":null},"content":{"truncated":false},"machine":"local"}]"#
    let records = try JSONDecoder().decode([TranscriptRecord].self, from: Data(recordJSON.utf8))
    #expect(records[0].record.text == "Hello")
    #expect(!records[0].record.isActivity)
}

@Test func sessionUsesRepositoryProjectForItsDisplayName() throws {
    let json = #"{"source":"codex","session_id":"s","source_path":"/s","project":"worktree","repo_project":"memex"}"#
    let session = try JSONDecoder().decode(Session.self, from: Data(json.utf8))
    #expect(session.projectName == "memex")
}

private func record(_ id: String, _ role: String, _ tool: String? = nil) -> TranscriptRecord {
    TranscriptRecord(recordID: id, record: Message(role: role, text: id, toolName: tool, toolInput: nil, toolOutput: nil))
}

@Test func groupsOnlyConsecutiveActivityAndPreservesEveryRecord() {
    let input = [record("1", "system"), record("2", "user"), record("3", "assistant"),
                 record("4", "tool_use", "Read"), record("5", "tool_result"),
                 record("6", "reasoning"), record("7", "assistant"),
                 record("8", "tool_use", "Bash"), record("9", "tool_result")]
    let items = TranscriptItem.group(input)
    #expect(items.count == 6)
    #expect(items[3].title == "Read, Reasoning")
    #expect(items[3].records.count == 3)
    #expect(items[5].title == "Bash")
    #expect(items.flatMap(\.records).map(\.id) == input.map(\.id))
    #expect(TranscriptItem.group([]).isEmpty)
}

@Test func groupingAcrossLoadedPagesIsStable() {
    let first = [record("1", "assistant"), record("2", "tool_use", "Read")]
    let next = [record("3", "tool_result"), record("4", "tool_use", "Read"), record("5", "assistant")]
    let before = TranscriptItem.group(first)
    let after = TranscriptItem.group(first + next)
    #expect(before[1].id == after[1].id)
    #expect(after[1].title == "Read")
    #expect(after[1].records.count == 3)
}

@Test func unknownRolesAndUnnamedToolsRemainReadable() {
    let items = TranscriptItem.group([record("1", "future_role"), record("2", "tool_result")])
    #expect(items.count == 2)
    #expect(!items[0].isActivity)
    #expect(items[1].title == "Tool results")
}

@Test func identityIncludesProviderAndPath() {
    let first = Session(source: "codex", sessionID: "same", sourcePath: "/a", project: "p")
    let second = Session(source: "claude", sessionID: "same", sourcePath: "/a", project: "p")
    let third = Session(source: "codex", sessionID: "same", sourcePath: "/b", project: "p")
    #expect(Set([first.id, second.id, third.id]).count == 3)
}

@Test func searchUsesKnownTitleAndResumeWithoutReplacingItWithExcerpt() throws {
    let known = Session(source: "codex", sessionID: "s", sourcePath: "/a", project: "p", label: "My session", resumeCommand: "codex resume s")
    let hit = SearchHit(source: "codex", sessionID: "s", sourcePath: "/a", project: "p", snippet: "a matching excerpt", ts: "2026-09-07T17:03:01Z")
    let result = hit.session(known: [known.id: known])
    #expect(result.title == "My session")
    #expect(result.snippet == "a matching excerpt")
    #expect(result.resumeCommand == "codex resume s")
}

@Test func setupInstructionsCollapseTogetherWithoutSwallowingConversation() {
    let input = [record("1", "system"), record("2", "developer"), record("3", "developer"),
                 record("4", "user"), record("5", "tool_use", "Read"), record("6", "tool_result"),
                 record("7", "developer"), record("8", "assistant")]
    let items = TranscriptItem.group(input)
    #expect(items.count == 5)
    #expect(items[0].title == "Session context")
    #expect(items[0].records.count == 3)
    #expect(items[1].records[0].record.role == "user")
    #expect(items[2].isActivity)
    #expect(items[3].isInstructions)
    #expect(items.flatMap(\.records).map(\.id) == input.map(\.id))
}

@MainActor @Test func sessionScrollingClaimsOnlyOnePageUntilLoadingFinishes() {
    let store = Store()
    store.sessions = (0..<200).map {
        Session(source: "codex", sessionID: "\($0)", sourcePath: "/\($0)", project: "memex")
    }
    store.hasMoreSessions = true
    store.loadMoreSessionsIfNeeded(visibleID: store.sessions[0].id)
    #expect(store.sessionLimit == 200)
    for session in store.sessions.suffix(5) { store.loadMoreSessionsIfNeeded(visibleID: session.id) }
    #expect(store.sessionLimit == 400)
    store.loadingSessions = true
    store.hasMoreSessions = true
    store.loadMoreSessionsIfNeeded(visibleID: store.sessions[199].id)
    #expect(store.sessionLimit == 400)
}

@Test func toolLabelsAreReadableWithoutChangingProviderNames() {
    for (raw, expected) in [("exec", "Exec"), ("spawn_agent", "Spawn Agent"),
                            ("write_file", "Write File"), ("read-file", "Read File"),
                            ("Read", "Read"), ("HTTP_get", "HTTP Get")] {
        let value = Message(role: "tool_use", text: "", toolName: raw, toolInput: nil, toolOutput: nil)
        #expect(value.activityTitle == expected)
        #expect(value.toolName == raw)
    }
    let items = TranscriptItem.group([record("label-1", "tool_use", "exec"), record("label-2", "tool_use", "spawn_agent")])
    #expect(items[0].title == "Exec, Spawn Agent")
}

private func linkedRecord(_ id: String, _ role: String, _ invocation: String?,
                          tool: String? = "exec", text: String = "", input: String? = nil,
                          output: String? = nil) -> TranscriptRecord {
    TranscriptRecord(recordID: id, record: Message(role: role, text: text, toolName: tool,
                                                  toolInput: input, toolOutput: output,
                                                  eventID: role == "tool_use" ? invocation : nil,
                                                  parentToolUseID: role == "tool_result" ? invocation : nil))
}

@Test func pairsParallelRepeatedNamesByInvocationRatherThanResultOrder() {
    let records = [linkedRecord("a", "tool_use", "call-a"), linkedRecord("b", "tool_use", "call-b"),
                   linkedRecord("rb", "tool_result", "call-b"), linkedRecord("ra", "tool_result", "call-a")]
    let activities = TranscriptItem(records: records).activities
    #expect(activities.map(\.id) == ["a", "b"])
    #expect(activities.map { $0.records.map(\.id) } == [["a", "ra"], ["b", "rb"]])
    #expect(activities.flatMap(\.records).count == records.count)
    #expect(Set(activities.flatMap(\.records).map(\.id)) == Set(records.map(\.id)))
}

@Test func unlinkedAdjacentToolsPairWithoutCombiningConcurrentCalls() {
    let adjacent = [record("a", "tool_use", "Read"), record("ra", "tool_result")]
    #expect(TranscriptItem(records: adjacent).activities.map { $0.records.map(\.id) } == [["a", "ra"]])
    let parallel = [record("a", "tool_use", "Read"), record("b", "tool_use", "Read"),
                    record("rb", "tool_result", "Read"), record("ra", "tool_result", "Read")]
    #expect(TranscriptItem(records: parallel).activities.count == 4)
    let differentNames = [record("a", "tool_use", "Read"), record("ra", "tool_result", "Write")]
    #expect(TranscriptItem(records: differentNames).activities.count == 2)
}

@Test func conflictingAndAmbiguousLinksLeaveResultsVisible() {
    let conflicting = [linkedRecord("a", "tool_use", "call-a"), linkedRecord("rb", "tool_result", "call-b")]
    #expect(TranscriptItem(records: conflicting).activities.map(\.id) == ["a", "rb"])
    let orphan = TranscriptItem(records: [linkedRecord("orphan", "tool_result", "missing")]).activities
    #expect(orphan.count == 1)
    #expect(orphan[0].id == "orphan")
    let duplicates = [linkedRecord("a", "tool_use", "same"), linkedRecord("b", "tool_use", "same"),
                      linkedRecord("result", "tool_result", "same")]
    #expect(TranscriptItem(records: duplicates).activities.count == 3)
}

@Test func pairingDoesNotCrossReasoningOrConversationBoundaries() {
    for boundary in ["reasoning", "user", "assistant", "developer", "system", "tool"] {
        let input = [linkedRecord("a", "tool_use", "call-a"), record("boundary", boundary),
                     linkedRecord("ra", "tool_result", "call-a")]
        let activities = TranscriptItem.group(input).flatMap(\.activities)
        #expect(activities.count == 3)
        #expect(activities.flatMap(\.records).map(\.id) == input.map(\.id))
    }
}

@Test func pairingRecomputesAcrossPagesAndKeepsCallIdentity() {
    let call = linkedRecord("a", "tool_use", "call-a")
    let answer = linkedRecord("ra", "tool_result", "call-a")
    let first = TranscriptItem.group([call]).flatMap(\.activities)
    let appended = TranscriptItem.group([call, answer]).flatMap(\.activities)
    #expect(first[0].id == appended[0].id)
    #expect(appended[0].records.map(\.id) == ["a", "ra"])
    let orphanPage = TranscriptItem.group([answer]).flatMap(\.activities)
    #expect(orphanPage[0].id == "ra")
    #expect(appended[0].records.contains { $0.id == orphanPage[0].id })
    let withBoundary = TranscriptItem.group([call, record("u", "user"), answer]).flatMap(\.activities)
    #expect(withBoundary.count == 3)
}

@Test func decodesFlattenedProviderInvocationLinks() throws {
    // The Rust Claude and Codex parsers emit invocation event_id on calls and
    // parent_tool_use_id on results. source_tool_use_id instead describes ancestry.
    let json = #"[{"record_id":"a","record":{"role":"tool_use","text":"ls","tool_name":"exec","event_id":"call-1","parent_event_id":"assistant-uuid","source_tool_use_id":"ancestor"}},{"record_id":"r","record":{"role":"tool_result","text":"files","event_id":"result-uuid:tool_result:call-1","parent_event_id":"call-1","parent_tool_use_id":"call-1"}}]"#
    let records = try JSONDecoder().decode([TranscriptRecord].self, from: Data(json.utf8))
    #expect(records[0].record.eventID == "call-1")
    #expect(records[1].record.parentToolUseID == "call-1")
    #expect(TranscriptItem(records: records).activities.map { $0.records.map(\.id) } == [["a", "r"]])
}

@Test func activityBodyPreservesDistinctContentWithoutRepeatingProviderMirrors() {
    let call = linkedRecord("a", "tool_use", "call-a", text: "argument", input: "argument")
    let answer = linkedRecord("r", "tool_result", "call-a", text: "extra metadata", output: "full output")
    let activity = TranscriptItem(records: [call, answer]).activities[0]
    #expect(activity.body == "Input\nargument\n\nOutput\nfull output\n\nextra metadata")
    let echoed = linkedRecord("r", "tool_result", "call-a", text: "argument", output: "argument")
    #expect(TranscriptItem(records: [call, echoed]).activities[0].body == "Input\nargument\n\nOutput\nargument")
    let instruction = TranscriptItem(records: [record("instructions", "developer")]).activities[0]
    #expect(instruction.body == "instructions")
    #expect(instruction.title == "Developer instructions")
}

@Test func searchExcerptNeverBecomesConversationTitle() {
    let hit = SearchHit(source: "claude", sessionID: "s", sourcePath: "/a", project: "p",
                        snippet: "old_string: String(large tool payload)", ts: nil)
    let unknown = hit.session(known: [:])
    #expect(unknown.label == nil)
    #expect(unknown.title == "Untitled conversation")
    #expect(unknown.snippet == hit.snippet)
    var known = unknown
    known.label = "Investigate equality deletes"
    #expect(hit.session(known: [known.id: known]).title == "Investigate equality deletes")
}

@Test func openingTitleSkipsInjectedContextAndUsesRequestOrSubagentName() {
    func entry(_ role: String, _ text: String) -> TranscriptRecord {
        TranscriptRecord(recordID: text, record: Message(role: role, text: text, toolName: nil, toolInput: nil, toolOutput: nil))
    }
    let context = entry("user", "<recommended_plugins>plugins</recommended_plugins>\n<environment_context>environment</environment_context>")
    let agent = entry("developer", "<context_window>\nAgent name: /root/activity_backfill\n</context_window>")
    #expect(Session.openingTitle([agent, context]) == "Activity backfill")
    #expect(Session.openingTitle([context, entry("user", "Please fix\n the backfill")]) == "Please fix the backfill")
    #expect(Session.openingTitle([agent, context, entry("user", "Actual assignment")]) == "Actual assignment")
    #expect(Session.openingTitle([context]) == nil)
}
