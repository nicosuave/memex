import Foundation
import Testing
@testable import Memex

struct ActivityPresentationTests {
    @Test func knownProviderToolsUseStructuredArguments() {
        let examples = [
            ("Read", #"{"file_path":"/project/settings.swift"}"#, "Read /project/settings.swift", "doc.text"),
            ("mcp__filesystem__read_file", #"{"path":"README.md"}"#, "Read README.md", "doc.text"),
            ("Grep", #"{"pattern":"TranscriptActivity"}"#, "Search TranscriptActivity", "magnifyingglass"),
            ("functions.exec_command", #"{"cmd":"git status --short"}"#, "Run git status --short", "terminal"),
            ("Bash", #"{"command":"swift test"}"#, "Run swift test", "terminal"),
            ("exec", #"{"cmd":"swift test"}"#, "Run swift test", "terminal"),
            ("Write", #"{"file_path":"new.swift","content":"ignored"}"#, "Write new.swift", "doc.badge.plus"),
            ("MultiEdit", #"{"file_path":"Models.swift"}"#, "Edit Models.swift", "pencil"),
            ("Glob", #"{"pattern":"**/*.swift"}"#, "List files **/*.swift", "folder")
        ]
        for (name, input, title, symbol) in examples {
            let presentation = activity(name, input: input).presentation
            #expect(presentation.title == title)
            #expect(presentation.symbolName == symbol)
            #expect(!presentation.hasFailure)
        }
    }

    @Test func unknownAndExecutableInputsAreNeverInterpreted() {
        let unknown = activity("custom_magic", input: #"{"command":"delete everything","path":"secret"}"#)
        #expect(unknown.presentation.title == "Custom Magic")
        #expect(unknown.presentation.category == nil)
        let executable = activity("functions.exec", input: "await tools.exec_command({cmd: 'swift test'})")
        #expect(executable.presentation.title == "Run tool script")
    }

    @Test func malformedOrMissingArgumentsHaveSafeFallbacks() {
        for input in ["not json", "[1, 2]", #"{"cmd":42}"#, #"{"cmd":"  "}"#, "null"] {
            #expect(activity("exec_command", input: input).presentation.title == "Run")
        }
        #expect(activity("Read").presentation.title == "Read")
        #expect(activity(nil).presentation.title == "Tool activity")
        #expect(activity("exec", input: #"{"cmd":42}"#).presentation.title == "Exec")
    }

    @Test func failureRequiresAnExplicitStructuredOutputStatus() {
        for output in [" \n\t{\"exit_code\":2}", "\u{FEFF}{\"exit_code\":2}", #"{"isError":true}"#, #"{"is_error":true}"#, #"{"exit_code":1}"#, #"{"exit_code":-9}"#] {
            #expect(activity("Bash", output: output).presentation.hasFailure)
            #expect(activity("Bash", output: output).presentation.title == "Failed · Run")
        }
        for output in ["error: something failed", #"{"error":"failed"}"#, #"{"isError":false,"exit_code":0}"#,
                       #"{"is_error":1}"#, #"{"exit_code":true}"#, #"{"exit_code":"1"}"#,
                       #"{"data":{"isError":true}}"#] {
            #expect(!activity("Bash", output: output).presentation.hasFailure)
        }
        #expect(!activity("Bash", input: #"{"isError":true,"exit_code":2}"#).presentation.hasFailure)
        let result = TranscriptRecord(recordID: "result", record: Message(role: "tool_result", text: #"{"exit_code":2}"#,
            toolName: "Bash", toolInput: nil, toolOutput: nil))
        #expect(TranscriptActivity(records: [result]).presentation.hasFailure)
        #expect(activity("custom_magic", output: #"{"isError":true}"#).presentation.title == "Failed · Custom Magic")
    }

    @Test func summariesPreserveSourceAndBoundDetails() {
        let input = "{\"cmd\":\"echo hello\\n" + String(repeating: "x", count: 160) + "\"}"
        let original = activity("Bash", input: input)
        let body = original.body
        #expect(original.presentation.title.hasPrefix("Run echo hello "))
        #expect(original.presentation.title.count <= 92)
        #expect(original.presentation.title.hasSuffix("…"))
        #expect(original.records[0].record.toolInput == input)
        #expect(original.body == body)
    }

    @MainActor @Test func decodedClaudeEnvelopeFailuresStayVisibleWithPlainTextOutput() throws {
        let data = Data(#"[{"record_id":"a","record":{"role":"tool_result","text":"ok","tool_output":"ok","tool_result_is_error":false}},{"record_id":"b","record":{"role":"tool_result","text":"permission denied","tool_output":"permission denied","tool_result_is_error":true}},{"record_id":"c","record":{"role":"tool_result","text":"ordinary"}}]"#.utf8)
        let records = try JSONDecoder().decode([TranscriptRecord].self, from: data)
        #expect(!TranscriptActivity(records: [records[0]]).presentation.hasFailure)
        #expect(TranscriptActivity(records: [records[1]]).presentation.hasFailure)
        #expect(!TranscriptActivity(records: [records[2]]).presentation.hasFailure)
        let reader = TranscriptController()
        reader.update(sessionID: "claude-errors", records: records, provider: "claude")
        #expect(reader.rows.count == 3)
        #expect(reader.measurement(at: 1).title.hasPrefix("Failed"))
        #expect(records[1].rawTranscriptBody.contains("tool_result_is_error"))
    }

    private func activity(_ name: String?, input: String? = nil, output: String? = nil) -> TranscriptActivity {
        TranscriptActivity(records: [TranscriptRecord(recordID: "call", record: Message(role: "tool_use", text: "",
            toolName: name, toolInput: input, toolOutput: output))])
    }
}
