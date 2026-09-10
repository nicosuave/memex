import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct ToolContentTests {
    @Test func jsonFieldsRenderProseAndHideOpaquePayloadWithoutLosingSource() throws {
        let payload = "gAAAAA" + String(repeating: "Az19_-", count: 100)
        let data = try JSONSerialization.data(withJSONObject: ["target": "packaging", "message": payload])
        let source = try #require(String(data: data, encoding: .utf8))
        let record = entry("call", input: source)
        let rendered = ToolContentRenderer.render([record]).string
        #expect(rendered.contains("target\npackaging"))
        #expect(rendered.contains("Encoded payload · 606 bytes"))
        #expect(!rendered.contains(payload))
        #expect(ToolContentRenderer.render([record], raw: true).string == source)
        #expect(ConversationMatcher.matches([record], query: payload).count == 1)
    }

    @Test func codeIsLiteralAndResultWrapperDecodesMultilineOutput() {
        let code = "text(await tools.exec_command({cmd: `echo **literal**`}));\nnext();"
        let output = #"{"exit_code":0,"output":"First line\nSecond line\n"}"#
        let records = [entry("call", input: code), entry("result", output: "Script running with cell ID 103\nOutput:\n" + output)]
        let rendered = ToolContentRenderer.render(records).string
        #expect(rendered.hasPrefix("Input\n" + code))
        #expect(rendered.contains("Output\nScript running with cell ID 103\nOutput:\n"))
        #expect(rendered.contains("output\nFirst line\nSecond line\n"))
        #expect(!rendered.contains(#"\nSecond"#))
        #expect(ToolContentRenderer.render(records, raw: true).string.contains(output))
    }

    @Test func tighterSpacingKeepsAllExecutionFieldsVisible() {
        let source = #"{"chunk_id":"4c7793","exit_code":0,"original_token_count":0,"output":"","wall_time_seconds":0.32208491700000003}"#
        let record = entry("result", output: source)
        let rendered = ToolContentRenderer.render([record]).string
        for key in ["chunk_id", "exit_code", "original_token_count", "output", "wall_time_seconds"] {
            #expect(rendered.contains(key + "\n"))
        }
        #expect(rendered.contains("4c7793"))
        #expect(rendered.contains("0.322084917"))
        #expect(ToolContentRenderer.render([record], raw: true).string == source)
    }

    @Test func formattedToolLayoutTrimsTrailingSpaceWhileRawRetainsSource() {
        let source = "{\"chunk_id\":\"sample\",\"exit_code\":0,\"output\":\"done\\n\\n\"}\n\n"
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 500)
        controller.update(sessionID: "spacing", records: [entry("result", output: source)], provider: "codex")
        controller.toggle(controller.rows[0].id)
        let formatted = controller.measurement(at: 0).attributedBody
        #expect(formatted.string.hasSuffix("done"))
        #expect(formatted.string.contains("chunk_id\nsample"))
        let key = (formatted.string as NSString).range(of: "exit_code")
        let style = formatted.attribute(.paragraphStyle, at: key.location, effectiveRange: nil) as? NSParagraphStyle
        #expect(style?.paragraphSpacingBefore == 4)
        controller.toggleRaw(controller.rows[0].id)
        #expect(controller.measurement(at: 0).attributedBody.string == source)
    }

    @Test func formattedSectionSpacingDoesNotAccumulateBlankParagraphs() throws {
        let records = [entry("call", input: "echo hello\n\n\n"), entry("result", output: #"{"exit_code":0,"output":"hello"}"#)]
        let rendered = ToolContentRenderer.render(records)
        #expect(rendered.string == "Input\necho hello\nOutput\nexit_code\n0\noutput\nhello\n")
        let source = rendered.string as NSString
        let range = source.range(of: "echo hello")
        let style = try #require(rendered.attribute(.paragraphStyle, at: range.location, effectiveRange: nil) as? NSParagraphStyle)
        #expect(style.paragraphSpacing == 0)
        #expect(ToolContentRenderer.render(records, raw: true).string.contains("echo hello\n\n\n"))
    }

    @Test func proseFieldsRenderMarkdownAndOrdinaryLongTextIsNeverCollapsed() throws {
        let text = "A **readable** message.\n\n" + String(repeating: "ordinary words ", count: 100)
        let data = try JSONSerialization.data(withJSONObject: ["message": text, "options": ["count": 2], "empty": []])
        let source = try #require(String(data: data, encoding: .utf8))
        let rendered = ToolContentRenderer.render([entry("call", input: source)]).string
        #expect(rendered.contains("A readable message."))
        #expect(rendered.contains(String(repeating: "ordinary words ", count: 99)))
        #expect(rendered.contains("options.count\n2"))
        #expect(rendered.contains("empty\n[]"))
        #expect(!ToolContentRenderer.isOpaque(String(repeating: "readable words ", count: 100)))
    }

    @Test func rawDisclosureAndFindRevealExactEncodedSource() throws {
        let payload = String(repeating: "Ab12_-", count: 100)
        let source = "{\"target\":\"packaging\",\"message\":\"\(payload)\"}"
        let record = entry("call", input: source)
        let controller = TranscriptController()
        controller.view.frame = NSRect(x: 0, y: 0, width: 700, height: 500)
        controller.update(sessionID: "tool", records: [record], provider: "codex")
        controller.toggle(controller.rows[0].id)
        #expect(!controller.measurement(at: 0).attributedBody.string.contains(payload))
        let cell = try #require(controller.table.view(atColumn: 0, row: 0, makeIfNecessary: true))
        #expect(!cell.subviews.compactMap { $0 as? NSButton }.contains { $0.title == "Raw" || $0.title == "Source" })
        cell.layoutSubtreeIfNeeded()
        let bodyView = try #require(cell.subviews.compactMap { $0 as? NSTextView }.first)
        #expect(bodyView.frame.minY == 44)
        controller.toggleRaw(controller.rows[0].id)
        #expect(controller.measurement(at: 0).attributedBody.string == source)
        controller.toggleRaw(controller.rows[0].id)
        let hit = try #require(ConversationMatcher.matches([record], query: payload).first)
        controller.update(sessionID: "tool", records: [record], provider: "codex", findQuery: payload, findHit: hit, findGeneration: 1)
        #expect(controller.measurement(at: 0).attributedBody.string == source)
        #expect(controller.selectedFindRange == hit.range)
        controller.update(sessionID: "tool", records: [record], provider: "codex", findGeneration: 2)
        #expect(!controller.measurement(at: 0).attributedBody.string.contains(payload))
    }

    @Test func adjacentExecutionResultsDecodeNestedJSONAndPreserveRaw() {
        let first = #"{"chunk_id":"one","output":"{\"conditions\":[{\"message\":\"failed } [\"}]}"}"#
        let second = #"{"chunk_id":"two","output":"log\nnext"}"#
        let source = "Script completed\nOutput:\n" + first + second
        let record = entry("result", output: source)
        let rendered = ToolContentRenderer.render([record]).string
        #expect(rendered.contains("conditions[0].message\nfailed } ["))
        #expect(rendered.contains("chunk_id\ntwo"))
        #expect(rendered.contains("output\nlog\nnext"))
        #expect(ToolContentRenderer.render([record], raw: true).string == source)
    }

    @Test func malformedAndOversizedResultsRemainLiteral() {
        for source in ["Output:\n{broken}", "Output:\n{\"output\":\"" + String(repeating: "x", count: 256_000) + "\"}"] {
            #expect(ToolContentRenderer.render([entry("result", output: source)]).string == source)
        }
    }

    private func entry(_ id: String, input: String? = nil, output: String? = nil) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: input == nil ? "tool_result" : "tool_use", text: "",
            toolName: "exec", toolInput: input, toolOutput: output))
    }
}
