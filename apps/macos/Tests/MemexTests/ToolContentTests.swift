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
        let reveal = try #require(cell.subviews.compactMap { $0 as? NSButton }.first { $0.title == "Show raw content" })
        cell.layoutSubtreeIfNeeded()
        let bodyView = try #require(cell.subviews.compactMap { $0 as? NSTextView }.first)
        #expect(bodyView.frame.minY == 62)
        #expect(reveal.frame.maxY < bodyView.frame.minY)
        reveal.performClick(nil)
        #expect(controller.measurement(at: 0).attributedBody.string == source)
        controller.toggleRaw(controller.rows[0].id)
        let hit = try #require(ConversationMatcher.matches([record], query: payload).first)
        controller.update(sessionID: "tool", records: [record], provider: "codex", findQuery: payload, findHit: hit, findGeneration: 1)
        #expect(controller.measurement(at: 0).attributedBody.string == source)
        #expect(controller.selectedFindRange == hit.range)
        controller.update(sessionID: "tool", records: [record], provider: "codex", findGeneration: 2)
        #expect(!controller.measurement(at: 0).attributedBody.string.contains(payload))
    }

    private func entry(_ id: String, input: String? = nil, output: String? = nil) -> TranscriptRecord {
        TranscriptRecord(recordID: id, record: Message(role: input == nil ? "tool_result" : "tool_use", text: "",
            toolName: "exec", toolInput: input, toolOutput: output))
    }
}
