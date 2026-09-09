import AppKit
import Testing
@testable import Memex

@Test func onlyTypedRepresentedImagesSuppressStandalonePlaceholders() {
    let text = "<<ImageDisplayed>>\nWhat changed?\n<<ImageDisplayed>>"
    var message = Message(role: "user", text: text, toolName: nil, toolInput: nil, toolOutput: nil)
    #expect(SourceContent.displayText(message) == text)
    message.sourceContent = #"[{"type":"input_image","image_url":"https://example.com/image.png"}]"#
    #expect(SourceContent.displayText(message) == "What changed?\n<<ImageDisplayed>>")
    #expect(message.text == text)
    message.text = "Explain the <<ImageDisplayed>> marker"
    #expect(SourceContent.displayText(message) == message.text)
    message.sourceContent = #"[{"type":"text","text":"image_url: https://example.com/image.png"}]"#
    message.text = text
    #expect(SourceContent.displayText(message) == text)
}

@Test func typedDocumentsAndImagePathsRetainTheirIdentity() {
    let message = Message(role: "user", text: "Look at these", toolName: nil, toolInput: nil, toolOutput: nil,
        sourceContent: #"[{"type":"localImage","path":"/tmp/photo.png"},{"type":"input_file","file_url":"/tmp/report.pdf","filename":"Report"} ,{"type":"document","source":{"type":"text","data":"literal **source**"}}]"#)
    let blocks = SourceContent.blocks(message)
    #expect(blocks.count == 3)
    #expect(blocks[0] == .attachment(label: "Image", source: "/tmp/photo.png", image: true))
    #expect(blocks[1] == .attachment(label: "Report", source: "/tmp/report.pdf", image: false))
    #expect(blocks[2] == .code("literal **source**", language: "text"))
}

@Test func placeholderProjectionKeepsOriginalRawRecordAndStableID() {
    let message = Message(role: "user", text: "<<ImageDisplayed>>\nQuestion", toolName: nil, toolInput: nil, toolOutput: nil,
        sourceContent: #"[{"type":"input_image","image_url":"/tmp/image.png"}]"#)
    let original = TranscriptRecord(recordID: "image", record: message)
    let projected = TranscriptPresentation.project([original])
    #expect(projected.first?.id == "image")
    #expect(projected.first?.record.text == "Question")
    #expect(projected.first?.rawTranscriptBody == original.rawTranscriptBody)
    #expect(TranscriptPresentation.project(projected) == projected)
}

@MainActor @Test func imageOnlyMessageGetsVisibleNativeContentAndRawFallback() {
    let source = #"[{"type":"input_image","image_url":"https://example.com/image.png"}]"#
    let record = TranscriptRecord(recordID: "image", record: Message(role: "user", text: "", toolName: nil,
        toolInput: nil, toolOutput: nil, sourceContent: source))
    let reader = TranscriptController()
    reader.view.frame = NSRect(x: 0, y: 0, width: 700, height: 500)
    reader.update(sessionID: "image", records: [record], provider: "codex")
    let value = reader.measurement(at: 0)
    #expect(value.richContent != nil)
    #expect(value.hasBody)
    #expect(value.textHeight >= 70)
    #expect(value.contentWidth > 200)
    reader.update(sessionID: "image", records: [record], provider: "codex", rawTranscript: true)
    #expect(reader.measurement(at: 0).richContent == nil)
    #expect(reader.measurement(at: 0).body.contains("source_content"))
}

@MainActor @Test func interruptedToolRemainsVisibleAmongRoutineOperations() {
    let records = ["a", "b", "c"].map { id in
        TranscriptRecord(recordID: id, record: Message(role: "tool_result", text: "", toolName: "bash", toolInput: nil,
            toolOutput: id == "b" ? #"{"status":"cancelled","output":"Interrupted by request"}"# : #"{"output":"routine"}"#))
    }
    let reader = TranscriptController()
    reader.update(sessionID: "statuses", records: records, provider: "codex")
    #expect(reader.rows.count == 3)
    #expect(reader.measurement(at: 1).title.hasPrefix("Cancelled"))
}

@MainActor @Test func embeddedAndUnavailableDocumentsHaveVisibleCardsAndPreserveRawPayload() throws {
    let sources = [
        #"[{"type":"document","title":"Report.pdf","source":{"type":"base64","media_type":"application/pdf","data":"JVBERi0xLjQ="}}]"#,
        #"[{"type":"input_file","filename":"Report.pdf","file_data":"data:application/pdf;base64,JVBERi0xLjQ="}]"#,
        #"[{"type":"input_file","filename":"Report.pdf","file_id":"file-123"}]"#,
        #"[{"type":"document","title":"Report.pdf","source":{"type":"base64","media_type":"application/pdf","data":"invalid"}}]"#,
        #"[{"type":"document","title":"Report.pdf","source":{"type":"unknown"}}]"#
    ]
    for source in sources {
        let record = TranscriptRecord(recordID: "document", record: Message(role: "user", text: "", toolName: nil,
            toolInput: nil, toolOutput: nil, sourceContent: source))
        let reader = TranscriptController()
        reader.view.frame = NSRect(x: 0, y: 0, width: 700, height: 500)
        reader.update(sessionID: source, records: [record], provider: "claude")
        let measurement = reader.measurement(at: 0)
        let rich = try #require(measurement.richContent)
        #expect(measurement.hasBody)
        #expect(measurement.textHeight >= 70)
        let card = try #require(rich.subviews.first as? AttachmentContentView)
        let labels = card.subviews.compactMap { ($0 as? NSTextField)?.stringValue }
        #expect(labels.contains("Report.pdf"))
        #expect(labels.contains { $0.contains("raw transcript") || $0.contains("provider") })
        #expect(card.subviews.compactMap { $0 as? NSButton }.allSatisfy { $0.isHidden })
        #expect(record.record.sourceContent == source)
        reader.update(sessionID: source, records: [record], provider: "claude", rawTranscript: true)
        #expect(reader.measurement(at: 0).body.contains("source_content"))
        #expect(reader.measurement(at: 0).body.contains("Report.pdf"))
    }
}
