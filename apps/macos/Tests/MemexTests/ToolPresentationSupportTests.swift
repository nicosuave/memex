import AppKit
import Testing
@testable import Memex

@Suite(.serialized) @MainActor
struct ToolPresentationSupportTests {
    @Test func patchHasDiffColorsAndKeepsEveryLine() throws {
        let patch = "*** Begin Patch\n@@ section\n-old\n+new\n unchanged\n*** End Patch"
        let result = try #require(ToolPresentationSupport.decorate(patch, path: "", toolName: "apply_patch", code: true))
        #expect(result.string == patch)
        let addition = (patch as NSString).range(of: "+new")
        #expect(result.attribute(.foregroundColor, at: addition.location, effectiveRange: nil) as? NSColor == .systemGreen)
    }

    @Test func searchResultsLinkFilesWithoutHidingMatchText() throws {
        let text = "/tmp/source.swift:42:8: matched text\nrelative.swift:1: another match"
        let result = try #require(ToolPresentationSupport.decorate(text, path: "output", toolName: "Grep", code: false))
        #expect(result.string == text)
        let destination = try #require(result.attribute(RichTextRenderer.sourceLocationAttribute, at: 0, effectiveRange: nil) as? String)
        #expect(destination == "/tmp/source.swift:42")
        #expect(result.attribute(.link, at: 0, effectiveRange: nil) == nil)
    }

    @Test func fileReadIsLiteralAndUnknownToolsKeepFallback() throws {
        let content = "# literal code\nlet value = **notMarkdown**"
        let result = try #require(ToolPresentationSupport.decorate(content, path: "text", toolName: "read_file", code: false))
        #expect(result.string == content)
        #expect(ToolPresentationSupport.decorate(content, path: "text", toolName: "custom", code: false) == nil)
    }

    @Test func toolRenderingRetainsAllFieldsAlongsideSpecializedContent() {
        let source = ##"{"path":"/tmp/file.swift","content":"# literal\nlet value = **literal**","metadata":{"count":2}}"##
        let entry = TranscriptRecord(recordID: "read", record: Message(role: "tool_result", text: "", toolName: "read_file", toolInput: nil, toolOutput: source))
        let result = ToolContentRenderer.render([entry])
        #expect(result.string.contains("content\n# literal\nlet value = **literal**"))
        #expect(result.string.contains("metadata.count\n2"))
        #expect(result.string.contains("path\n/tmp/file.swift"))
        #expect(ToolContentRenderer.render([entry], raw: true).string == source)
    }

    @Test func richToolCardsPreserveFieldsAndUseLiteralCode() {
        let source = #"{"cmd":"printf '**literal**'","workdir":"/tmp","timeout_ms":5000}"#
        let entry = TranscriptRecord(recordID: "command", record: Message(role: "tool_use", text: "", toolName: "exec_command", toolInput: source, toolOutput: nil))
        let blocks = ToolContentRenderer.richBlocks([entry])
        #expect(blocks.contains(.code("printf '**literal**'", language: "sh")))
        let combined = blocks.map { block -> String in
            switch block {
            case .attributed(let text): return text.string
            case .code(let source, _), .markdown(let source): return source
            case .attachment, .attachmentNotice, .embeddedImage: return ""
            }
        }.joined()
        #expect(combined == ToolContentRenderer.render([entry]).string)
        #expect(combined.contains("timeout_ms\n5000"))
        #expect(combined.contains("workdir\n/tmp"))
    }

    @Test func imagesRequireExplicitToolOrPayloadEvidence() {
        let call = TranscriptRecord(recordID: "image", record: Message(role: "tool_use", text: "", toolName: "view_image", toolInput: #"{"path":"/tmp/image.png"}"#, toolOutput: nil))
        let result = TranscriptRecord(recordID: "result", record: Message(role: "tool_result", text: #"{"content":[{"type":"image_url","image_url":{"url":"https://example.com/image.png"}}],"path":"/tmp/not-an-image"}"#, toolName: "custom", toolInput: nil, toolOutput: nil))
        #expect(ToolContentRenderer.imageSources([call, result]) == ["/tmp/image.png", "https://example.com/image.png"])
    }

    @Test func embeddedImagesKeepOriginalPayloadInRawContent() {
        let source = #"{"content":[{"type":"image","data":"aGVsbG8=","mimeType":"image/png"}]}"#
        let entry = TranscriptRecord(recordID: "image", record: Message(role: "tool_result", text: source, toolName: "custom", toolInput: nil, toolOutput: nil))
        #expect(ToolContentRenderer.richBlocks([entry]).contains(.embeddedImage(label: "Image result", data: Data("hello".utf8), mimeType: "image/png")))
        #expect(ToolContentRenderer.render([entry], raw: true).string == source)
    }

    @Test func explicitInterruptionsAreDistinctFromFailure() {
        for status in ["cancelled", "interrupted", "incomplete"] {
            let entry = TranscriptRecord(recordID: "result", record: Message(role: "tool_result", text: "", toolName: "Bash", toolInput: nil, toolOutput: "{\"status\":\"\(status)\"}"))
            let presentation = TranscriptActivity(records: [entry]).presentation
            #expect(presentation.title == status.capitalized + " · Run")
            #expect(!presentation.hasFailure)
        }
    }
}
