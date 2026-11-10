import AppKit
import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor struct ConversationMediaTests {
    private func bitmap() throws -> NSBitmapImageRep {
        let colorSpace = try #require(CGColorSpace(name: CGColorSpace.sRGB))
        let context = try #require(CGContext(data: nil, width: 16, height: 16,
            bitsPerComponent: 8, bytesPerRow: 64, space: colorSpace,
            bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue))
        context.setFillColor(red: 1, green: 0, blue: 0, alpha: 1)
        context.fill(CGRect(x: 0, y: 0, width: 16, height: 16))
        return NSBitmapImageRep(cgImage: try #require(context.makeImage()))
    }

    @Test func smallSupportedImagesRetainExactBytesAndTiffIsNormalized() throws {
        let bitmap = try bitmap()
        let png = try #require(bitmap.representation(using: .png, properties: [:]))
        #expect(try ConversationImageNormalization.normalize(png).data == png)
        let tiff = try #require(bitmap.representation(using: .tiff, properties: [:]))
        let normalized = try ConversationImageNormalization.normalize(tiff)
        #expect(normalized.mimeType == "image/png")
        #expect(normalized.data.count <= ConversationImageNormalization.maximumOutputBytes)
        let decoded = try #require(NSBitmapImageRep(data: normalized.data))
        #expect(decoded.pixelsWide == 16)
        #expect(decoded.pixelsHigh == 16)
        var pixel = [UInt](repeating: 0, count: decoded.samplesPerPixel)
        decoded.getPixel(&pixel, atX: 0, y: 0)
        #expect(Array(pixel.prefix(3)) == [255, 0, 0])
    }

    @Test func invalidImageFailsInsteadOfBecomingAnOpaqueAttachment() {
        #expect(throws: (any Error).self) { try ConversationImageNormalization.normalize(Data("not an image".utf8)) }
    }

    @Test func dictationRecoversLatestAudioWithoutRecordingOrSending() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: directory) }
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let older = directory.appendingPathComponent("100-first.m4a")
        let latest = directory.appendingPathComponent("200-second.m4a")
        try Data([1]).write(to: older)
        try Data([2]).write(to: latest)
        let dictation = ConversationDictation(directory: directory)
        let recovered = try #require(dictation.recordingURL)
        #expect(recovered.resolvingSymlinksInPath().path == latest.resolvingSymlinksInPath().path)
        #expect(try Data(contentsOf: recovered) == Data([2]))
        #expect(dictation.phase == .idle)
        #expect(dictation.transcript.isEmpty)
        dictation.cancel()
        #expect(try Data(contentsOf: latest) == Data([2]))
    }

    #if canImport(SQACPHost)
    @Test func editingAnnotationKeepsIdentityLocationAndOriginalCapture() throws {
        let original = try ConversationAttachment.text(title: "Selected response", text: "Captured text", source: "session-id#record-id")
        let first = try original.annotated(passage: "line 2", comment: "Explain this")
        let edited = try first.annotated(passage: "line 2", comment: "Explain more")
        #expect(edited.id == first.id)
        #expect(edited.annotation?.attachmentID == original.id)
        #expect(edited.annotation?.source == "session-id#record-id")
        #expect(edited.annotation?.comment == "Explain more")
        #expect(edited.content != first.content)
        let restored = try JSONDecoder().decode([ConversationAttachment].self,
            from: JSONEncoder().encode([original, edited]))
        #expect(restored[0].content == original.content)
        #expect(restored[1] == edited)
    }
    #endif
}
