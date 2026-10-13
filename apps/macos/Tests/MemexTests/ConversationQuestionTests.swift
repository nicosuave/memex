import Foundation
import Testing
@testable import Memex

struct ConversationQuestionTests {
    @Test func answerUsesCapturedTextAfterSourceChangesAndPreservesUserAnswer() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        let file = directory.appendingPathComponent("requirements.txt")
        try Data("Original bytes\nsecond line".utf8).write(to: file)
        let captured = try ConversationQuestionContext.capture([file], existing: [])
        try Data("Changed afterward".utf8).write(to: file)
        let answer = ConversationQuestionContext.answerText("My answer", contexts: captured)
        #expect(answer == "My answer\n\nAttached answer context: requirements.txt\nOriginal bytes\nsecond line")
        #expect(ConversationQuestionContext.answerText("", contexts: captured).contains("Original bytes"))
    }

    @Test func questionAttachmentsRejectMediaBinaryAndOversizeInsteadOfDroppingBytes() throws {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        defer { try? FileManager.default.removeItem(at: directory) }
        for (name, data) in [("image.svg", Data("<svg/>".utf8)), ("binary.dat", Data([0, 255, 1])),
                             ("large.txt", Data(repeating: 65, count: ConversationQuestionContext.maximumBytes + 1))] {
            let file = directory.appendingPathComponent(name)
            try data.write(to: file)
            #expect(throws: ConversationRuntimeError.self) {
                try ConversationQuestionContext.capture([file], existing: [])
            }
        }
    }

    @Test func progressNavigationPreservesNativeIdentityAcrossReorderAndAcknowledgement() {
        var navigation = ConversationQuestionNavigation()
        navigation.reconcile(["request::one", "request::two", "other::one"])
        #expect(navigation.selectedID == "request::one")
        #expect(!navigation.canGoBack && navigation.canGoNext)
        navigation.move(1)
        #expect(navigation.selectedID == "request::two")
        navigation.move(-1)
        #expect(navigation.selectedID == "request::one")
        navigation.reconcile(["other::one", "request::one", "request::two"])
        #expect(navigation.index == 1)
        #expect(navigation.selectedID == "request::one")
        navigation.reconcile(["other::one", "request::two"])
        #expect(navigation.selectedID == "request::two")
        navigation.move(1)
        #expect(navigation.selectedID == "request::two")
        navigation.reconcile([])
        #expect(navigation.selectedID == nil)
    }
}
