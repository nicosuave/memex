import Darwin
import Foundation
import Testing
@testable import Memex

struct ConversationOwnershipWriterTests {
    @Test func onlyExternalWritableDescriptorsBlockContinuation() {
        #expect(!ConversationOwnership.containsExternalWriter("p123\nf3\nar\n", excludingPID: 42))
        #expect(!ConversationOwnership.containsExternalWriter("p42\nf3\nau\n", excludingPID: 42))
        #expect(ConversationOwnership.containsExternalWriter("p123\nf3\naw\n", excludingPID: 42))
        #expect(ConversationOwnership.containsExternalWriter("p123\nf3\nau\n", excludingPID: 42))
        #expect(!ConversationOwnership.containsExternalWriter("au\n", excludingPID: 42))
    }

    @Test func realOpenTranscriptWriterIsDetected() throws {
        let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString + ".jsonl")
        try Data().write(to: file)
        defer { try? FileManager.default.removeItem(at: file) }
        let child = Process()
        child.executableURL = URL(fileURLWithPath: "/bin/sh")
        child.arguments = ["-c", "exec 3>>\"$1\"; printf ready; read response", "writer-probe", file.path]
        let ready = Pipe()
        let input = Pipe()
        child.standardOutput = ready
        child.standardInput = input
        try child.run()
        defer {
            try? input.fileHandleForWriting.write(contentsOf: Data("done\n".utf8))
            child.waitUntilExit()
        }
        #expect(try ready.fileHandleForReading.read(upToCount: 5) == Data("ready".utf8))
        #expect(try ConversationOwnership.hasExternalWriter(at: file))
    }
}
