import Foundation
import Testing
@testable import Memex

@Test func chatGPTResumeTargetsExistingLocalCodexConversation() {
    let id = "01a07ccd-f180-7242-9826-5bdf2eba294b"
    let session = Session(source: "codex", sessionID: id, sourcePath: "/old", project: "p")
    #expect(ChatGPTResume.url(for: session)?.absoluteString == "codex://threads/" + id)
    var remote = session
    remote.machine = "nicbook-atm"
    #expect(ChatGPTResume.url(for: remote) == nil)
    for (source, id) in [("claude", id), ("codex", "../new?prompt=bad"), ("codex", "")] {
        #expect(ChatGPTResume.url(for: Session(source: source, sessionID: id, sourcePath: "/old", project: "p")) == nil)
    }
}
