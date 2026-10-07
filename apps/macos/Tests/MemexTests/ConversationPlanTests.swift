import Foundation
import Testing
@testable import Memex

@Test func fullPlanDocumentKeepsMarkdownAndNativeRecordIdentity() {
    var message = Message(role: "tool_use", text: "", toolName: "Plan", toolInput: nil, toolOutput: nil)
    message.structuredActivity  = ##"{"plan":{"_0":[]},"planDocument":{"id":"native-plan","text":"# Plan\n\nFull **design** with rationale."}}"##
    let plan = ConversationWork.project([TranscriptRecord(recordID: "native-record", record: message)]).plan
    #expect(plan?.recordID == "native-record")
    #expect(plan?.text == "# Plan\n\nFull **design** with rationale.")
    #expect(plan?.steps.isEmpty == true)
}

@Test func childGroupsRequireKnownProviderStatuses() {
    #expect(ConversationWork.Agent(id: "a", status: "running").group == "Active")
    #expect(ConversationWork.Agent(id: "b", status: "completed").group == "Done")
    #expect(ConversationWork.Agent(id: "c", status: "not_found").group == "Status unavailable")
    #expect(ConversationWork.Agent(id: "d", status: "unknown").group == "Status unavailable")
}

@Test func planDocumentPreservesEditsSeparatesRevisionsAndChecksConflicts() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let session = Session(source: "codex", sessionID: "native", sourcePath: "/history.jsonl", project: "test")
    let original = ConversationWork.Plan(recordID: "plan-item", steps: [], markdown: "# Original")
    let document = ConversationPlanDocument(plan: original, session: session, directory: directory)
    let client = WorkspaceFilesClient()
    let first = try await document.open(original: original.text, client: client)
    let edited = try await client.save(root: document.root, path: document.path, contents: "# Revised",
                                       expectedFingerprint: first.fingerprint)
    let reopened = try await document.open(original: original.text, client: client)
    #expect(reopened == edited)
    let revised = document.revised(original, revision: edited)
    #expect(revised.text == "# Revised")
    #expect(revised.recordID == original.recordID)
    #expect(revised.provenance(session: session).contains(edited.fingerprint))
    var next = original
    next.markdown = "# New provider revision"
    #expect(ConversationPlanDocument(plan: next, session: session, directory: directory).root != document.root)
    do {
        _ = try await client.save(root: document.root, path: document.path, contents: "stale", expectedFingerprint: first.fingerprint)
        Issue.record("A stale editor must not overwrite the saved plan")
    } catch WorkspaceFileError.conflict {}
    #expect(try await client.read(root: document.root, path: document.path) == edited)
}

@Test func planInitializationFailureDoesNotPublishAnEmptyDocument() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let session = Session(source: "codex", sessionID: "native", sourcePath: "/history.jsonl", project: "test")
    let plan = ConversationWork.Plan(recordID: "item", steps: [], markdown: "Original")
    let document = ConversationPlanDocument(plan: plan, session: session, directory: directory)
    do {
        _ = try await document.open(original: String(repeating: "x", count: WorkspaceFilesClient.maximumBytes + 1))
        Issue.record("Oversized initial content must fail")
    } catch WorkspaceFileError.tooLarge {}
    #expect(!FileManager.default.fileExists(atPath: document.root.appendingPathComponent(document.path).path))
    #expect(try await document.open(original: plan.text).contents == plan.text)
}

@Test func concurrentPlanInitializationPublishesOnlyCompleteOriginal() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: directory) }
    let session = Session(source: "codex", sessionID: "native", sourcePath: "/history.jsonl", project: "test")
    let plan = ConversationWork.Plan(recordID: "item", steps: [], markdown: String(repeating: "Complete plan\n", count: 20_000))
    let document = ConversationPlanDocument(plan: plan, session: session, directory: directory)
    let originals = try await withThrowingTaskGroup(of: WorkspaceFileRevision.self) { group in
        // Separate actors exercise the filesystem boundary, not actor serialization.
        for _ in 0..<12 {
            group.addTask { try await document.open(original: plan.text, client: WorkspaceFilesClient()) }
        }
        var revisions: [WorkspaceFileRevision] = []
        for try await revision in group { revisions.append(revision) }
        return revisions
    }
    #expect(originals.count == 12)
    #expect(originals.allSatisfy { $0.contents == plan.text })
    #expect(Set(originals.map(\.fingerprint)).count == 1)
    #expect(try FileManager.default.contentsOfDirectory(atPath: document.root.path) == ["PLAN.md"])
}
