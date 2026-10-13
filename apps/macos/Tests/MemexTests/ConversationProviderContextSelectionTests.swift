import Foundation
import Testing

@testable import Memex

#if canImport(SQACPHost)
  import SQACP

  @Suite(.serialized) @MainActor struct ConversationProviderContextSelectionTests {
    @Test func nativeReferencesRetainIdentityAcrossSavedDraftAndQueueWithoutReadingSkillBytes()
      async throws
    {
      let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      defer { try? FileManager.default.removeItem(at: root) }
      let skillPath = "/provider-owned/skills/review with spaces/SKILL.md"
      let skill = try ConversationAttachment.codexSkill(name: "careful-review", path: skillPath)
      let plugin = try ConversationAttachment.codexPlugin(name: "Tools", id: "tools@market")
      let saved = ConversationDraftStore(directory: root)
      let text = "Keep my exact unsent text\n"
      let queued = ConversationQueuedPrompt(
        .init(.prompt, text: "Queued text", attachments: [plugin, skill]))
      saved.set(
        .init(text: text, attachments: [skill, plugin], queue: [queued]), for: "codex:session")
      await saved.flush()
      let draft = try #require(ConversationDraftStore(directory: root).drafts["codex:session"])
      #expect(draft.text == text)
      #expect(draft.attachments == [skill, plugin])
      guard case .resourceLink(let skillReference) = try draft.attachments[0].promptContent(),
        case .resourceLink(let pluginReference) = try draft.attachments[1].promptContent()
      else {
        Issue.record("Expected retained native references")
        return
      }
      #expect(skillReference.nativeInvocation == .codexSkill)
      #expect(URL(string: skillReference.uri)?.path == skillPath)
      #expect(pluginReference.nativeInvocation == .codexPlugin)
      #expect(pluginReference.uri == "plugin://tools@market")
      #expect(draft.pendingPrompt == nil)
      #expect(draft.queue == [queued])
      #expect(draft.queue[0].command().attachments == [plugin, skill])
    }
  }
#endif
