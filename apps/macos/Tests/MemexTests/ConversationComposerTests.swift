import AppKit
import Foundation
import Testing
@testable import Memex

@Suite(.serialized) @MainActor struct ConversationComposerTests {
    private func directory() throws -> URL {
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        return directory.resolvingSymlinksInPath()
    }

    @Test func stashRoundTripsCapturedAttachmentsAndNeverDeletesOnReadOrRestore() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let attachment = ConversationAttachment(id: "captured", title: "context", path: "/source", content: Data("immutable".utf8))
        let stash = ConversationPromptLibrary(directory: root)
        #expect(await stash.stash(text: "Keep this exact prompt", attachments: [attachment]))
        let reopened = ConversationPromptLibrary(directory: root)
        #expect(reopened.entries.count == 1)
        #expect(reopened.entries[0].text == "Keep this exact prompt")
        #expect(reopened.entries[0].attachments == [attachment])
        #expect(ConversationPromptLibrary(directory: root).entries == reopened.entries)
        #expect(await reopened.remove(reopened.entries[0].id))
        #expect(ConversationPromptLibrary(directory: root).entries.isEmpty)
    }

    @Test func fullOrUnreadableStashDoesNotReplaceSavedWork() async throws {
        let stash = ConversationPromptLibrary()
        for index in 0..<ConversationPromptLibrary.maximumEntries {
            #expect(await stash.stash(text: "Prompt \(index)", attachments: []))
        }
        let saved = stash.entries
        #expect(!(await stash.stash(text: "Overflow", attachments: [])))
        #expect(stash.entries == saved)

        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let file = root.appendingPathComponent("prompt-stash.json")
        let invalid = Data("unreadable saved work".utf8)
        try invalid.write(to: file)
        let unreadable = ConversationPromptLibrary(directory: root)
        #expect(unreadable.error != nil)
        #expect(!(await unreadable.stash(text: "New work", attachments: [])))
        #expect(try Data(contentsOf: file) == invalid)
    }

    @Test func promptRecallOnlyRunsInTheComposerAtTheDraftEdge() throws {
        #expect(PromptRecallKeys.shouldRecall(-1, selection: NSRange(location: 0, length: 0), length: 20))
        #expect(!PromptRecallKeys.shouldRecall(-1, selection: NSRange(location: 5, length: 0), length: 20))
        #expect(PromptRecallKeys.shouldRecall(1, selection: NSRange(location: 20, length: 0), length: 20))
        #expect(!PromptRecallKeys.shouldRecall(1, selection: NSRange(location: 0, length: 3), length: 20))
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 400, height: 300), styleMask: [.titled],
                              backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        defer { window.close() }
        let content = NSView(frame: NSRect(x: 0, y: 0, width: 400, height: 300))
        window.contentView = content
        let monitor = PromptRecallKeyMonitor.MonitorView(frame: NSRect(x: 0, y: 0, width: 400, height: 100))
        let composer = NSTextView(frame: NSRect(x: 10, y: 10, width: 300, height: 60))
        let find = NSTextView(frame: NSRect(x: 10, y: 200, width: 300, height: 30))
        for view in [monitor, composer, find] as [NSView] { content.addSubview(view) }
        window.orderFront(nil)
        var history = ["earlier prompt"]
        monitor.recall = { direction in direction < 0 ? history.popLast() : nil }
        func commandKey(_ code: UInt16) throws -> NSEvent {
            try #require(NSEvent.keyEvent(with: .keyDown, location: .zero, modifierFlags: [.command, .numericPad, .function],
                timestamp: 0, windowNumber: window.windowNumber, context: nil, characters: "",
                charactersIgnoringModifiers: "", isARepeat: false, keyCode: code))
        }
        composer.string = "a long draft"
        window.makeFirstResponder(composer)
        composer.setSelectedRange(NSRange(location: 4, length: 0))
        #expect(!monitor.handle(try commandKey(126))) // Mid-draft ⌘↑ keeps moving to the start.
        composer.setSelectedRange(NSRange(location: 0, length: 0))
        #expect(monitor.handle(try commandKey(126)))
        #expect(composer.string == "earlier prompt")
        #expect(composer.selectedRange() == NSRange(location: 0, length: 0))
        find.string = ""
        history = ["another"]
        window.makeFirstResponder(find)
        #expect(!monitor.handle(try commandKey(126))) // Other text views keep the standard shortcut.
        #expect(find.string.isEmpty)
    }

    @Test func ownershipNoticeNamesTheOwningProvider() {
        #expect(ConversationOwnershipNotice.message(for: "codex").contains("other Codex app or CLI"))
        #expect(ConversationOwnershipNotice.message(for: "claude").contains("other Claude Code app or CLI"))
        #expect(!ConversationOwnershipNotice.message(for: "acp:gemini").contains("Codex"))
    }

    @Test func recallRestoresUnsentDraftAndAnEditStartsANewRecallSession() {
        var recall = ConversationPromptRecall()
        let history = ["First", "Second"]
        #expect(recall.move(-1, current: "Unsent", history: history) == "Second")
        #expect(recall.move(-1, current: "Second", history: history) == "First")
        #expect(recall.move(1, current: "First", history: history) == "Second")
        #expect(recall.move(1, current: "Second", history: history) == "Unsent")
        #expect(recall.position == nil)
        #expect(recall.move(-1, current: "Unsent", history: history) == "Second")
        #expect(recall.move(-1, current: "Edited recalled prompt", history: history) == "Second")
        #expect(recall.move(1, current: "Second", history: history) == "Edited recalled prompt")
        #expect(recall.move(-1, current: "Latest unsent draft", history: history) == "Second")
        #expect(recall.move(1, current: "Second", history: history + ["Newly indexed prompt"]) == "Latest unsent draft")
    }

    @Test func preferencesAreProviderScopedAndOnlyExplicitChoicesAreSaved() throws {
        let name = "memex.composer.tests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: name))
        defer { defaults.removePersistentDomain(forName: name) }
        ConversationComposerPreferences.saveModel("chosen", provider: "codex", defaults: defaults)
        ConversationComposerPreferences.saveConfiguration("reasoning_effort", value: "high", provider: "codex", defaults: defaults)
        let codex = ConversationComposerPreferences.selection(provider: "codex", defaults: defaults)
        #expect(codex.modelID == "chosen")
        #expect(codex.configurations == ["reasoning_effort": "high"])
        #expect(ConversationComposerPreferences.selection(provider: "claude", defaults: defaults).modelID == nil)
    }

    @Test func claudeDefaultsToNativeAutoForNewAndResumedChatsAndKeepsExplicitChoices() throws {
        let name = "memex.permissions.tests.\(UUID().uuidString)"
        let defaults = try #require(UserDefaults(suiteName: name))
        defer { defaults.removePersistentDomain(forName: name) }
        #expect(ConversationComposerPreferences.claudePermissionMode(defaults: defaults) == "auto")
        #expect(ConversationComposerPreferences.claudePermissionMode(sessionID: "resumed", defaults: defaults) == "auto")
        #expect(ConversationComposerPreferences.selection(provider: "claude", defaults: defaults).configurations.isEmpty)
        ConversationComposerPreferences.saveConfiguration("permission_mode", value: "acceptEdits", provider: "claude", defaults: defaults)
        #expect(ConversationComposerPreferences.claudePermissionMode(defaults: defaults) == "acceptEdits")
        #expect(ConversationComposerPreferences.claudePermissionMode(sessionID: "resumed", defaults: defaults) == "acceptEdits")
        ConversationComposerPreferences.saveClaudePermissionMode("default", sessionID: "resumed", defaults: defaults)
        ConversationComposerPreferences.saveConfiguration("permission_mode", value: "auto", provider: "claude", defaults: defaults)
        #expect(ConversationComposerPreferences.claudePermissionMode(sessionID: "resumed", defaults: defaults) == "default")
        #expect(ConversationComposerPreferences.claudePermissionMode(sessionID: "resumed", hostedMode: "auto", defaults: defaults) == "default")
        #expect(ConversationComposerPreferences.claudePermissionMode(sessionID: "hosted", hostedMode: "acceptEdits", defaults: defaults) == "acceptEdits")
        #expect(ConversationComposerPreferences.claudePermissionMode(sessionID: "other", defaults: defaults) == "auto")
    }

    @Test func fileAndSkillCatalogUsesWorkspaceAndOwningProviderRoots() throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let workspace = root.appendingPathComponent("workspace")
        let skill = workspace.appendingPathComponent(".agents/skills/review/SKILL.md")
        let command = root.appendingPathComponent("provider/commands/explain.md")
        let source = workspace.appendingPathComponent("Sources/Main.swift")
        let ignored = workspace.appendingPathComponent("node_modules/huge.js")
        for file in [skill, command, source, ignored] {
            try FileManager.default.createDirectory(at: file.deletingLastPathComponent(), withIntermediateDirectories: true)
            try Data("example".utf8).write(to: file)
        }
        try Data("---\nname: careful-review\ndescription: Read before changing\n---\nFull skill".utf8).write(to: skill)
        let catalog = try ConversationComposerCatalog.load(workspace: workspace,
            providerHome: root.appendingPathComponent("provider"), locallyAccessible: true,
            userHome: root.appendingPathComponent("user"))
        #expect(catalog.matchingFiles("main").map(\.relativePath) == ["Sources/Main.swift"])
        #expect(catalog.matchingFiles("huge").isEmpty)
        #expect(catalog.prompts.contains { $0.name == "careful-review" && $0.description == "Read before changing" && $0.url == skill })
        #expect(catalog.prompts.contains { $0.name == "explain" && $0.kind == .command && $0.url == command })

        // A remote host can use identical path strings, including a native pair
        // whose machine label is local. Neither authorizes viewer filesystem reads.
        for (machineID, isServerOwned) in [("remote", false), ("remote", true), ("local", true)] {
            let accessible = ConversationComposerCatalog.locallyAccessible(machineID: machineID, isServerOwned: isServerOwned)
            #expect(!accessible)
            let remote = try ConversationComposerCatalog.load(workspace: workspace,
                providerHome: root.appendingPathComponent("provider"), locallyAccessible: accessible,
                userHome: workspace)
            #expect(remote.files.isEmpty)
            #expect(remote.prompts.isEmpty)
        }
        #expect(ConversationComposerCatalog.locallyAccessible(machineID: "local", isServerOwned: false))
    }
}
