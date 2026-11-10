import Foundation
import Observation

struct ConversationPromptEntry: Identifiable, Codable, Equatable, Sendable {
    let id: String
    let createdAt: Date
    let text: String
    let attachments: [ConversationAttachment]

    var title: String {
        text.nilIfBlank.map { String($0.prefix(100)) } ?? attachments.map(\.title).joined(separator: ", ")
    }
}

/// Explicit stashes are independent from conversation drafts. Restoration copies
/// an entry; only Delete removes it, so an interrupted draft save loses nothing.
@MainActor @Observable final class ConversationPromptLibrary {
    static let shared = ConversationPromptLibrary(directory: FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent("Library/Application Support/dev.memex.app/Drafts"))
    static let maximumEntries = 20
    private(set) var entries: [ConversationPromptEntry] = []
    private(set) var error: String?
    private(set) var saving = false
    @ObservationIgnored private let directory: URL?
    @ObservationIgnored private var readable = true

    init(directory: URL? = nil) {
        self.directory = directory
        guard let directory else { return }
        do {
            entries = try JSONDecoder().decode([ConversationPromptEntry].self,
                from: Data(contentsOf: directory.appendingPathComponent("prompt-stash.json")))
        } catch let failure as CocoaError where failure.code == .fileReadNoSuchFile {
        } catch {
            readable = false
            self.error = "The prompt stash could not be read. Its saved file has been preserved."
        }
    }

    @discardableResult func stash(text: String, attachments: [ConversationAttachment]) async -> Bool {
        guard text.nilIfBlank != nil || !attachments.isEmpty else { return false }
        guard entries.count < Self.maximumEntries else {
            error = "The stash holds \(Self.maximumEntries) prompts. Delete an entry before saving another."
            return false
        }
        return await save([.init(id: UUID().uuidString, createdAt: Date(), text: text, attachments: attachments)] + entries)
    }

    @discardableResult func remove(_ id: String) async -> Bool {
        await save(entries.filter { $0.id != id })
    }

    private func save(_ next: [ConversationPromptEntry]) async -> Bool {
        guard readable, !saving else { return false }
        saving = true
        defer { saving = false }
        do {
            if let directory {
                try await Task.detached {
                    let manager = FileManager.default
                    try manager.createDirectory(at: directory, withIntermediateDirectories: true,
                        attributes: [.posixPermissions: 0o700])
                    let file = directory.appendingPathComponent("prompt-stash.json")
                    try JSONEncoder().encode(next).write(to: file, options: .atomic)
                    try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
                }.value
            }
            entries = next
            error = nil
            return true
        } catch {
            self.error = "The prompt stash could not be saved: \(error.localizedDescription)"
            return false
        }
    }
}

/// A recall session retains the unsent draft and restores it on forward navigation.
struct ConversationPromptRecall {
    private(set) var position: Int?
    private var savedText = ""
    private var recalledText: String?
    private var entries: [String] = []

    mutating func move(_ direction: Int, current: String, history: [String]) -> String? {
        guard !history.isEmpty else { return nil }
        if let recalledText, current != recalledText { reset() }
        if position == nil {
            guard direction < 0 else { return nil }
            savedText = current
            entries = history
            position = entries.count
        }
        let next = min(entries.count, max(0, (position ?? entries.count) + direction))
        if next == entries.count {
            let original = savedText
            reset()
            return original
        }
        position = next
        recalledText = entries[next]
        return recalledText
    }

    mutating func reset() { position = nil; recalledText = nil; savedText = ""; entries = [] }
}

enum ConversationComposerPreferences {
    struct Selection: Codable, Equatable {
        var modelID: String?
        var configurations: [String: String] = [:]
    }

    static func selection(provider: String, defaults: UserDefaults = .standard) -> Selection {
        guard let data = defaults.data(forKey: "conversation.composer.settings.\(provider)"),
              let value = try? JSONDecoder().decode(Selection.self, from: data) else { return Selection() }
        return value
    }

    /// Persist only explicit user changes, never overwrite defaults from a resumed chat.
    static func saveModel(_ id: String, provider: String, defaults: UserDefaults = .standard) {
        var value = selection(provider: provider, defaults: defaults)
        value.modelID = id
        save(value, provider: provider, defaults: defaults)
    }

    static func saveConfiguration(_ id: String, value selected: String, provider: String, defaults: UserDefaults = .standard) {
        var value = selection(provider: provider, defaults: defaults)
        value.configurations[id] = selected
        save(value, provider: provider, defaults: defaults)
    }

    /// Memex starts Claude in native Auto mode. Explicit conversation choices
    /// take precedence over the remembered provider preference on reconnect.
    static func claudePermissionMode(sessionID: String? = nil, hostedMode: String? = nil, defaults: UserDefaults = .standard) -> String {
        if let sessionID, let selected = defaults.string(forKey: "conversation.claude.permissions.\(sessionID)") {
            return selected
        }
        return hostedMode ?? selection(provider: "claude", defaults: defaults).configurations["permission_mode"] ?? "auto"
    }

    static func saveClaudePermissionMode(_ mode: String, sessionID: String, defaults: UserDefaults = .standard) {
        defaults.set(mode, forKey: "conversation.claude.permissions.\(sessionID)")
    }

    private static func save(_ selection: Selection, provider: String, defaults: UserDefaults) {
        guard let data = try? JSONEncoder().encode(selection) else { return }
        defaults.set(data, forKey: "conversation.composer.settings.\(provider)")
    }
}
