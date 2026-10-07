import Foundation
import Observation

/// Drafts belong to the same provider, machine and source-file identity as history.
/// Saving a draft never creates a runtime or schedules a send.
@MainActor @Observable
final class ConversationDraftStore {
    struct Draft: Codable, Equatable, Sendable {
        var text: String
        var deliveryUncertain = false
        var attachments: [ConversationAttachment] = []
        var pendingPrompt: ConversationPendingPrompt?
        var queue: [ConversationQueuedPrompt] = []
        var queueHeld = false

        init(text: String, deliveryUncertain: Bool = false, attachments: [ConversationAttachment] = [],
             pendingPrompt: ConversationPendingPrompt? = nil,
             queue: [ConversationQueuedPrompt] = [], queueHeld: Bool = false) {
            self.text = text
            self.deliveryUncertain = deliveryUncertain
            self.attachments = attachments
            self.pendingPrompt = pendingPrompt
            self.queue = queue
            self.queueHeld = queueHeld
        }

        private enum CodingKeys: String, CodingKey { case text, deliveryUncertain, attachments, pendingPrompt, queue, queueHeld }

        init(from decoder: Decoder) throws {
            let values = try decoder.container(keyedBy: CodingKeys.self)
            text = try values.decode(String.self, forKey: .text)
            deliveryUncertain = try values.decodeIfPresent(Bool.self, forKey: .deliveryUncertain) ?? false
            attachments = try values.decodeIfPresent([ConversationAttachment].self, forKey: .attachments) ?? []
            pendingPrompt = try values.decodeIfPresent(ConversationPendingPrompt.self, forKey: .pendingPrompt)
            queue = try values.decodeIfPresent([ConversationQueuedPrompt].self, forKey: .queue) ?? []
            queueHeld = try values.decodeIfPresent(Bool.self, forKey: .queueHeld) ?? false
        }
    }

    private(set) var drafts: [String: Draft] = [:]
    private(set) var error: String?
    @ObservationIgnored private let writer: DraftFileWriter?
    @ObservationIgnored private var canWrite = true
    @ObservationIgnored private var revision = 0
    @ObservationIgnored private var pendingWrite: Task<Void, Never>?

    /// A nil directory gives tests and previews an isolated, in-memory store.
    init(directory: URL? = nil) {
        writer = directory.map { DraftFileWriter(directory: $0) }
        guard let directory else { return }
        let file = directory.appendingPathComponent("drafts.json")
        do {
            let data = try Data(contentsOf: file)
            let saved = try JSONDecoder().decode(SavedDrafts.self, from: data)
            guard saved.version == 1 else { throw CocoaError(.fileReadCorruptFile) }
            drafts = saved.drafts
        } catch let failure as CocoaError where failure.code == .fileReadNoSuchFile {
        } catch {
            // Do not overwrite unreadable saved drafts with a partial new catalog.
            canWrite = false
            self.error = "Saved drafts could not be read. New edits are kept in this window only."
        }
    }

    static func persistent() -> ConversationDraftStore {
        let directory = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/dev.memex.app/Drafts", isDirectory: true)
        return ConversationDraftStore(directory: directory)
    }

    func set(_ draft: Draft, for sessionID: String) {
        let value: Draft? = draft.text.isEmpty && draft.attachments.isEmpty && draft.pendingPrompt == nil
            && draft.queue.isEmpty && !draft.queueHeld && !draft.deliveryUncertain ? nil : draft
        guard drafts[sessionID] != value else { return }
        drafts[sessionID] = value
        persist()
    }

    func retrySave() async {
        persist()
        await flush()
    }

    private func persist() {
        guard canWrite, let writer else { return }
        revision += 1
        let revision = revision
        let drafts = drafts
        pendingWrite = Task {
            do {
                try await writer.save(drafts, revision: revision)
                if self.revision == revision { error = nil }
            } catch {
                if self.revision == revision {
                    self.error = "The draft could not be saved. Keep this window open until saving succeeds."
                }
            }
        }
    }

    func flush() async {
        while let pendingWrite {
            let revision = revision
            await pendingWrite.value
            if self.revision == revision { return }
        }
    }
}

private struct SavedDrafts: Codable, Sendable {
    let version: Int
    let drafts: [String: ConversationDraftStore.Draft]
}

private actor DraftFileWriter {
    let directory: URL
    private var latestRevision = 0

    init(directory: URL) { self.directory = directory }

    func save(_ drafts: [String: ConversationDraftStore.Draft], revision: Int) throws {
        // Actor jobs may arrive after a newer edit. Never overwrite it with an old one.
        guard revision > latestRevision else { return }
        latestRevision = revision
        let manager = FileManager.default
        try manager.createDirectory(at: directory, withIntermediateDirectories: true,
                                    attributes: [.posixPermissions: 0o700])
        let file = directory.appendingPathComponent("drafts.json")
        let data = try JSONEncoder().encode(SavedDrafts(version: 1, drafts: drafts))
        try data.write(to: file, options: .atomic)
        try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    }
}
