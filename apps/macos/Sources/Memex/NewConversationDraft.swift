import Foundation
import Observation

/// A new-chat draft has no provider session yet. Keep its workspace reservation
/// and eventual native session identity so recovery never creates or sends twice.
@MainActor @Observable
final class NewConversationDraft {
    struct Value: Codable, Equatable, Sendable {
        var text = ""
        var provider = "codex"
        var projectID: String?
        var workspaceMode = ConversationWorkspaceMode.existingDirectory
        var baseRef: String?
        var preparedWorkspace: ConversationWorkspace?
        var createdSessionID: String?
    }

    var value = Value() { didSet { if value != oldValue { persist() } } }
    private(set) var error: String?
    var focusRequest = 0
    @ObservationIgnored private let writer: NewConversationDraftWriter?
    @ObservationIgnored private var canWrite = true
    @ObservationIgnored private var revision = 0
    @ObservationIgnored private var pendingWrite: Task<Void, Never>?

    private struct Saved: Codable {
        let version: Int
        let value: Value
    }

    init(directory: URL? = nil) {
        writer = directory.map(NewConversationDraftWriter.init)
        guard let directory else { return }
        do {
            let data = try Data(contentsOf: directory.appendingPathComponent("new-conversation.json"))
            let saved = try JSONDecoder().decode(Saved.self, from: data)
            guard saved.version == 1 else { throw CocoaError(.fileReadCorruptFile) }
            value = saved.value
        } catch let failure as CocoaError where failure.code == .fileReadNoSuchFile {
        } catch {
            canWrite = false
            self.error = "The new conversation draft could not be read. The saved file has been preserved."
        }
    }

    static func persistent() -> NewConversationDraft {
        NewConversationDraft(directory: FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/dev.memex.app/Drafts", isDirectory: true))
    }

    func selectProject(_ project: LocalProject, resetWorkspace: Bool = false) {
        guard value.createdSessionID == nil, resetWorkspace || value.projectID != project.id else { return }
        var next = value
        next.projectID = project.id
        next.workspaceMode = project.defaultWorkspace
        next.baseRef = project.defaultBaseRef
        next.preparedWorkspace = nil
        value = next
    }

    func selectNoProject() {
        guard value.createdSessionID == nil else { return }
        var next = value
        next.projectID = nil
        next.workspaceMode = .existingDirectory
        next.baseRef = nil
        next.preparedWorkspace = nil
        value = next
    }

    func selectWorkspace(_ mode: ConversationWorkspaceMode, baseRef: String?) {
        guard value.createdSessionID == nil, value.projectID != nil else { return }
        var next = value
        next.workspaceMode = mode
        next.baseRef = baseRef
        next.preparedWorkspace = nil
        value = next
    }

    func finishTransfer() async throws {
        let previous = value
        var next = value
        next.text = ""
        next.preparedWorkspace = nil
        next.createdSessionID = nil
        value = next
        await flush()
        if let error {
            // Keep the recovery action in this window as well as on disk. A
            // failed clear must never make the next click create a second chat.
            value = previous
            throw ConversationRuntimeError(message: error)
        }
    }

    func retrySave() async {
        persist()
        await flush()
    }

    private func persist() {
        guard canWrite, let writer else { return }
        revision += 1
        let revision = revision
        let saved = Saved(version: 1, value: value)
        pendingWrite = Task {
            do {
                let data = try JSONEncoder().encode(saved)
                try await writer.save(data, revision: revision)
                if self.revision == revision { error = nil }
            } catch {
                if self.revision == revision { self.error = "The new conversation draft could not be saved. Keep Memex open and retry after saving is available." }
            }
        }
    }

    func flush() async {
        while let pendingWrite {
            let revision = revision
            await pendingWrite.value
            if revision == self.revision { return }
        }
    }
}

private actor NewConversationDraftWriter {
    let directory: URL
    private var revision = 0

    init(directory: URL) { self.directory = directory }

    func save(_ data: Data, revision: Int) throws {
        guard revision > self.revision else { return }
        self.revision = revision
        let manager = FileManager.default
        try manager.createDirectory(at: directory, withIntermediateDirectories: true,
                                    attributes: [.posixPermissions: 0o700])
        let file = directory.appendingPathComponent("new-conversation.json")
        try data.write(to: file, options: .atomic)
        try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
    }
}
