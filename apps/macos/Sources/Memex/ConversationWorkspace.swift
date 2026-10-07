import Foundation
import MemexExecutionHostCore

typealias ConversationWorkspaceRepository = MemexExecutionHostCore.ConversationWorkspaceRepository
typealias ConversationWorkspace = MemexExecutionHostCore.ConversationWorkspace
typealias ConversationWorkspacePreparationError = MemexExecutionHostCore.ConversationWorkspacePreparationError

struct ConversationWorkspaceClient: Sendable {
    let store: ManagedWorkspaceStore
    var managedRoot: URL { store.managedRoot }
    var temporaryRoot: URL { store.temporaryRoot }

    init(managedRoot: URL? = nil, temporaryRoot: URL? = nil, makeID: @escaping @Sendable () -> UUID = { UUID() }) {
        store = ManagedWorkspaceStore(managedRoot: managedRoot, temporaryRoot: temporaryRoot, makeID: makeID)
    }

    func prepareTemporaryDirectory() async throws -> ConversationWorkspace {
        try Task.checkCancellation()
        return try await Task.detached(priority: .userInitiated) { try store.prepareTemporaryDirectory() }.value
    }

    func inspect(directory: URL) async throws -> ConversationWorkspaceRepository? {
        try await WorkspaceGitCommand.run { try store.inspect(directory: directory, command: $0) }
    }

    func prepare(directory: URL, mode: ConversationWorkspaceMode, baseRef: String? = nil) async throws -> ConversationWorkspace {
        try await WorkspaceGitCommand.run { try store.prepare(directory: directory, newWorktree: mode == .newWorktree, baseRef: baseRef, command: $0) }
    }
}
