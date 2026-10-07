import Darwin
import Foundation
import Observation

@MainActor @Observable
final class ConversationRelationships {
    struct Link: Codable, Equatable, Identifiable {
        enum Kind: String, Codable { case nativeFork, contextBranch, providerTransition, rewind, queueTransfer }
        let parent: Session
        let child: Session
        let kind: Kind
        let sourceRecordID: String?
        let createdAt: Date
        var id: String { child.id }
    }
    struct Pending: Codable, Equatable, Identifiable {
        let id: String
        let source: Session
        let operation: String
        let boundary: ConversationHistoryBoundary?
        let issuedAt: Date
        var result: Session?
    }
    struct QueueTransfer: Codable, Equatable, Identifiable {
        let id: String
        let source: Session
        let entry: ConversationQueuedPrompt
        let context: CreatedConversationCatalog.Context
        var result: Session?
        var completed = false
    }
    private struct Saved: Codable {
        var version = 1
        var links: [Link] = []
        var pending: [Pending] = []
        var queueTransfers: [QueueTransfer]? = nil
    }
    private(set) var links: [Link] = []
    private(set) var pending: [Pending] = []
    private(set) var queueTransfers: [QueueTransfer] = []
    private(set) var error: String?
    @ObservationIgnored private let directory: URL?

    init(directory: URL? = nil) {
        self.directory = directory
        do { if let directory { install(try read(directory)) } }
        catch { self.error = "Saved conversation relationships could not be read. Their file was preserved: \(error.localizedDescription)" }
    }
    static func persistent() -> Self {
        Self(directory: FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/dev.memex.app/Conversations"))
    }
    func parent(of id: String) -> Link? { links.first { $0.child.id == id } }
    func children(of id: String) -> [Link] { links.filter { $0.parent.id == id } }

    func begin(source: Session, operation: String, boundary: ConversationHistoryBoundary? = nil) throws -> Pending {
        let operation = Pending(id: UUID().uuidString, source: source, operation: operation, boundary: boundary, issuedAt: Date())
        try mutate { saved in
            guard !saved.pending.contains(where: { $0.source.id == source.id }) else {
                throw ConversationRuntimeError(message: "A previous history change needs inspection before another can start.")
            }
            saved.pending.append(operation)
        }
        return operation
    }
    func recordResult(_ session: Session, for operation: Pending) throws {
        try mutate { saved in
            guard let index = saved.pending.firstIndex(where: { $0.id == operation.id }) else {
                throw ConversationRuntimeError(message: "The saved history operation is unavailable.")
            }
            saved.pending[index].result = session
        }
    }
    func finish(_ operation: Pending, link: Link?) throws {
        try mutate { saved in
            if let link, !saved.links.contains(where: { $0.id == link.id }) { saved.links.append(link) }
            saved.pending.removeAll { $0.id == operation.id }
        }
    }
    func acknowledge(_ operation: Pending) throws { try finish(operation, link: nil) }

    /// Reservation is durable before native creation. An existing reservation
    /// without a result means creation may have happened; it cannot be replayed.
    func reserveQueueTransfer(source: Session, entry: ConversationQueuedPrompt,
                              context: CreatedConversationCatalog.Context) throws -> (QueueTransfer, Bool) {
        let key = source.id + ":" + entry.id
        var result: QueueTransfer?
        var reserved = false
        try mutate { saved in
            if let existing = saved.queueTransfers?.first(where: { $0.id == key }) {
                guard existing.entry == entry else {
                    throw ConversationRuntimeError(message: "This queued message changed after its transfer began. Inspect the saved side-chat draft before moving it again.")
                }
                result = existing
            } else {
                let transfer = QueueTransfer(id: key, source: source, entry: entry, context: context)
                saved.queueTransfers = (saved.queueTransfers ?? []) + [transfer]
                result = transfer
                reserved = true
            }
        }
        guard let result else { throw ConversationRuntimeError(message: "The queue transfer could not be reserved.") }
        return (result, reserved)
    }

    func recordQueueTransferResult(_ session: Session, transferID: String) throws {
        try mutate { saved in
            guard let index = saved.queueTransfers?.firstIndex(where: { $0.id == transferID }) else {
                throw ConversationRuntimeError(message: "The queue transfer reservation is missing.")
            }
            if let previous = saved.queueTransfers?[index].result, previous.id != session.id {
                throw ConversationRuntimeError(message: "The queue transfer already names another native session.")
            }
            saved.queueTransfers?[index].result = session
        }
    }

    func finishQueueTransfer(_ transfer: QueueTransfer, result: Session) throws {
        try mutate { saved in
            guard let index = saved.queueTransfers?.firstIndex(where: { $0.id == transfer.id }),
                  saved.queueTransfers?[index].result?.id == result.id else {
                throw ConversationRuntimeError(message: "The saved side-chat identity does not match this transfer.")
            }
            saved.queueTransfers?[index].completed = true
            if !saved.links.contains(where: { $0.child.id == result.id }) {
                saved.links.append(.init(parent: transfer.source, child: result, kind: .queueTransfer,
                    sourceRecordID: transfer.entry.id, createdAt: Date()))
            }
        }
    }

    private func read(_ directory: URL) throws -> Saved {
        let file = directory.appendingPathComponent("relationships.json")
        do {
            let saved = try JSONDecoder().decode(Saved.self, from: Data(contentsOf: file))
            guard saved.version == 1 else { throw CocoaError(.fileReadCorruptFile) }
            return saved
        } catch let error as CocoaError where error.code == .fileReadNoSuchFile { return Saved() }
    }
    private func install(_ saved: Saved) { links = saved.links; pending = saved.pending; queueTransfers = saved.queueTransfers ?? []; error = nil }
    private func mutate(_ body: (inout Saved) throws -> Void) throws {
        do {
            if let directory {
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
                let descriptor = Darwin.open(directory.appendingPathComponent("relationships.lock").path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
                guard descriptor >= 0 else { throw POSIXError(.EIO) }
                defer { Darwin.close(descriptor) }
                guard flock(descriptor, LOCK_EX) == 0 else { throw POSIXError(.EIO) }
                defer { _ = flock(descriptor, LOCK_UN) }
                var saved = try read(directory)
                try body(&saved)
                let file = directory.appendingPathComponent("relationships.json")
                try JSONEncoder().encode(saved).write(to: file, options: .atomic)
                try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
                install(saved)
            } else {
                var saved = Saved(links: links, pending: pending, queueTransfers: queueTransfers)
                try body(&saved)
                install(saved)
            }
        } catch {
            self.error = error.localizedDescription
            throw error
        }
    }
}
