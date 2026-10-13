import Foundation
import Observation

/// A local navigation catalog, not a transcript store. Entries are created only
/// after the provider returns its native session identity and transcript path.
@MainActor @Observable
final class CreatedConversationCatalog {
    struct Context: Codable, Equatable, Sendable {
        let projectID: String?
        let projectName: String
        let workspace: ConversationWorkspace
    }

    private(set) var sessions: [Session] = []
    private(set) var contexts: [String: Context] = [:]
    private(set) var error: String?
    @ObservationIgnored private let directory: URL?
    @ObservationIgnored private var canWrite = true

    private struct Saved: Codable {
        let version: Int
        let sessions: [Session]
        var contexts: [String: Context]?
    }

    init(directory: URL? = nil) {
        self.directory = directory
        guard let directory else { return }
        do {
            let data = try Data(contentsOf: directory.appendingPathComponent("conversations.json"))
            let saved = try JSONDecoder().decode(Saved.self, from: data)
            guard saved.version == 1 else { throw CocoaError(.fileReadCorruptFile) }
            sessions = saved.sessions
            contexts = saved.contexts ?? [:]
        } catch let failure as CocoaError where failure.code == .fileReadNoSuchFile {
        } catch {
            canWrite = false
            self.error = "Saved conversations could not be read. Their catalog has been preserved."
        }
    }

    static func persistent() -> CreatedConversationCatalog {
        CreatedConversationCatalog(directory: FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/dev.memex.app/Conversations", isDirectory: true))
    }

    func contains(_ session: Session) -> Bool { sessions.contains { $0.id == session.id } }

    func save(_ session: Session, context: Context? = nil) {
        let contextChanged = context != nil && contexts[session.id] != context
        if let context { contexts[session.id] = context }
        if let index = sessions.firstIndex(where: { $0.id == session.id }) {
            guard sessions[index] != session || contextChanged || error != nil else { return }
            sessions[index] = session
        } else { sessions.insert(session, at: 0) }
        retrySave()
    }

    func retrySave() {
        guard canWrite, let directory else { return }
        do {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                                                    attributes: [.posixPermissions: 0o700])
            let file = directory.appendingPathComponent("conversations.json")
            try JSONEncoder().encode(Saved(version: 1, sessions: sessions, contexts: contexts)).write(to: file, options: .atomic)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
            error = nil
        } catch {
            self.error = "This conversation could not be saved to the list. Keep Memex open until saving succeeds."
        }
    }

    func merging(_ indexed: [Session], machines: [String], project: String?, filters: ConversationFilters,
                 query: String?, since: String?, limit: Int) -> [Session] {
        let saved = Dictionary(sessions.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
        let indexed = indexed.map { row in
            var row = row
            if let fallback = saved[row.id] {
                row.label = row.label ?? fallback.label
                row.cwd = row.cwd ?? fallback.cwd
            }
            return row
        }
        let indexedIDs = Set(indexed.map(\.id))
        let local = sessions.filter { session in
            guard !indexedIDs.contains(session.id), machines.contains(session.machineID),
                  project == nil || session.project == project || session.repoProject == project,
                  filters.provider == .all || session.source == filters.provider.rawValue,
                  filters.origin != .subagent,
                  since == nil || (session.lastAt ?? "") >= since! else { return false }
            if let query = query?.nilIfBlank {
                return session.title.localizedCaseInsensitiveContains(query)
            }
            return true
        }
        // Indexed search results keep their source-backed rank and exact anchors.
        if query?.nilIfBlank != nil { return Array((indexed + local).prefix(limit)) }
        return Array((indexed + local).sorted {
            let left = $0.date ?? .distantPast, right = $1.date ?? .distantPast
            return left == right ? $0.id < $1.id : left > right
        }.prefix(limit))
    }
}
