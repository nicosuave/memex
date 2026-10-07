import Darwin
import Foundation
import Observation

/// Presentation metadata owned by Memex. Native transcripts and draft/runtime
/// registrations are never modified by an organization action.
@MainActor @Observable
final class ConversationLibrary {
    enum Scope: String, CaseIterable, Identifiable {
        case active, archived, removed
        var id: Self { self }
        var title: String {
            switch self {
            case .active: "Conversations"
            case .archived: "Archived"
            case .removed: "Removed from Memex"
            }
        }
    }

    struct Entry: Codable, Equatable {
        /// Keep the original title and source identity recoverable.
        var session: Session
        var title: String?
        var pinned = false
        var archived = false
        var removed = false
        var order: Int?
        var sectionID: String?
        var unread: Bool?
    }

    struct CustomSection: Codable, Equatable, Identifiable {
        var id: String
        var name: String
    }

    private struct Saved: Codable {
        var version = 2
        var entries: [String: Entry] = [:]
        // Optional on disk so version 1 organization files migrate without loss.
        var sections: [CustomSection]?
    }

    private(set) var entries: [String: Entry] = [:]
    private(set) var sections: [CustomSection] = []
    private(set) var error: String?
    @ObservationIgnored private let directory: URL?

    init(directory: URL? = nil) {
        self.directory = directory
        reload()
    }

    static func persistent() -> ConversationLibrary {
        ConversationLibrary(directory: FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/dev.memex.app/Conversations", isDirectory: true))
    }

    func reload() {
        guard let directory else { return }
        do {
            let saved = try read(directory)
            entries = saved.entries
            sections = saved.sections ?? []
            error = nil
        } catch {
            self.error = "Conversation organization could not be read. Saved metadata has been preserved. \(error.localizedDescription)"
        }
    }

    func title(for session: Session) -> String { entries[session.id]?.title ?? session.title }
    func isPinned(_ session: Session) -> Bool { entries[session.id]?.pinned == true }
    func isUnread(_ session: Session) -> Bool { entries[session.id]?.unread == true }
    func sectionID(for session: Session) -> String? { entries[session.id]?.sectionID }

    @discardableResult
    func markRead(_ sessions: [Session], read: Bool) -> Bool {
        update(sessions) { $0.unread = !read }
    }

    @discardableResult
    func createSection(named name: String) -> Bool {
        guard let name = name.trimmingCharacters(in: .whitespacesAndNewlines).nilIfBlank else { return false }
        return mutate { saved in
            var sections = saved.sections ?? []
            sections.append(CustomSection(id: UUID().uuidString, name: name))
            saved.sections = sections
        }
    }

    @discardableResult
    func renameSection(_ id: String, to name: String) -> Bool {
        guard let name = name.trimmingCharacters(in: .whitespacesAndNewlines).nilIfBlank else { return false }
        return mutate { saved in
            guard let index = saved.sections?.firstIndex(where: { $0.id == id }) else { return }
            saved.sections?[index].name = name
        }
    }

    /// Deleting a section returns its chats to the ordinary list; their other metadata survives.
    @discardableResult
    func deleteSection(_ id: String) -> Bool {
        mutate { saved in
            saved.sections?.removeAll { $0.id == id }
            for key in Array(saved.entries.keys) where saved.entries[key]?.sectionID == id {
                saved.entries[key]?.sectionID = nil
            }
        }
    }

    @discardableResult
    func move(_ sessions: [Session], toSection id: String?) -> Bool {
        mutate { saved in
            // Validate against the locked, freshly loaded metadata, not a stale window snapshot.
            guard id == nil || saved.sections?.contains(where: { $0.id == id }) == true else { return }
            for session in sessions {
                Self.capture(session, in: &saved)
                saved.entries[session.id]?.sectionID = id
            }
        }
    }

    @discardableResult
    func moveSection(_ id: String, by offset: Int) -> Bool {
        mutate { saved in
            guard var sections = saved.sections, let index = sections.firstIndex(where: { $0.id == id }),
                  sections.indices.contains(index + offset) else { return }
            sections.swapAt(index, index + offset)
            saved.sections = sections
        }
    }

    func savedSession(id: String) -> Session? { entries[id]?.session }

    @discardableResult
    func retain(_ session: Session) -> Bool { update([session]) { _ in } }

    func includes(_ session: Session, in scope: Scope) -> Bool {
        let entry = entries[session.id]
        switch scope {
        case .active: return entry?.archived != true && entry?.removed != true
        case .archived: return entry?.archived == true && entry?.removed != true
        case .removed: return entry?.removed == true
        }
    }

    func presenting(_ session: Session) -> Session {
        var presented = session
        if let title = entries[session.id]?.title { presented.label = title }
        return presented
    }

    @discardableResult
    func rename(_ session: Session, to title: String) -> Bool {
        update([session]) { $0.title = title.trimmingCharacters(in: .whitespacesAndNewlines).nilIfBlank }
    }

    @discardableResult
    func pin(_ sessions: [Session], pinned: Bool) -> Bool {
        update(sessions) { $0.pinned = pinned }
    }

    @discardableResult
    func archive(_ sessions: [Session], archived: Bool) -> Bool {
        update(sessions) { $0.archived = archived }
    }

    /// A reversible local display tombstone, never deletion of provider history.
    @discardableResult
    func remove(_ sessions: [Session]) -> Bool { update(sessions) { $0.removed = true } }

    @discardableResult
    func restore(_ sessions: [Session]) -> Bool {
        update(sessions) { $0.removed = false; $0.archived = false }
    }

    @discardableResult
    func resetOrder(_ sessions: [Session]) -> Bool { update(sessions) { $0.order = nil } }

    func sorted(_ sessions: [Session], preservingSearchRank: Bool = false) -> [Session] {
        guard !preservingSearchRank else { return sessions }
        return sessions.sorted { lhs, rhs in
            let left = entries[lhs.id], right = entries[rhs.id]
            if (left?.pinned == true) != (right?.pinned == true) { return left?.pinned == true }
            if left?.order != right?.order, left?.order != nil || right?.order != nil {
                return (left?.order ?? Int.max) < (right?.order ?? Int.max)
            }
            if lhs.lastAt != rhs.lastAt { return (lhs.lastAt ?? "") > (rhs.lastAt ?? "") }
            return lhs.id < rhs.id
        }
    }

    /// Reorder exactly the visible subset, retaining positions of conversations
    /// hidden by the current machine/project filters.
    @discardableResult
    func reorder(_ visible: [Session], from offsets: IndexSet, to destination: Int) -> Bool {
        guard !offsets.isEmpty, offsets.allSatisfy(visible.indices.contains),
              (0...visible.count).contains(destination) else { return false }
        let moving = offsets.sorted().map { visible[$0] }
        var ordered = visible.enumerated().filter { !offsets.contains($0.offset) }.map(\.element)
        let insertion = destination - offsets.filter { $0 < destination }.count
        ordered.insert(contentsOf: moving, at: insertion)
        // Pinning is explicit. Reordering doesn't silently unpin a conversation.
        let pins = ordered.filter(isPinned), unpinned = ordered.filter { !isPinned($0) }
        return mutate { saved in
            for session in visible { Self.capture(session, in: &saved) }
            for group in [pins, unpinned] {
                guard !group.isEmpty else { continue }
                let ids = Set(group.map(\.id))
                let all = saved.entries.values.filter { $0.pinned == (group.first.map(isPinned) ?? false) }
                    .sorted {
                        let lhs = $0.order ?? Int.max, rhs = $1.order ?? Int.max
                        return lhs == rhs ? $0.session.id < $1.session.id : lhs < rhs
                    }
                var replacement = group.makeIterator()
                let merged = all.map { entry in ids.contains(entry.session.id) ? replacement.next()!.id : entry.session.id }
                for (rank, id) in merged.enumerated() { saved.entries[id]?.order = rank }
            }
        }
    }

    private func update(_ sessions: [Session], change: (inout Entry) -> Void) -> Bool {
        guard !sessions.isEmpty else { return true }
        return mutate { saved in
            for session in sessions {
                Self.capture(session, in: &saved)
                change(&saved.entries[session.id]!)
            }
        }
    }

    private static func capture(_ session: Session, in saved: inout Saved) {
        if saved.entries[session.id] == nil { saved.entries[session.id] = Entry(session: session) }
        // Callers pass native rows, never the presentation copy with its title override.
        else { saved.entries[session.id]?.session = session }
    }

    private func read(_ directory: URL) throws -> Saved {
        let file = directory.appendingPathComponent("organization.json")
        do {
            let saved = try JSONDecoder().decode(Saved.self, from: Data(contentsOf: file))
            guard (1...2).contains(saved.version) else { throw CocoaError(.fileReadCorruptFile) }
            return saved
        } catch let failure as CocoaError where failure.code == .fileReadNoSuchFile {
            return Saved()
        }
    }

    /// Read-modify-write under a process lock so two Memex windows/processes
    /// cannot overwrite each other's metadata. Publish UI changes after saving.
    private func mutate(_ body: (inout Saved) -> Void) -> Bool {
        do {
            if let directory {
                try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                                                        attributes: [.posixPermissions: 0o700])
                let descriptor = Darwin.open(directory.appendingPathComponent("organization.lock").path,
                                             O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
                guard descriptor >= 0 else { throw POSIXError(.EIO) }
                defer { Darwin.close(descriptor) }
                guard flock(descriptor, LOCK_EX | LOCK_NB) == 0 else { throw POSIXError(.EAGAIN) }
                defer { _ = flock(descriptor, LOCK_UN) }
                var saved = try read(directory)
                body(&saved)
                saved.version = 2
                let file = directory.appendingPathComponent("organization.json")
                try JSONEncoder().encode(saved).write(to: file, options: [.atomic])
                try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
                entries = saved.entries
                sections = saved.sections ?? []
            } else {
                var saved = Saved(entries: entries, sections: sections)
                body(&saved)
                entries = saved.entries
                sections = saved.sections ?? []
            }
            error = nil
            return true
        } catch {
            self.error = "Conversation organization was not saved. No provider history was changed. \(error.localizedDescription)"
            return false
        }
    }
}
