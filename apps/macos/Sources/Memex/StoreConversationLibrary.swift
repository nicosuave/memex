import Foundation

extension Store {
    /// Source-backed rows remain in `sessions`; only presentation uses overrides
    /// and local archive/removal state. Searches retain their record anchors.
    var librarySessions: [Session] {
        var rows = sessions
        let ids = Set(rows.map(\.id))
        let since = filters.timeframe.since(relativeTo: Date())
        let machines = Set(selectedMachineIDs)
        let project = selectedProject
        for entry in conversationLibrary.entries.values.sorted(by: { $0.session.id < $1.session.id }) where !ids.contains(entry.session.id) {
            let session = entry.session
            guard machines.contains(session.machineID),
                  project == nil || session.project == project || session.repoProject == project,
                  filters.provider == .all || filters.provider.rawValue == session.source,
                  since == nil || (session.lastAt ?? "") >= since! else { continue }
            switch filters.origin {
            case .interactive: if session.isSubagent || session.conversationKind == "guardian_review" { continue }
            case .subagent: if !session.isSubagent { continue }
            case .all: if session.conversationKind == "guardian_review" { continue }
            case .includingReviews: break
            }
            if let query = query.nilIfBlank,
               !conversationLibrary.title(for: session).localizedCaseInsensitiveContains(query) { continue }
            rows.append(session)
        }
        return rows.filter { conversationLibrary.includes($0, in: conversationLibraryScope) }
            .map(conversationLibrary.presenting)
    }

    func conversationTitle(_ session: Session) -> String { conversationLibrary.title(for: session) }

    /// Resolve UI copies back to the original row before saving metadata.
    func nativeLibrarySession(_ session: Session) -> Session {
        sessions.first { $0.id == session.id } ?? conversationLibrary.savedSession(id: session.id) ?? session
    }

    func librarySelection(_ ids: Set<String>) -> [Session] {
        librarySessions.filter { ids.contains($0.id) }.map(nativeLibrarySession)
    }

    func finishLibraryAction(_ succeeded: Bool) {
        guard succeeded else { return }
        if let selectedID, !librarySessions.contains(where: { $0.id == selectedID }) {
            self.selectedID = nil
        }
    }
}
