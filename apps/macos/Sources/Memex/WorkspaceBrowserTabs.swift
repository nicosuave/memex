import Combine
import Foundation

struct WorkspaceBrowserTabState: Codable {
    var id: UUID
    var history: [URL]
    var index: Int
    var zoom: Double
    var viewportWidth: Double?
}

struct WorkspaceBrowserState: Codable {
    var selectedID: UUID
    var tabs: [WorkspaceBrowserTabState]
}

/// Only navigation and presentation are persisted. Restoring a tab does not
/// restore an automation grant or grant access to another application's tabs.
@MainActor final class WorkspaceBrowserTabs: ObservableObject {
    let conversationID: String
    @Published private(set) var sessions: [WorkspaceBrowserSession] = []
    @Published var selectedID: UUID { didSet { persist() } }
    @Published var automationGrant: WorkspaceBrowserAutomationGrant?
    private let defaults: UserDefaults
    private var restoring = true
    private var key: String { "workspace-browser-tabs-" + conversationID }

    init(conversationID: String, defaults: UserDefaults = .standard) {
        self.conversationID = conversationID
        self.defaults = defaults
        selectedID = UUID()
        if let data = defaults.data(forKey: key), data.count <= 1_048_576,
           let saved = try? JSONDecoder().decode(WorkspaceBrowserState.self, from: data) {
            for state in saved.tabs.prefix(20) where !sessions.contains(where: { $0.id == state.id }) {
                let session = makeSession(id: state.id)
                sessions.append(session)
                session.restore(history: state.history, index: state.index, zoom: state.zoom, viewportWidth: state.viewportWidth)
            }
            if sessions.contains(where: { $0.id == saved.selectedID }) { selectedID = saved.selectedID }
        }
        if sessions.isEmpty { sessions = [makeSession(id: selectedID)] }
        if !sessions.contains(where: { $0.id == selectedID }) { selectedID = sessions[0].id }
        restoring = false
    }

    var selected: WorkspaceBrowserSession { sessions.first { $0.id == selectedID } ?? sessions[0] }

    @discardableResult func add(url: URL? = nil) -> WorkspaceBrowserSession? {
        guard sessions.count < 20 else { return nil }
        let session = makeSession(id: UUID())
        sessions.append(session)
        selectedID = session.id
        if let url { _ = session.navigate(to: url) }
        persist()
        return session
    }

    func close(_ id: UUID) {
        guard let session = sessions.first(where: { $0.id == id }) else { return }
        session.stopLoading()
        session.webView.removeFromSuperview()
        session.stateDidChange = nil
        sessions.removeAll { $0.id == id }
        if sessions.isEmpty { _ = add() }
        if selectedID == id { selectedID = sessions[0].id }
        persist()
    }

    func discard() {
        for session in sessions { session.stateDidChange = nil; session.stopLoading() }
        defaults.removeObject(forKey: key)
    }

    private func makeSession(id: UUID) -> WorkspaceBrowserSession {
        let session = WorkspaceBrowserSession(id: id)
        session.stateDidChange = { [weak self] in self?.persist() }
        session.openTab = { [weak self] url in _ = self?.add(url: url) }
        return session
    }

    private func persist() {
        guard !restoring else { return }
        let state = WorkspaceBrowserState(selectedID: selectedID, tabs: sessions.map {
            WorkspaceBrowserTabState(id: $0.id, history: $0.history, index: $0.historyIndex,
                                     zoom: $0.zoom, viewportWidth: $0.viewportWidth)
        })
        if let data = try? JSONEncoder().encode(state) { defaults.set(data, forKey: key) }
    }
}
