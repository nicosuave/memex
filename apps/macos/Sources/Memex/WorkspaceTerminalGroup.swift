import Foundation
import Observation
import CryptoKit

/// Shells are process-owned by this app and retained across chat/pane switches.
/// Saved snapshots are evidence only; they are never replayed into a new shell.
@MainActor @Observable final class WorkspaceTerminalGroup {
    enum Layout: String, CaseIterable, Codable { case tabs, sideBySide, stacked }
    private struct SavedShell: Codable {
        let id: UUID
        let history: String
    }
    private struct SavedWorkspace: Codable {
        /// Absent in snapshots written before pruning existed.
        var directory: String? = nil
        let selectedID: UUID
        let layout: Layout
        let shells: [SavedShell]
        let lastSnapshot: String
    }
    let directory: URL
    private(set) var sessions: [WorkspaceTerminalSession]
    var selectedID: UUID
    var layout: Layout = .tabs
    private(set) var savedHistory: String = ""
    private(set) var historyError: String?
    @ObservationIgnored private let historyURL: URL

    init(directory: URL, initial: WorkspaceTerminalSession, historyDirectory: URL? = nil) {
        self.directory = directory
        sessions = [initial]
        selectedID = initial.id
        historyURL = Self.historyURL(directory: directory, historyDirectory: historyDirectory ?? Self.defaultHistoryDirectory)
        if let data = try? Data(contentsOf: historyURL), data.count <= 20_971_520,
           let saved = try? JSONDecoder().decode(SavedWorkspace.self, from: data), !saved.shells.isEmpty {
            sessions = saved.shells.prefix(16).map {
                WorkspaceTerminalSession(directory: directory, id: $0.id, restoredHistory: String($0.history.suffix(262_144)))
            }
            selectedID = sessions.contains { $0.id == saved.selectedID } ? saved.selectedID : sessions[0].id
            layout = saved.layout
            savedHistory = String(saved.lastSnapshot.suffix(262_144))
        }
        for session in sessions { observeFocus(session) }
    }

    var selected: WorkspaceTerminalSession { sessions.first { $0.id == selectedID } ?? sessions[0] }
    var visible: [WorkspaceTerminalSession] {
        layout == .tabs ? [selected] : sessions
    }

    @discardableResult func add() -> WorkspaceTerminalSession {
        guard sessions.count < 16 else { return selected }
        let session = WorkspaceTerminalSession(directory: directory)
        observeFocus(session)
        sessions.append(session)
        selectedID = session.id
        return session
    }

    /// The caller confirms live process termination before calling this method.
    func remove(_ session: WorkspaceTerminalSession) {
        guard sessions.contains(where: { $0 === session }) else { return }
        saveHistory(from: session)
        session.close()
        sessions.removeAll { $0 === session }
        if sessions.isEmpty { _ = add() }
        if !sessions.contains(where: { $0.id == selectedID }) { selectedID = sessions[0].id }
        persist()
    }

    func saveHistory(from session: WorkspaceTerminalSession) {
        let text = session.captureHistory()
        guard !text.isEmpty else { return }
        savedHistory = text
        persist()
    }

    func saveAllHistory() {
        for session in sessions { _ = session.captureHistory() }
        if !selected.capturedHistory.isEmpty { savedHistory = selected.capturedHistory }
        persist()
    }

    private func observeFocus(_ session: WorkspaceTerminalSession) {
        session.didFocus = { [weak self, weak session] in
            if let session { self?.selectedID = session.id }
        }
    }

    private func persist() {
        do {
            try FileManager.default.createDirectory(at: historyURL.deletingLastPathComponent(), withIntermediateDirectories: true,
                                                   attributes: [.posixPermissions: 0o700])
            // A removed checkout keeps no scrollback behind; startup pruning
            // covers checkouts removed while the app was not running.
            guard FileManager.default.fileExists(atPath: directory.path) else {
                try? FileManager.default.removeItem(at: historyURL)
                return
            }
            let saved = SavedWorkspace(directory: directory.path, selectedID: selectedID, layout: layout,
                                       shells: sessions.map { SavedShell(id: $0.id, history: $0.capturedHistory) }, lastSnapshot: savedHistory)
            try JSONEncoder().encode(saved).write(to: historyURL, options: .atomic)
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: historyURL.path)
            historyError = nil
        } catch { historyError = error.localizedDescription }
    }

    /// Scrollback lives with the app's other state under dev.memex.app.
    nonisolated static var defaultHistoryDirectory: URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("dev.memex.app/TerminalHistory", isDirectory: true)
    }

    nonisolated private static func historyURL(directory: URL, historyDirectory: URL) -> URL {
        let name = SHA256.hash(data: Data(directory.path.utf8)).map { String(format: "%02x", $0) }.joined()
        return historyDirectory.appendingPathComponent(name + ".json")
    }

    /// Forget a removed workspace's saved scrollback. `directory` is the
    /// canonical worktree root the terminal group was opened with.
    nonisolated static func discardHistory(directory: URL, historyDirectory: URL = defaultHistoryDirectory) {
        try? FileManager.default.removeItem(at: historyURL(directory: directory, historyDirectory: historyDirectory))
    }

    /// Moves snapshots from the former `Application Support/Memex` folder into
    /// the app's standard support folder. Existing new-location files win.
    nonisolated static func migrateHistory(historyDirectory: URL = defaultHistoryDirectory, legacyDirectory: URL =
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("Memex/TerminalHistory", isDirectory: true)) {
        let manager = FileManager.default
        guard let legacy = try? manager.contentsOfDirectory(at: legacyDirectory, includingPropertiesForKeys: nil) else { return }
        try? manager.createDirectory(at: historyDirectory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        for file in legacy where file.pathExtension == "json" {
            let destination = historyDirectory.appendingPathComponent(file.lastPathComponent)
            if manager.fileExists(atPath: destination.path) { try? manager.removeItem(at: file) }
            else { try? manager.moveItem(at: file, to: destination) }
        }
        if (try? manager.contentsOfDirectory(atPath: legacyDirectory.path))?.isEmpty == true {
            try? manager.removeItem(at: legacyDirectory)
            let parent = legacyDirectory.deletingLastPathComponent()
            if (try? manager.contentsOfDirectory(atPath: parent.path))?.isEmpty == true { try? manager.removeItem(at: parent) }
        }
    }

    /// Deletes snapshots whose recorded workspace no longer exists, such as a
    /// worktree removed outside this window. Snapshots written before the
    /// directory was recorded are kept until their group rewrites them.
    nonisolated static func pruneHistory(historyDirectory: URL = defaultHistoryDirectory) {
        let manager = FileManager.default
        for file in (try? manager.contentsOfDirectory(at: historyDirectory, includingPropertiesForKeys: nil)) ?? []
            where file.pathExtension == "json" {
            guard let data = try? Data(contentsOf: file), data.count <= 20_971_520,
                  let saved = try? JSONDecoder().decode(SavedWorkspace.self, from: data),
                  let path = saved.directory else { continue }
            if !manager.fileExists(atPath: path) { try? manager.removeItem(at: file) }
        }
    }
}
