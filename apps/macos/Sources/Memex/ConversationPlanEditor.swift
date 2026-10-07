import CryptoKit
import Foundation
import SwiftUI

/// A local editable copy, keyed by source session, native record and original
/// content. New provider revisions never overwrite a user's saved plan.
struct ConversationPlanDocument {
    let root: URL
    let source: String
    let path = "PLAN.md"

    init(plan: ConversationWork.Plan, session: Session, directory: URL = FileManager.default.homeDirectoryForCurrentUser
         .appendingPathComponent("Library/Application Support/dev.memex.app/Plans")) {
        source = "\(session.id)#\(plan.recordID)"
        let key = source + "\0" + plan.text
        let hash = SHA256.hash(data: Data(key.utf8)).map { String(format: "%02x", $0) }.joined()
        root = directory.appendingPathComponent(hash, isDirectory: true)
    }

    func open(original: String, client: WorkspaceFilesClient = .shared) async throws -> WorkspaceFileRevision {
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        return try await client.openOrCreate(root: root, name: path, contents: original)
    }

    func revised(_ plan: ConversationWork.Plan, revision: WorkspaceFileRevision) -> ConversationWork.Plan {
        var result = plan
        result.markdown = revision.contents
        result.savedSource = "\(source)\nEdited plan: \(root.appendingPathComponent(path).path)\nSHA-256: \(revision.fingerprint)"
        return result
    }
}

struct ConversationPlanEditor: View {
    let plan: ConversationWork.Plan
    let session: Session
    let canImplement: Bool
    let implement: (ConversationWork.Plan) -> Void
    let branch: (ConversationWork.Plan) -> Void
    @Environment(\.dismiss) private var dismiss
    @State private var text = ""
    @State private var loaded: WorkspaceFileRevision?
    @State private var error: String?
    @State private var busy = false
    @State private var diskVersion: WorkspaceFileRevision?
    private var document: ConversationPlanDocument { .init(plan: plan, session: session) }

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Plan document").font(.headline)
                Spacer()
                Button("Close") { dismiss() }.keyboardShortcut(.cancelAction)
            }
            Text("Editable local copy · Source \(plan.recordID)").font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
            if let error { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
            if let diskVersion {
                DisclosureGroup("Saved version changed on disk") {
                    ScrollView { Text(diskVersion.contents).textSelection(.enabled) }.frame(maxHeight: 150)
                    Button("Keep my edits and use this disk version as the save baseline") {
                        do {
                            try WorkspaceFileDrafts.shared.save(root: document.root, path: document.path,
                                original: diskVersion, edited: text)
                            loaded = diskVersion
                            self.diskVersion = nil
                            error = nil
                        } catch { self.error = error.localizedDescription }
                    }
                }
            }
            if loaded != nil {
                TextEditor(text: $text).font(.system(.body, design: .monospaced))
                    .onChange(of: text) { _, value in
                        guard let loaded else { return }
                        do { try WorkspaceFileDrafts.shared.save(root: document.root, path: document.path, original: loaded, edited: value) }
                        catch { self.error = error.localizedDescription }
                    }
                HStack {
                    Button("Save") { Task { _ = await save() } }
                    Button("Save and implement") { Task { if let revised = await save() { implement(revised); dismiss() } } }
                        .disabled(!canImplement)
                    Button("Save and implement in new conversation…") {
                        Task { if let revised = await save() { dismiss(); branch(revised) } }
                    }.disabled(!canImplement)
                    Spacer()
                    Text(text == loaded?.contents ? "Saved" : "Unsaved edits").font(.caption).foregroundStyle(.secondary)
                }.disabled(busy)
            } else {
                ScrollView { Text(plan.text).frame(maxWidth: .infinity, alignment: .leading).textSelection(.enabled) }
                if busy { ProgressView() }
                else { Text("Read-only: the local plan document could not be opened.").font(.caption).foregroundStyle(.secondary) }
            }
        }.padding(20).frame(minWidth: 650, idealWidth: 800, minHeight: 450, idealHeight: 650)
            .task(id: plan.recordID) {
                busy = true
                defer { busy = false }
                do {
                    let revision = try await document.open(original: plan.text)
                    if let draft = try WorkspaceFileDrafts.shared.read(root: document.root, path: document.path) {
                        loaded = WorkspaceFileRevision(contents: draft.original, fingerprint: draft.expectedFingerprint)
                        text = draft.edited
                        if revision.fingerprint != draft.expectedFingerprint { diskVersion = revision; error = WorkspaceFileError.conflict.localizedDescription }
                    } else { loaded = revision; text = revision.contents }
                } catch { self.error = error.localizedDescription }
            }
    }

    private func save() async -> ConversationWork.Plan? {
        guard let loaded else { return nil }
        busy = true
        defer { busy = false }
        do {
            let revision = try await WorkspaceFilesClient.shared.save(root: document.root, path: document.path,
                contents: text, expectedFingerprint: loaded.fingerprint)
            self.loaded = revision
            try WorkspaceFileDrafts.shared.discard(root: document.root, path: document.path)
            error = nil
            return document.revised(plan, revision: revision)
        } catch {
            self.error = error.localizedDescription
            diskVersion = try? await WorkspaceFilesClient.shared.read(root: document.root, path: document.path)
            return nil
        }
    }
}
