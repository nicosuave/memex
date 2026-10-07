import AppKit
import SwiftUI

/// Root mounts this once per workspace (`.id(directory)`). Edits are never
/// autosaved: a file switch asks for an explicit save or discard decision.
struct WorkspaceFilesView: View {
    let directory: URL
    var addContext: ((String) -> Bool)? = nil
    @State private var folder = ""
    @State private var entries: [WorkspaceFileEntry] = []
    @State private var selected: String?
    @State private var loaded: WorkspaceFileRevision?
    @State private var text = ""
    @State private var error: String?
    @State private var busy = false
    @State private var nextSelection: String?
    @State private var confirmDiscard = false
    @State private var diskVersion: WorkspaceFileRevision?
    @State private var rendered = false
    @State private var previewImage: NSImage?
    @State private var previewURL: URL?
    @State private var creating = false
    @State private var creatingDirectory = false
    @State private var newName = ""
    private let client = WorkspaceFilesClient.shared
    private var dirty: Bool { loaded.map { $0.contents != text } ?? false }

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Button("Up", systemImage: "arrow.up") {
                    folder = folder.split(separator: "/").dropLast().joined(separator: "/")
                }.disabled(folder.isEmpty)
                Text(folder.isEmpty ? directory.lastPathComponent : folder).lineLimit(1)
                Spacer()
                Menu("New") {
                    Button("File…") { creatingDirectory = false; newName = ""; creating = true }
                    Button("Folder…") { creatingDirectory = true; newName = ""; creating = true }
                }.disabled(busy)
                Button("Refresh", systemImage: "arrow.clockwise") { Task { await list() } }
            }.padding(8)
            Divider()
            HSplitView {
                List(entries) { entry in
                    Button {
                        if entry.isDirectory && !entry.isSymbolicLink { folder = entry.path }
                        else { requestOpen(entry.path) }
                    } label: {
                        Label(entry.name, systemImage: entry.isSymbolicLink ? "link" : entry.isDirectory ? "folder" : "doc")
                            .frame(maxWidth: .infinity, alignment: .leading).contentShape(Rectangle())
                    }.buttonStyle(.plain).disabled(entry.isSymbolicLink || busy)
                        .listRowBackground(selected == entry.path ? Color.accentColor.opacity(0.15) : .clear)
                }.frame(minWidth: 150, idealWidth: 220, maxWidth: 350)
                VStack(alignment: .leading, spacing: 0) {
                    if let selected {
                        HStack {
                            Text(selected + (dirty ? " •" : "")).lineLimit(1).truncationMode(.middle)
                            Spacer()
                            if ["md", "markdown", "html", "htm", "csv", "tsv"].contains(URL(fileURLWithPath: selected).pathExtension.lowercased()), loaded != nil {
                                Toggle("Preview", isOn: $rendered).toggleStyle(.button)
                            }
                            Button("Open") { NSWorkspace.shared.open(directory.appendingPathComponent(selected)) }
                            if let addContext {
                                Button("Add to chat") {
                                    if !addContext("File: \(directory.appendingPathComponent(selected).path)\n\n\(text)") {
                                        error = "The file could not be added to this chat. Your file and edits are unchanged."
                                    }
                                }
                                    .disabled(loaded == nil)
                            }
                            Button("Save") { Task { await save() } }.keyboardShortcut("s", modifiers: .command)
                                .disabled(!dirty || busy)
                        }.font(.caption).padding(8)
                        Divider()
                        if let diskVersion {
                            HStack {
                                Text("Disk changed. Compare your edits (left) with disk (right).")
                                Spacer()
                                Button("Keep my edits") {
                                    // Acknowledges the compared disk version, but does
                                    // not write. Save will check this new version again.
                                    loaded = diskVersion
                                    self.diskVersion = nil
                                    error = nil
                                    persistDraft()
                                }
                                Button("Use disk") {
                                    loaded = diskVersion
                                    text = diskVersion.contents
                                    self.diskVersion = nil
                                    error = nil
                                }
                            }.font(.caption).padding(8)
                            HSplitView {
                                editor
                                ScrollView { Text(diskVersion.contents).font(.system(.body, design: .monospaced))
                                    .textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading).padding(8) }
                            }
                        } else if loaded != nil {
                            if rendered {
                                renderedPreview(path: selected)
                            } else { editor }
                        } else if let image = previewImage {
                            ScrollView([.horizontal, .vertical]) { Image(nsImage: image).resizable().scaledToFit() }
                        } else if let previewURL, previewURL.pathExtension.lowercased() == "pdf" {
                            WorkspacePDFPreview(url: previewURL)
                        } else if let previewURL, ["mp4", "mov", "m4v", "mp3", "m4a", "wav", "aac", "flac", "aiff"].contains(previewURL.pathExtension.lowercased()) {
                            WorkspaceMediaPreview(url: previewURL)
                        } else {
                            ContentUnavailableView("Preview unavailable", systemImage: "doc",
                                description: Text("Use Open to view this file in its default application."))
                        }
                    } else {
                        ContentUnavailableView("Workspace files", systemImage: "folder", description: Text("Select a file to read or edit."))
                    }
                }.frame(minWidth: 280, maxWidth: .infinity, maxHeight: .infinity)
            }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled).padding(8) }
        }
        .task(id: folder) { await list() }
        .onChange(of: text) { _, _ in persistDraft() }
        .sheet(isPresented: $creating) {
            VStack(alignment: .leading, spacing: 12) {
                Text(creatingDirectory ? "New folder" : "New file").font(.headline)
                TextField("Name", text: $newName)
                if let error { Text(error).font(.caption).foregroundStyle(.orange) }
                HStack {
                    Spacer()
                    Button("Cancel") { creating = false }
                    Button("Create") { Task { await create() } }.disabled(newName.nilIfBlank == nil || busy)
                }
            }.padding(20).frame(width: 350)
        }
        .confirmationDialog("Save changes before opening another file?", isPresented: $confirmDiscard, titleVisibility: .visible) {
            Button("Save and open") { Task { if await save(), let nextSelection { await open(nextSelection) } } }
            Button("Discard edits and open", role: .destructive) {
                do {
                    if let selected { try WorkspaceFileDrafts.shared.discard(root: directory, path: selected) }
                    if let nextSelection { Task { await open(nextSelection) } }
                } catch { self.error = error.localizedDescription }
            }
            Button("Cancel", role: .cancel) { nextSelection = nil }
        }
    }

    private var editor: some View {
        TextEditor(text: $text).font(.system(.body, design: .monospaced)).disabled(busy)
    }

    @ViewBuilder private func renderedPreview(path: String) -> some View {
        switch URL(fileURLWithPath: path).pathExtension.lowercased() {
        case "html", "htm": WorkspaceHTMLPreview(text: text)
        case "csv": WorkspaceDelimitedPreview(text: text, delimiter: ",")
        case "tsv": WorkspaceDelimitedPreview(text: text, delimiter: "\t")
        default: ScrollView { WorkspaceMarkdownPreview(text: text).frame(maxWidth: .infinity, alignment: .leading).padding() }
        }
    }

    private func requestOpen(_ path: String) {
        guard path != selected else { return }
        if dirty { nextSelection = path; confirmDiscard = true }
        else { Task { await open(path) } }
    }

    @MainActor private func create() async {
        busy = true
        defer { busy = false }
        do {
            let path = try await client.create(root: directory, folder: folder, name: newName, isDirectory: creatingDirectory)
            creating = false
            error = nil
            await list()
            if !creatingDirectory { requestOpen(path) }
        } catch { self.error = error.localizedDescription }
    }

    @MainActor private func list() async {
        let requested = folder
        do {
            let result = try await client.entries(root: directory, path: requested)
            guard !Task.isCancelled, requested == folder else { return }
            entries = result
        } catch { self.error = error.localizedDescription }
    }

    @MainActor private func open(_ path: String) async {
        busy = true
        defer { busy = false }
        selected = path
        loaded = nil
        diskVersion = nil
        previewImage = nil
        previewURL = nil
        text = ""
        error = nil
        do {
            let revision = try await client.read(root: directory, path: path)
            if let draft = try WorkspaceFileDrafts.shared.read(root: directory, path: path) {
                loaded = WorkspaceFileRevision(contents: draft.original, fingerprint: draft.expectedFingerprint)
                text = draft.edited
                if revision.fingerprint != draft.expectedFingerprint { diskVersion = revision }
            } else {
                loaded = revision
                text = revision.contents
            }
        } catch {
            self.error = error.localizedDescription
            if let url = try? await client.fileURL(root: directory, path: path) {
                let extensionName = url.pathExtension.lowercased()
                if ["mp4", "mov", "m4v", "mp3", "m4a", "wav", "aac", "flac", "aiff"].contains(extensionName) {
                    previewURL = url
                    self.error = nil
                } else if let size = try? url.resourceValues(forKeys: [.fileSizeKey]).fileSize, size <= 20_000_000 {
                    previewURL = url
                    if ["png", "jpg", "jpeg", "gif", "webp", "tiff", "tif", "heic", "heif", "bmp", "ico"].contains(extensionName) {
                        previewImage = NSImage(contentsOf: url)
                    }
                    if previewImage != nil || extensionName == "pdf" { self.error = nil }
                }
            }
        }
    }

    @MainActor @discardableResult private func save() async -> Bool {
        guard let selected, let loaded, !busy else { return false }
        busy = true
        defer { busy = false }
        do {
            self.loaded = try await client.save(root: directory, path: selected, contents: text,
                                                 expectedFingerprint: loaded.fingerprint)
            try WorkspaceFileDrafts.shared.discard(root: directory, path: selected)
            error = nil
            diskVersion = nil
            return true
        } catch {
            self.error = error.localizedDescription
            if case WorkspaceFileError.conflict = error {
                diskVersion = try? await client.read(root: directory, path: selected)
            }
            return false
        }
    }

    private func persistDraft() {
        guard let selected, let loaded else { return }
        do { try WorkspaceFileDrafts.shared.save(root: directory, path: selected, original: loaded, edited: text) }
        catch { self.error = "Your file edits could not be saved as a draft: \(error.localizedDescription)" }
    }
}
