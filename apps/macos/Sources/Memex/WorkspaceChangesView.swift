import AppKit
import SwiftUI

struct WorkspaceChangesView: View {
    let directory: URL
    var isWorking = false
    var initialSelectedPath: String? = nil
    var reviewRequest: UUID? = nil
    var close: (() -> Void)? = nil
    var conversationID: String? = nil
    var isolation: (() -> WorkspaceIsolation?)? = nil
    var rewindConversation: ((WorkspaceCheckpoint) async throws -> Void)? = nil
    var addReviewContext: ((WorkspaceReviewContext) -> Bool)? = nil
    var setupCommand: String? = nil
    @State private var state = WorkspaceChangesState()
    @State private var refreshID = UUID()
    @State private var scope = WorkspaceDiffScope.current
    @State private var displayedScope = WorkspaceDiffScope.current
    @State private var branchRef = ""
    @State private var splitDiff = false
    @State private var reviewComment = ""
    @State private var actionError: String?
    @State private var indexBusy = false
    @State private var showingSetup = false
    @State private var showingPullRequest = false
    @State private var pullRequestURL: URL?
    private let client = WorkspaceChangesClient()

    private var displayedRoot: String {
        (state.directory == directory ? state.snapshot?.root.path : nil) ?? directory.path
    }

    private struct PatchTask: Equatable {
        let directory: URL
        let path: String?
        let revision: UUID
    }

    private struct RefreshTask: Equatable {
        let directory: URL
        let revision: UUID
        let isWorking: Bool
        let scope: WorkspaceDiffScope
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 8) {
                Label(displayedRoot, systemImage: "folder")
                    .font(.system(size: 12)).foregroundStyle(.secondary).lineLimit(1).truncationMode(.middle)
                    .help(displayedRoot)
                Spacer()
                if state.loading { ProgressView().controlSize(.small).help("Refreshing workspace changes") }
                if let close {
                    Button(action: close) { Image(systemName: "xmark") }
                        .help("Close workspace changes").accessibilityLabel("Close workspace changes")
                }
                Button { refreshID = UUID() } label: { Image(systemName: "arrow.clockwise").frame(width: 28, height: 28) }
                    .help("Refresh workspace changes").accessibilityLabel("Refresh workspace changes")
                    .disabled(state.loading)
            }
            .buttonStyle(.plain).font(.system(size: 11))
            .padding(.horizontal, 10).padding(.vertical, 6).background(.bar)
            HStack {
                Menu(scope.label) {
                    Button("Current working tree") { scope = .current }
                    Button("Branch comparison") { if branchRef.nilIfBlank != nil { scope = .branch(branchRef) } }
                        .disabled(branchRef.nilIfBlank == nil)
                }
                TextField("Base ref", text: $branchRef).frame(maxWidth: 140)
                    .onSubmit { if branchRef.nilIfBlank != nil { scope = .branch(branchRef) } }
                Toggle("Split", isOn: $splitDiff).toggleStyle(.button)
                Spacer()
                Button("Pull request…") { pullRequestURL = nil; showingPullRequest = true }
                WorkspaceGitActionsView(directory: directory, refreshKey: refreshID.uuidString + String(isWorking), didChange: { refreshID = UUID() },
                    didCreatePullRequest: { pullRequestURL = $0; showingPullRequest = true })
                    .disabled(isWorking)
                if setupCommand?.nilIfBlank != nil {
                    Button("Run setup…") { showingSetup = true }.disabled(isWorking)
                }
            }.font(.caption).padding(.horizontal, 10).padding(.vertical, 6)
            if let conversationID {
                WorkspaceCheckpointControls(directory: directory, conversationID: conversationID,
                    scope: $scope, isolation: isolation, rewindConversation: rewindConversation,
                    didChange: { refreshID = UUID() })
                    .padding(.horizontal, 10).padding(.bottom, 6)
            }
            Divider()
            if state.directory != directory {
                ProgressView("Reading workspace changes…").frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                if let error = state.error {
                    errorBanner(error, retained: state.snapshot != nil) { refreshID = UUID() }
                }
                if let snapshot = state.snapshot {
                    if snapshot.files.isEmpty {
                        ContentUnavailableView(scope == .current ? "No uncommitted changes" : "No changes in this comparison", systemImage: "checkmark.circle",
                            description: Text(scope == .current ? "This Git working tree has no staged, unstaged, or untracked files." : "The selected revisions have no changed paths."))
                    } else {
                        HSplitView {
                            List(snapshot.files, selection: Binding(get: { state.selectedPath }, set: { state.select($0) })) { file in
                                VStack(alignment: .leading, spacing: 3) {
                                    Text(file.label).lineLimit(2).truncationMode(.middle)
                                    Text(scope == .current ? file.status : file.isUntracked ? "Untracked" : String(file.worktreeStatus))
                                        .font(.caption).foregroundStyle(.secondary)
                                }.tag(file.id).help(file.label)
                            }.listStyle(.sidebar).frame(minWidth: 150, idealWidth: 230, maxWidth: 340)
                            VStack(spacing: 0) {
                                if let error = state.patchError {
                                    errorBanner(error, retained: state.patch != nil) { state.retryPatch() }
                                }
                                if scope == .current, let path = state.selectedPath {
                                    HStack {
                                        Button("Stage file") { Task { await changeIndex(path: path, stage: true) } }
                                        Button("Unstage file") { Task { await changeIndex(path: path, stage: false) } }
                                        Spacer()
                                    }.font(.caption).padding(6).disabled(isWorking || indexBusy || state.loading || state.loadingPatch)
                                }
                                Group {
                                    if splitDiff { WorkspaceSplitDiffView(patch: state.patch ?? "") }
                                    else { WorkspaceDiffText(text: state.patch, identity: directory.path + "/" + (state.selectedPath ?? "")) }
                                }
                                    .overlay {
                                        if state.patch == nil {
                                            if state.loadingPatch { ProgressView("Reading diff…") }
                                            else {
                                                Text(state.patchError != nil ? "Could not read this diff." : "Select a file to see its changes.")
                                                    .foregroundStyle(.secondary)
                                            }
                                        }
                                    }
                                    .overlay(alignment: .topTrailing) {
                                        if state.loadingPatch && state.patch != nil {
                                            ProgressView().controlSize(.small).padding(8).help("Refreshing diff")
                                        }
                                    }
                            }.frame(minWidth: 250, maxWidth: .infinity, maxHeight: .infinity)
                        }
                    }
                } else if state.loading {
                    ProgressView("Reading workspace changes…").frame(maxWidth: .infinity, maxHeight: .infinity)
                } else if state.error != nil {
                    ContentUnavailableView("Could not read changes", systemImage: "exclamationmark.triangle",
                        description: Text("Try refreshing the workspace changes."))
                } else {
                    ContentUnavailableView("Not a Git working tree", systemImage: "folder",
                        description: Text("The selected workspace is not inside a Git repository."))
                }
            }
            Divider()
            if let addReviewContext, let path = state.selectedPath, let patch = state.patch {
                HStack {
                    TextField("Review comment", text: $reviewComment)
                    Button("Add review to chat") {
                        let context = WorkspaceReviewContext(directory: state.snapshot?.root ?? directory,
                            path: path, scope: scope.label, patch: patch, comment: reviewComment)
                        if addReviewContext(context) { reviewComment = ""; actionError = nil }
                        else { actionError = "The review context could not be added to this chat." }
                    }
                }.padding(8).disabled(state.loading || state.loadingPatch)
            }
            if let actionError { Text(actionError).font(.caption).foregroundStyle(.orange).padding(8) }
            Text(scope == .current ? "Current working tree · Staged, unstaged, and untracked files" : scope.label)
                .font(.caption).foregroundStyle(.secondary).padding(8)
        }
        .task(id: RefreshTask(directory: directory, revision: refreshID, isWorking: isWorking, scope: scope)) { await refresh() }
        .task(id: PatchTask(directory: directory, path: state.selectedPath, revision: state.patchRevision)) { await loadPatch() }
        .onChange(of: reviewRequest) { _, _ in
            if let path = initialSelectedPath {
                state.select(path)
                refreshID = UUID()
            }
        }
        .sheet(isPresented: $showingPullRequest) {
            WorkspacePullRequestView(directory: directory, initialURL: pullRequestURL, addReviewContext: addReviewContext)
        }
        .sheet(isPresented: $showingSetup) {
            if let setupCommand { WorkspaceSetupView(directory: directory, script: setupCommand) }
        }
    }

    private func errorBanner(_ message: String, retained: Bool, retry: @escaping () -> Void) -> some View {
        HStack(alignment: .top) {
            Image(systemName: "exclamationmark.triangle").foregroundStyle(.orange)
            Text((retained ? "Showing the last successful read. " : "") + message)
                .font(.caption).textSelection(.enabled)
            Spacer()
            Button("Retry", action: retry).controlSize(.small)
        }.padding(10).background(.quaternary)
    }

    @MainActor private func refresh() async {
        if displayedScope != scope {
            state = WorkspaceChangesState()
            displayedScope = scope
        }
        let request = state.beginRefresh(directory: directory, initialSelectedPath: initialSelectedPath)
        do {
            let next = try await client.snapshot(directory: directory, scope: scope)
            guard !Task.isCancelled else { return }
            state.finishRefresh(next, request: request)
        } catch {
            guard !Task.isCancelled else { return }
            state.failRefresh(error.localizedDescription, request: request)
        }
    }

    @MainActor private func loadPatch() async {
        guard state.directory == directory, let read = state.beginPatch() else { return }
        do {
            let next = try await client.diff(file: read.file, root: read.root, scope: scope)
            guard !Task.isCancelled else { return }
            state.finishPatch(next, request: read.request)
        } catch {
            guard !Task.isCancelled else { return }
            state.failPatch(error.localizedDescription, request: read.request)
        }
    }

    @MainActor private func changeIndex(path: String, stage: Bool) async {
        indexBusy = true
        defer { indexBusy = false }
        do {
            let original = state.snapshot?.files.first { $0.path == path }?.originalPath
            let paths = stage ? [path] : [original, path].compactMap { $0 }
            if stage { try await WorkspaceGitClient().stage(directory: directory, paths: paths) }
            else { try await WorkspaceGitClient().unstage(directory: directory, paths: paths) }
            actionError = nil
            refreshID = UUID()
        } catch { actionError = error.localizedDescription }
    }
}

private struct WorkspaceSplitDiffView: View {
    let patch: String
    var body: some View {
        ScrollView([.horizontal, .vertical]) {
            LazyVStack(alignment: .leading, spacing: 0) {
                ForEach(WorkspaceSplitDiffRow.parse(patch)) { row in
                    HStack(alignment: .top, spacing: 0) {
                        Text(row.left.isEmpty ? " " : row.left).frame(minWidth: 350, maxWidth: .infinity, alignment: .leading)
                            .background(row.kind == .change && !row.left.isEmpty ? Color.red.opacity(0.1) : .clear)
                        Divider()
                        Text(row.right.isEmpty ? " " : row.right).frame(minWidth: 350, maxWidth: .infinity, alignment: .leading)
                            .background(row.kind == .change && !row.right.isEmpty ? Color.green.opacity(0.1) : .clear)
                    }.font(.system(size: 12, design: .monospaced)).textSelection(.enabled)
                }
            }.padding(8)
        }
    }
}

struct WorkspaceDiffText: NSViewRepresentable {
    let text: String?
    let identity: String

    func makeCoordinator() -> WorkspaceDiffViewport { WorkspaceDiffViewport() }
    func makeNSView(context: Context) -> NSScrollView { Self.makeScrollView() }
    func updateNSView(_ scroll: NSScrollView, context: Context) {
        context.coordinator.update(scroll, text: text, identity: identity)
    }

    static func makeScrollView() -> NSScrollView {
        let scroll = NSScrollView()
        scroll.hasVerticalScroller = true
        scroll.hasHorizontalScroller = true
        scroll.autohidesScrollers = true
        let view = RichContentView.textView()
        view.textContainerInset = NSSize(width: 12, height: 12)
        view.isHorizontallyResizable = true
        view.isVerticallyResizable = true
        view.minSize = .zero
        view.maxSize = NSSize(width: 1_000_000, height: CGFloat.greatestFiniteMagnitude)
        view.textContainer?.containerSize = view.maxSize
        view.textContainer?.heightTracksTextView = false
        scroll.documentView = view
        return scroll
    }
}

/// Lives with the native view, so reading position and text selection survive both
/// changed patches and file switches without publishing scroll events into SwiftUI.
@MainActor final class WorkspaceDiffViewport {
    private struct Position {
        let origin: NSPoint
        let selection: NSRange
    }
    private var positions: [String: Position] = [:]
    private var identity: String?
    private var hasText = false

    func update(_ scroll: NSScrollView, text: String?, identity nextIdentity: String) {
        guard let view = scroll.documentView as? NSTextView else { return }
        if let identity, hasText {
            positions[identity] = Position(origin: scroll.contentView.bounds.origin, selection: view.selectedRange())
        }
        let changedFile = identity != nextIdentity
        identity = nextIdentity
        let position = positions[nextIdentity]
        let next = text ?? ""
        let changedText = view.string != next
        hasText = text != nil
        guard changedFile || changedText else { return }
        if changedText {
            view.textStorage?.setAttributedString(CodeSyntax.render(next, language: "diff", font: .systemFont(ofSize: 13)))
        }
        if let container = view.textContainer, let manager = view.layoutManager {
            manager.ensureLayout(for: container)
            let size = manager.usedRect(for: container).size
            view.setFrameSize(NSSize(width: max(scroll.contentSize.width, ceil(size.width) + 24),
                                     height: max(scroll.contentSize.height, ceil(size.height) + 24)))
        }
        let length = (next as NSString).length
        let selection = position?.selection ?? NSRange(location: 0, length: 0)
        let start = min(selection.location, length)
        view.setSelectedRange(NSRange(location: start, length: min(selection.length, length - start)))
        let origin = position?.origin ?? .zero
        scroll.contentView.scroll(to: NSPoint(
            x: min(max(0, origin.x), max(0, view.frame.width - scroll.contentSize.width)),
            y: min(max(0, origin.y), max(0, view.frame.height - scroll.contentSize.height))))
        scroll.reflectScrolledClipView(scroll.contentView)
    }
}
