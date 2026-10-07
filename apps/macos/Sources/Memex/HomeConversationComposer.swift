import AppKit
import SwiftUI

#if canImport(SQACPUI)
import SQACPUI

struct HomeConversationComposer: View {
    @Bindable var store: Store
    @State private var repository: ConversationWorkspaceRepository?
    @State private var inspecting = false
    @State private var workspaceError: String?
    @State private var providers = ConversationProviderCatalog()
    @State private var providerError: String?
    @State private var showingProviderSetup = false
    @State private var dropTargeted = false
    @State private var showingDictation = false

    private var baseRefs: [String] {
        Array(Set((repository?.localBranches ?? []) + [repository?.defaultBaseRef, store.newConversationDraft.value.baseRef].compactMap { $0 })).sorted()
    }

    private var canPrepareDraft: Bool {
        store.canPrepareConversation && !inspecting
            && (store.newConversationDraft.value.workspaceMode == .existingDirectory
                || (repository != nil && store.newConversationDraft.value.baseRef != nil))
    }

    var body: some View {
        @Bindable var draft = store.newConversationDraft
        VStack(alignment: .leading, spacing: 12) {
            Text("What would you like to work on?").font(.title2.weight(.semibold))
            if draft.value.createdSessionID != nil {
                VStack(alignment: .leading, spacing: 8) {
                    Text("Your conversation was created. Open it to review the saved prompt before sending.")
                    Button("Open created conversation") { Task { await store.openCreatedConversationFromHome() } }
                }
                .font(.callout).padding(12).frame(maxWidth: .infinity, alignment: .leading)
                .background(.quaternary, in: RoundedRectangle(cornerRadius: 10))
            }
            AcpComposerView(
                text: $draft.value.text,
                placeholder: "Ask the agent…",
                isRunning: false,
                canSend: store.canStartConversation && !inspecting
                    && (draft.value.workspaceMode == .existingDirectory || (repository != nil && draft.value.baseRef != nil)),
                focusRequestID: draft.focusRequest,
                composerFont: .system(size: 14),
                onSubmit: { Task { await store.startConversationFromHome() } },
                onCancel: {},
                leadingAccessory: { controls },
                sendButton: { AcpSendButton().accessibilityLabel("Start conversation") },
                cancelButton: { AcpStopButton() }
            )
            .disabled(store.startingConversation || draft.value.createdSessionID != nil)
            .onPasteCommand(of: ConversationClipboard.supportedTypes) { providers in
                Task { await captureClipboard(providers) }
            }
            .onDrop(of: ConversationClipboard.supportedTypes, isTargeted: $dropTargeted) { providers in
                guard store.canPrepareConversation, !inspecting else { return false }
                Task { await captureClipboard(providers) }
                return true
            }
            .overlay(RoundedRectangle(cornerRadius: 10).stroke(dropTargeted ? Color.accentColor : .clear, lineWidth: 2))
            if store.startingConversation {
                HStack(spacing: 8) {
                    ProgressView().controlSize(.small)
                    Text(draft.value.workspaceMode == .newWorktree ? "Preparing worktree and conversation…" : "Creating conversation…")
                }.font(.caption).foregroundStyle(.secondary)
            }
            if let workspace = draft.value.preparedWorkspace, workspace.state == .ready {
                Text("Prepared folder: \(workspace.workingDirectory.path)")
                    .font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
            }
            if let workspace = draft.value.preparedWorkspace, workspace.state == .failed {
                VStack(alignment: .leading, spacing: 6) {
                    Text("Folder preparation failed. Its files were retained at \(workspace.worktreeRoot?.path ?? workspace.workingDirectory.path).")
                        .textSelection(.enabled)
                    Button("Prepare another folder") {
                        draft.value.preparedWorkspace = nil
                        store.newConversationError = nil
                    }
                }.font(.caption).foregroundStyle(.secondary)
            }
            if let error = store.newConversationError ?? draft.error ?? providerError ?? (draft.value.projectID == nil ? nil : store.localProjects.error) ?? workspaceError {
                Text(error).font(.callout).foregroundStyle(.orange).textSelection(.enabled)
            }
            if draft.error != nil {
                Button("Retry saving draft") { Task { await draft.retrySave() } }
                    .font(.caption)
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .task(id: store.newConversationProject) { await inspectProject() }
        .task { reloadProviders() }
        .sheet(isPresented: $showingDictation) {
            ConversationDictationView { text in
                draft.value.text = [draft.value.text, text].filter { !$0.isEmpty }.joined(separator: "\n\n")
            }
        }
        .sheet(isPresented: $showingProviderSetup, onDismiss: reloadProviders) { ConversationProviderSetupView() }
    }

    @MainActor private func attachToNewDraft() async {
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = true
        panel.prompt = "Attach to new draft"
        guard await panel.begin() == .OK else { return }
        await store.startConversationFromHome(sendImmediately: false, attachments: panel.urls)
    }

    @MainActor private func captureClipboard(_ providers: [NSItemProvider]) async {
        guard store.canPrepareConversation, !inspecting else { return }
        var prepared: LiveConversation?
        await store.startConversationFromHome(sendImmediately: false, onPrepared: { prepared = $0 })
        guard let conversation = prepared,
              let controls = conversation.snapshot.controls else { return }
        do {
            let items = try await ConversationClipboard.capture(providers, controls: controls)
            guard !items.isEmpty, conversation.appendCapturedContext(items) else {
                throw ConversationRuntimeError(message: "These clipboard attachments could not be added to the new draft.")
            }
            conversation.reportAttachmentError(nil)
        } catch { conversation.reportAttachmentError(error.localizedDescription) }
    }

    private var controls: some View {
        @Bindable var draft = store.newConversationDraft
        return HStack(spacing: 12) {
            Menu {
                Button("Attach files…") { Task { await attachToNewDraft() } }
                Button("Dictate a prompt…") { showingDictation = true }
            } label: {
                Image(systemName: "plus").font(.system(size: 14))
                    .frame(width: 28, height: 28).contentShape(Rectangle())
            }
            .buttonStyle(.plain).help("Attach files").accessibilityLabel("Attach files")
            .disabled(!canPrepareDraft)

            Menu {
                ForEach(providers.creatableProviders) { provider in
                    Button { draft.value.provider = provider.id } label: {
                        if draft.value.provider == provider.id { Label(provider.name, systemImage: "checkmark") }
                        else { Text(provider.name) }
                    }
                }
                Divider()
                Button("Model and permissions…") {
                    Task { await store.startConversationFromHome(sendImmediately: false) }
                }
                .disabled(!canPrepareDraft)
                Button("Configure providers…") { showingProviderSetup = true }
            } label: {
                Text(providers.providers.first(where: { $0.id == draft.value.provider })?.name ?? draft.value.provider)
            }
            .help("Choose a provider or configure the model and permissions before sending.")
            Menu {
                Button {
                    draft.selectNoProject()
                    store.newConversationError = nil
                } label: {
                    if draft.value.projectID == nil { Label("Don't work in a project", systemImage: "checkmark") }
                    else { Text("Don't work in a project") }
                }
                Divider()
                ForEach(store.localProjects.projects) { project in
                    Button {
                        draft.selectProject(project)
                        store.newConversationError = nil
                    } label: {
                        if draft.value.projectID == project.id { Label(project.name, systemImage: "checkmark") }
                        else { Text(project.name) }
                    }
                }
                if !store.localProjects.projects.isEmpty { Divider() }
                Button("Add New Project") { store.addNewProject() }
            } label: {
                Label(store.newConversationProject?.name ?? (draft.value.projectID == nil ? "No project" : "Missing project"), systemImage: "folder")
                    .lineLimit(1)
            }
            .help(store.newConversationProject?.directoryPath ?? "This chat gets its own folder, kept for later use.")
            if store.newConversationProject != nil {
                Menu {
                    Button("Existing folder") { draft.selectWorkspace(.existingDirectory, baseRef: draft.value.baseRef) }
                    Button("New worktree") {
                        draft.selectWorkspace(.newWorktree, baseRef: draft.value.baseRef ?? repository?.defaultBaseRef)
                    }.disabled(repository == nil || inspecting)
                } label: {
                    Label(draft.value.workspaceMode == .newWorktree ? "New worktree" : "Existing folder",
                          systemImage: draft.value.workspaceMode == .newWorktree ? "arrow.triangle.branch" : "folder")
                }
                .help(repository == nil ? "A Git repository is required to create a worktree." : "Choose where this conversation will work.")
            }
            if store.newConversationProject != nil && draft.value.workspaceMode == .newWorktree {
                Menu {
                    ForEach(baseRefs, id: \.self) { ref in
                        Button(ref) { draft.selectWorkspace(.newWorktree, baseRef: ref) }
                    }
                } label: {
                    Text(draft.value.baseRef ?? "Choose base branch").lineLimit(1)
                }
                .help("The worktree starts from this local Git ref. Uncommitted changes stay in the original folder.")
            }
        }
        .menuStyle(.borderlessButton).fixedSize(horizontal: false, vertical: true)
        .font(.system(size: 12)).foregroundStyle(.secondary)
    }

    private func reloadProviders() {
        do {
            providers = try ConversationProviderCatalog.load()
            providerError = nil
        } catch { providerError = "Provider configuration could not be read: \(error.localizedDescription)" }
    }

    @MainActor private func inspectProject() async {
        repository = nil
        workspaceError = nil
        guard let project = store.newConversationProject else { inspecting = false; return }
        inspecting = true
        defer { if store.newConversationProject == project { inspecting = false } }
        do {
            let repository = try await store.workspaceClient.inspect(directory: project.directory)
            guard !Task.isCancelled, store.newConversationProject == project else { return }
            self.repository = repository
            if store.newConversationDraft.value.baseRef == nil, let defaultRef = repository?.defaultBaseRef {
                store.newConversationDraft.value.baseRef = defaultRef
            }
        } catch is CancellationError {} catch {
            if store.newConversationProject == project { workspaceError = error.localizedDescription }
        }
    }
}
#else
struct HomeConversationComposer: View {
    let store: Store
    var body: some View { EmptyView() }
}
#endif
