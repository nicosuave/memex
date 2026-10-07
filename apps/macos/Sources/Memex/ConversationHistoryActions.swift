import SwiftUI
import Observation

@Observable @MainActor final class ConversationHistoryPresentation {
    var showingBranch = false
    var operation: ConversationHistoryMutation.Operation?
    var selectedBoundary: String?
    var confirmingMutation = false

    func begin(_ operation: ConversationHistoryMutation.Operation, boundaries: [ConversationHistoryBoundary]) {
        selectedBoundary = boundaries.last?.id
        self.operation = operation
    }
}

struct ConversationHistoryActions: View {
    @Bindable var store: Store
    let session: Session
    @Bindable var presentation: ConversationHistoryPresentation = .init()
    var showsStatus = true
    @State private var inspectedOperation: ConversationRelationships.Pending?

    private var pending: [ConversationRelationships.Pending] {
        store.conversationRelationships.pending.filter { $0.source.id == session.id }
    }
    private var boundaries: [ConversationHistoryBoundary] { store.selectedHistoryBoundaries }

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            if showsStatus {
                if store.historyActionInProgress { ProgressView().controlSize(.small) }
                ForEach(pending) { request in
                    HStack(alignment: .top) {
                        Text("A previous \(request.operation) request needs inspection. It will not be retried automatically.")
                        if let result = request.result {
                            Button("Open result") { store.openRelatedConversation(result) }
                        }
                        Button("Mark inspected…") { inspectedOperation = request }
                    }.font(.caption).foregroundStyle(.orange)
                }
                if let error = store.historyActionError ?? store.conversationRelationships.error {
                    Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled)
                }
                if let warning = store.workspaceCheckpointWarning {
                    Text(warning).font(.caption).foregroundStyle(.secondary).textSelection(.enabled)
                }
            }
        }
        .sheet(isPresented: $presentation.showingBranch) { ConversationContextBranchSheet(store: store, source: session) }
        .sheet(isPresented: Binding(get: { presentation.operation != nil }, set: { if !$0 { presentation.operation = nil } })) {
            mutationSheet
        }
        .confirmationDialog("Mark the previous history request as inspected?", isPresented: Binding(
            get: { inspectedOperation != nil }, set: { if !$0 { inspectedOperation = nil } }), titleVisibility: .visible) {
            Button("Mark inspected") {
                guard let request = inspectedOperation else { return }
                do { try store.conversationRelationships.acknowledge(request); store.historyActionError = nil }
                catch { store.historyActionError = error.localizedDescription }
                inspectedOperation = nil
            }
            Button("Cancel", role: .cancel) { inspectedOperation = nil }
        } message: {
            Text("Inspect native history and any resulting conversation first. Clearing this recovery marker does not undo provider changes or send any message.")
        }
    }

    private var mutationSheet: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text(presentation.operation == .fork ? "Fork native history" : "Rewind conversation").font(.title2)
            Text(presentation.operation == .fork
                 ? "Create a native branch through the selected turn. The source conversation and workspace files remain unchanged."
                 : "Remove the selected turn and later turns from the resumed conversation. Claude preserves the original as a separate branch. Workspace files are unchanged; use Checkpoints to restore an owned worktree separately.")
                .font(.callout).foregroundStyle(.secondary)
            Picker("Turn", selection: $presentation.selectedBoundary) {
                ForEach(boundaries) { boundary in Text(boundary.title).tag(Optional(boundary.id)) }
            }
            HStack {
                Spacer()
                Button("Cancel") { presentation.operation = nil }.keyboardShortcut(.cancelAction)
                Button(presentation.operation == .fork ? "Fork" : "Rewind", role: presentation.operation == .revert ? .destructive : nil) {
                    presentation.confirmingMutation = true
                }.disabled(presentation.selectedBoundary == nil || store.historyActionInProgress)
            }
        }.padding(24).frame(width: 540)
            .confirmationDialog("Apply this native history change?", isPresented: $presentation.confirmingMutation, titleVisibility: .visible) {
                Button(presentation.operation == .fork ? "Fork" : "Rewind", role: presentation.operation == .revert ? .destructive : nil) {
                    guard let operation = presentation.operation, let boundary = boundaries.first(where: { $0.id == presentation.selectedBoundary }) else { return }
                    presentation.operation = nil
                    perform { _ = try await store.mutateConversation(source: session, operation: operation, boundary: boundary) }
                }
                Button("Cancel", role: .cancel) {}
            }
    }

    private func perform(_ action: @escaping @MainActor () async throws -> Void) {
        store.historyActionError = nil
        Task { do { try await action() } catch { store.historyActionError = error.localizedDescription } }
    }
}

struct ConversationContextBranchSheet: View {
    @Bindable var store: Store
    let source: Session
    var plan: ConversationWork.Plan? = nil
    @Environment(\.dismiss) private var dismiss
    @State private var provider = "codex"
    @State private var isolatedWorkspace = false
    @State private var providers: [ConversationProviderDescriptor] = ConversationProviderCatalog.builtins
    @State private var error: String?

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Branch with context").font(.title2)
            Text("Prepare a new conversation on this Mac with a captured copy of this conversation as an attachment. Review its model, permissions and draft before sending.")
                .foregroundStyle(.secondary)
            if let plan {
                Text("The selected plan and implementation instructions will be added to the new draft.")
                    .font(.callout).foregroundStyle(.secondary)
                ScrollView { Text(plan.text).frame(maxWidth: .infinity, alignment: .leading).textSelection(.enabled) }
                    .frame(maxHeight: 160)
            }
            Picker("Provider", selection: $provider) {
                ForEach(providers) { Text($0.name).tag($0.id) }
            }
            if store.canAccessLocalFiles(for: source), source.cwd != nil {
                Toggle("Create an isolated worktree from the repository’s default branch", isOn: $isolatedWorkspace)
                Text(isolatedWorkspace ? "The new worktree starts from the configured default branch. Uncommitted files are not copied."
                     : "The branch shares the current working folder. Both conversations can edit the same files.")
                    .font(.caption).foregroundStyle(.secondary)
            } else { Text("The new conversation receives its own local folder.").font(.caption).foregroundStyle(.secondary) }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled) }
            HStack {
                Spacer()
                Button("Cancel") { dismiss() }.keyboardShortcut(.cancelAction)
                Button("Prepare branch") {
                    Task {
                        do {
                            _ = try await store.branchWithContext(source: source, provider: provider,
                                isolatedWorkspace: isolatedWorkspace, plan: plan)
                            dismiss()
                        } catch { self.error = error.localizedDescription }
                    }
                }.disabled(store.historyActionInProgress).keyboardShortcut(.defaultAction)
            }
        }.padding(24).frame(width: 540)
            .onAppear {
                do {
                    providers = try ConversationProviderCatalog.load().creatableProviders
                    if providers.contains(where: { $0.id == source.source }) { provider = source.source }
                } catch { self.error = error.localizedDescription }
            }
    }
}
