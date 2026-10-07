import AppKit
import SwiftUI

struct ReaderView: View {
    @Bindable var store: Store
    @State private var navigation = TranscriptNavigationState()
    @State private var outline = ConversationOutline()
    @State private var requestedRecordID: String?
    @State private var requestGeneration = 0
    @State private var outlineRevealID = UUID()
    @State private var find: ConversationFindState?
    @State private var rawTranscript = false
    @State private var footerHeight: CGFloat = 0
    @State private var planBranch: PlanBranch?
    @State private var selectionReveal: TranscriptSelectionReveal?
    @State private var selectionLoadID = UUID()
    @FocusState private var findFocused: Bool

    var body: some View {
        Group {
            if let session = store.selected {
                VStack(spacing: 0) {
                    header(session)
                    if let error = store.createdConversations.error {
                        HStack {
                            Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled)
                            Button("Retry saving") { store.createdConversations.retrySave() }
                        }.padding(8)
                    }
                    ConversationRecoveryView(session: session, conversation: store.selectedLiveConversation)
                    if let live = store.selectedLiveConversation, live.error == nil, let warning = live.snapshot.warning {
                        Text(warning).font(.caption).foregroundStyle(.secondary).padding(8)
                    }
                    if let find, find.isOpen { findBar(find) }
                    ConversationWorkView(state: ConversationWork.project(currentRecords),
                                         conversation: store.selectedLiveConversation, session: session,
                                         sessions: contextSessions, navigate: store.openConversation,
                                         branchPlan: { planBranch = PlanBranch(source: session, plan: $0) })
                    ZStack(alignment: .bottom) {
                        transcript(session)
                            .contextMenu { Toggle("Raw transcript", isOn: $rawTranscript) }
                            .overlay(alignment: .topLeading) {
                                GeometryReader { geometry in
                                    ConversationOutlineRail(outline: outline,
                                        previewWidth: min(360, max(80, geometry.size.width - 64)),
                                        reveal: revealPrompt)
                                        .frame(height: max(0, geometry.size.height - footerHeight - 16))
                                        .padding(.leading, 4).padding(.top, 8)
                                }
                            }
                        footer(session)
                            .fixedSize(horizontal: false, vertical: true)
                            .onGeometryChange(for: CGFloat.self) { $0.size.height } action: { footerHeight = $0 }
                    }
                }
            } else {
                ContentUnavailableView("Your conversations, together", systemImage: "bubble.left.and.bubble.right",
                    description: Text("Select a conversation or search your history."))
            }
        }
        .onAppear { if find == nil { find = ConversationFindState(client: store.client) } }
        .sheet(item: $planBranch) { branch in
            ConversationContextBranchSheet(store: store, source: branch.source, plan: branch.plan)
        }
        .onChange(of: store.selected, initial: true) { _, session in
            if let session { store.liveConversations.prepare(session) }
        }
        .onChange(of: store.selected?.id) { _, _ in
            selectionLoadID = UUID()
            selectionReveal = nil
            outlineRevealID = UUID()
            requestedRecordID = nil
        }
        .task(id: store.readerTranscriptKey) {
            if store.readerUsesLiveSnapshot { outline.load(records: currentRecords) }
            else { await outline.load(session: store.selected, client: store.client) }
            outline.follow(navigation.visibleRecordID)
        }
        .onChange(of: navigation.visibleRecordID) { _, id in outline.follow(id) }
        .task(id: store.selectedLiveConversation?.session.id) {
            guard let live = store.selectedLiveConversation else { return }
            while !Task.isCancelled {
                live.refreshOwnership()
                do { try await Task.sleep(for: .seconds(2)) } catch { return }
            }
        }
        .onChange(of: store.findConversationRequest) { _, _ in
            find?.isOpen = true
            findFocused = true
        }
        .onChange(of: store.readerTranscriptKey) { _, _ in
            search()
        }
        .onChange(of: find?.query) { _, _ in search() }
        .onChange(of: store.selectedLiveConversation?.revision) { _, _ in
            if store.readerUsesLiveSnapshot {
                search()
                outline.load(records: currentRecords)
                outline.follow(navigation.visibleRecordID)
            }
            store.updateCreatedConversationTitle()
        }
        .task(id: find?.generation) {
            guard let hit = find?.selectedHit else { return }
            if let live = store.selectedLiveConversation, store.readerUsesLiveSnapshot { live.revealRecord(hit.recordID) }
            else { await store.revealRecord(hit.recordID, offset: hit.recordOffset) }
        }
        .onDisappear { find?.reset() }
    }

    private struct PlanBranch: Identifiable {
        let id = UUID()
        let source: Session
        let plan: ConversationWork.Plan
    }

    private func transcript(_ session: Session) -> some View {
        VStack(spacing: 0) {
            if let live = store.selectedLiveConversation, store.readerUsesLiveSnapshot {
                NativeTranscript(sessionID: session.id + ":live", records: live.visibleRecords,
                                 provider: session.source, hasMore: false, isLoading: false, onLoadMore: {},
                                 hasEarlier: live.hasEarlierRecords, startsAtEnd: true, navigation: navigation,
                                 onLoadEarlier: { live.loadEarlierRecords() },
                                 findQuery: find?.isOpen == true ? find?.query ?? "" : "",
                                 findHit: find?.selectedHit, findGeneration: find?.generation ?? 0,
                                 rawTranscript: rawTranscript, isLocalHost: store.canAccessLocalFiles(for: session),
                                 mcpAppTransport: live.snapshot.connected ? live.snapshot.mcpAppConnection?.transport : nil, sourcePath: session.sourcePath,
                                 requestedRecordID: requestedRecordID, requestGeneration: requestGeneration,
                                 followLatest: true, bottomInset: footerHeight,
                                 onAddSelection: selectionHandler(for: session),
                                 selectionReveal: selectionReveal, onSelectionRevealResult: selectionRevealResult)
            } else {
                NativeTranscript(sessionID: store.readerPositionKey,
                                 records: store.loadedReaderKey == store.readerPositionKey ? store.records : [],
                                 provider: session.source, hasMore: store.hasMoreRecords,
                                 isLoading: store.loadingRecords,
                                 onLoadMore: { Task { await store.loadMoreRecords() } },
                                 hasEarlier: store.hasEarlierRecords, startsAtEnd: store.readerStartsAtEnd,
                                 anchorID: store.readerAnchorID, navigation: navigation,
                                 onLoadEarlier: { Task { await store.loadEarlierRecords() } },
                                 findQuery: find?.isOpen == true ? find?.query ?? "" : "",
                                 findHit: find?.selectedHit, findGeneration: find?.generation ?? 0,
                                 rawTranscript: rawTranscript, isLocalHost: store.canAccessLocalFiles(for: session),
                                 sourcePath: session.sourcePath, requestedRecordID: requestedRecordID,
                                 requestGeneration: requestGeneration, bottomInset: footerHeight,
                                 onAddSelection: selectionHandler(for: session),
                                 selectionReveal: selectionReveal, onSelectionRevealResult: selectionRevealResult)
                if let error = store.readerError {
                    ErrorBanner(message: error) {
                        Task { await store.retryRecords() }
                    }
                }
                if store.loadingRecords {
                    ProgressView("Loading conversation…").controlSize(.small).padding(12)
                } else if store.records.isEmpty && store.readerError == nil {
                    Text("No messages in this transcript.").foregroundStyle(.secondary).padding(12)
                }
            }
        }
    }

    private func footer(_ session: Session) -> some View {
        VStack(spacing: 0) {
            if let live = store.selectedLiveConversation {
                ConversationPendingView(conversation: live)
            }
            if store.selectedLiveConversation == nil, let directory = store.selectedWorkspace {
                WorkspaceChangeSummary(directory: directory,
                                       isWorking: store.selectedLiveConversation?.isWorking == true,
                                       review: store.reviewWorkspaceChange)
            }
            if let live = store.selectedLiveConversation {
                ConversationComposer(conversation: live, contextSessions: contextSessions, onRevealSelection: revealSelection,
                    workspaceSummary: store.selectedWorkspace.map { directory in
                        AnyView(WorkspaceChangeSummary(directory: directory, isWorking: live.isWorking,
                            review: store.reviewWorkspaceChange, horizontalInset: 0))
                    })
            } else if !InAppResumeTarget.isArchived(session) {
                Label(InAppResumeTarget.unavailableReason(for: session)
                      ?? "This build supports continuing conversations through Open in.", systemImage: "info.circle")
                    .font(.caption).foregroundStyle(.secondary)
                    .padding(8).background(.regularMaterial, in: RoundedRectangle(cornerRadius: 8))
                    .frame(maxWidth: ConversationReadingLane.maximumWidth, alignment: .leading)
                    .padding(.horizontal, ConversationReadingLane.minimumMargin).padding(.vertical, 12)
                    .frame(maxWidth: .infinity)
            }
        }
    }

    private func search() {
        if let live = store.selectedLiveConversation, store.readerUsesLiveSnapshot { find?.search(records: live.snapshot.records) }
        else { find?.search(in: store.selected) }
    }

    private func revealPrompt(_ prompt: ConversationPrompt) {
        guard let session = store.selected else { return }
        let requestID = UUID()
        outlineRevealID = requestID
        Task { @MainActor in
            if let live = store.selectedLiveConversation, store.readerUsesLiveSnapshot {
                live.revealRecord(prompt.id)
            } else {
                await store.revealRecord(prompt.id, offset: prompt.offset)
            }
            guard outlineRevealID == requestID, store.selected?.id == session.id else { return }
            requestedRecordID = prompt.id
            requestGeneration += 1
            outline.selectedID = prompt.id
        }
    }

    private var currentRecords: [TranscriptRecord] {
        if store.readerUsesLiveSnapshot, let live = store.selectedLiveConversation { return live.snapshot.records }
        return store.loadedReaderKey == store.readerPositionKey ? store.records : []
    }

    private var contextSessions: [Session] {
        var known = Set<String>()
        return (store.sessions + store.createdConversations.sessions).filter { known.insert($0.id).inserted }
    }

    private func selectionHandler(for session: Session) -> ((TranscriptSelection) -> String?)? {
        guard let live = store.selectedLiveConversation else { return nil }
        return { selection in
            guard store.selected?.id == session.id, store.selectedLiveConversation === live else {
                return "The conversation changed. Select the text again."
            }
            return live.appendTranscriptSelection(selection)
        }
    }

    private func revealSelection(_ selection: TranscriptSelection) {
        guard let session = store.selected, selection.sessionID == session.id,
              let recordID = selection.sourceIDs.last else { return }
        let loadID = UUID()
        selectionLoadID = loadID
        Task { @MainActor in
            guard selectionLoadID == loadID, store.selected?.id == session.id else { return }
            if let live = store.selectedLiveConversation, store.readerUsesLiveSnapshot {
                let id = live.snapshot.records.first(where: { $0.sourceID == recordID })?.id ?? recordID
                live.revealRecord(id)
            } else {
                await store.revealRecord(recordID)
            }
            guard selectionLoadID == loadID, store.selected?.id == session.id else { return }
            selectionReveal = TranscriptSelectionReveal(selection: selection, transcriptKey: store.readerTranscriptKey)
        }
    }

    private func selectionRevealResult(_ id: UUID, _ error: String?) {
        guard selectionReveal?.id == id, selectionReveal?.selection.sessionID == store.selected?.id else { return }
        store.selectedLiveConversation?.reportAttachmentError(error)
    }

    private func findBar(_ state: ConversationFindState) -> some View {
        @Bindable var state = state
        return HStack(spacing: 8) {
            TextField("Find in conversation", text: $state.query)
                .textFieldStyle(.roundedBorder).focused($findFocused)
                .onAppear { findFocused = true }
                .onSubmit { state.move(NSEvent.modifierFlags.contains(.shift) ? -1 : 1) }
                .onExitCommand { state.close() }
                .accessibilityLabel("Find in conversation")
                .frame(minWidth: 100).layoutPriority(1)
            if state.isScanning { ProgressView().controlSize(.small) }
            Text(state.status).font(.caption).foregroundStyle(.secondary).lineLimit(1)
                .frame(maxWidth: 90).help(state.statusDetail)
                .accessibilityLabel(state.statusDetail)
            Button { state.move(-1) } label: { Image(systemName: "chevron.up") }
                .keyboardShortcut("g", modifiers: [.command, .shift])
                .help("Previous match (⇧⌘G)").disabled(state.hits.isEmpty)
            Button { state.move(1) } label: { Image(systemName: "chevron.down") }
                .keyboardShortcut("g", modifiers: .command)
                .help("Next match (⌘G)").disabled(state.hits.isEmpty)
            Button { state.close() } label: { Image(systemName: "xmark") }
                .help("Close find").keyboardShortcut(.escape, modifiers: [])
        }
        .buttonStyle(.borderless).padding(.horizontal, 20).padding(.vertical, 8)
    }

    private func header(_ session: Session) -> some View {
        ConversationHistoryActions(store: store, session: session)
            .frame(maxWidth: ConversationReadingLane.maximumWidth, alignment: .leading)
            .padding(.horizontal, ConversationReadingLane.minimumMargin)
            .frame(maxWidth: .infinity)
            .help([session.source, store.projectName(for: session),
                   session.machineID == "local" ? nil : session.machineID,
                   store.createdConversations.contexts[session.id]?.workspace.branch]
                .compactMap { $0 }.joined(separator: " · "))
    }
}
