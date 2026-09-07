import AppKit
import SwiftUI

struct ReaderView: View {
    @Bindable var store: Store
    @State private var copiedResume = false
    @State private var navigation = TranscriptNavigationState()
    @State private var find: ConversationFindState?
    @FocusState private var findFocused: Bool

    var body: some View {
        Group {
            if let session = store.selected {
                VStack(spacing: 0) {
                    header(session)
                    Divider().opacity(0.5)
                    if let find, find.isOpen { findBar(find) }
                    NativeTranscript(sessionID: store.readerPositionKey,
                                     records: store.loadedReaderKey == store.readerPositionKey ? store.records : [],
                                     provider: session.source, hasMore: store.hasMoreRecords,
                                     isLoading: store.loadingRecords,
                                     onLoadMore: { Task { await store.loadMoreRecords() } },
                                     hasEarlier: store.hasEarlierRecords, startsAtEnd: store.readerStartsAtEnd,
                                     anchorID: store.readerAnchorID, navigation: navigation,
                                     onLoadEarlier: { Task { await store.loadEarlierRecords() } },
                                     findQuery: find?.isOpen == true ? find?.query ?? "" : "",
                                     findHit: find?.selectedHit, findGeneration: find?.generation ?? 0)
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
                .toolbar {
                    ToolbarItemGroup(placement: .primaryAction) {
                        Button {
                            find?.isOpen = true
                            findFocused = true
                        } label: { Label("Find in conversation", systemImage: "magnifyingglass") }
                        .keyboardShortcut("f", modifiers: .command)
                        .help("Find in conversation (⌘F)")
                        Button { NSPasteboard.general.clearContents(); NSPasteboard.general.setString(session.sessionID, forType: .string) } label: {
                            Label("Copy session ID", systemImage: "link")
                        }.help("Copy session ID")
                        if session.machineID == "local" {
                            Button { NSWorkspace.shared.activateFileViewerSelecting([URL(fileURLWithPath: session.sourcePath)]) } label: {
                                Label("Reveal source", systemImage: "doc")
                            }.help("Reveal transcript in Finder")
                        }
                        if session.machineID == "local", let command = session.resumeCommand {
                            Button {
                                NSPasteboard.general.clearContents()
                                NSPasteboard.general.setString(command, forType: .string)
                                copiedResume = true
                            } label: {
                                Label(copiedResume ? "Copied" : "Copy resume command", systemImage: copiedResume ? "checkmark" : "terminal")
                            }.help("Copy resume command")
                        }
                    }
                }
            } else {
                ContentUnavailableView("Your conversations, together", systemImage: "bubble.left.and.bubble.right",
                    description: Text("Select a conversation or search your history."))
            }
        }
        .onAppear { if find == nil { find = ConversationFindState(client: store.client) } }
        .onChange(of: store.selectedID) { _, _ in
            copiedResume = false
            find?.search(in: store.selected)
        }
        .onChange(of: find?.query) { _, _ in find?.search(in: store.selected) }
        .task(id: find?.generation) {
            guard let hit = find?.selectedHit else { return }
            await store.revealRecord(hit.recordID, offset: hit.recordOffset)
        }
        .onDisappear { find?.reset() }
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
        VStack(alignment: .leading, spacing: 8) {
            Text(session.title).font(.title2.weight(.semibold)).lineLimit(2)
            HStack(spacing: 8) {
                Text(session.source)
                Text("·")
                Text(session.projectName)
                if session.machineID != "local" {
                    Text("·")
                    Label(session.machineID, systemImage: "desktopcomputer")
                }
                Spacer()
                if let date = session.date { Text(date, format: .dateTime.month().day().hour().minute()) }
            }
            .font(.caption).foregroundStyle(.secondary).lineLimit(1)
        }
        .padding(.horizontal, 30).padding(.vertical, 22)
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
