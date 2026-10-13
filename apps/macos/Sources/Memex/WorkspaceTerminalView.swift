import AppKit
import SwiftUI

struct WorkspaceTerminalView: View {
    enum Placement { case rightPane, drawer }
    @Bindable var store: Store
    let placement: Placement
    @State private var group: WorkspaceTerminalGroup?
    @State private var loadedDirectory: URL?
    @State private var error: String?
    @State private var retry = 0

    private struct Request: Equatable {
        let directory: URL?
        let retry: Int
    }

    var body: some View {
        Group {
            if let directory = store.selectedWorkspace {
                if directory == loadedDirectory, let group {
                    WorkspaceTerminalGroupView(group: group, store: store, placement: placement)
                } else if directory == loadedDirectory, let error {
                    ContentUnavailableView {
                        Label("Couldn’t open terminal", systemImage: "terminal")
                    } description: {
                        Text(error)
                    } actions: {
                        Button("Try again") { retry += 1 }
                    }
                } else {
                    ProgressView("Opening workspace terminal…")
                        .frame(maxWidth: .infinity, maxHeight: .infinity)
                }
            } else {
                ContentUnavailableView("Local workspace unavailable", systemImage: "terminal",
                    description: Text("A terminal requires a conversation with a local working folder."))
            }
        }
        .task(id: Request(directory: store.selectedWorkspace, retry: retry)) {
            let directory = store.selectedWorkspace
            error = nil
            guard let directory else { group = nil; loadedDirectory = nil; return }
            do {
                let next = try await store.workspaceTerminals.group(for: directory)
                guard !Task.isCancelled, store.selectedWorkspace == directory else { return }
                group = next
                loadedDirectory = directory
            } catch {
                guard !Task.isCancelled, store.selectedWorkspace == directory else { return }
                group = nil
                self.error = error.localizedDescription
                loadedDirectory = directory
            }
        }
    }
}

private struct WorkspaceTerminalGroupView: View {
    @Bindable var group: WorkspaceTerminalGroup
    @Bindable var store: Store
    let placement: WorkspaceTerminalView.Placement
    @State private var closing: WorkspaceTerminalSession?
    @State private var restarting: WorkspaceTerminalSession?
    @State private var showingHistory = false
    @State private var contextError: String?

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 6) {
                ScrollView(.horizontal) {
                    HStack(spacing: 4) {
                        ForEach(group.sessions, id: \.id) { session in
                            Button { group.selectedID = session.id } label: {
                                HStack(spacing: 4) {
                                    Image(systemName: "terminal")
                                    Text(session.isExited ? "Exited" : session.title).lineLimit(1)
                                }
                                .padding(5)
                                .background(group.selectedID == session.id ? Color.accentColor.opacity(0.15) : .clear,
                                            in: RoundedRectangle(cornerRadius: 4))
                            }
                        }
                    }
                }
                Button { _ = group.add() } label: { Image(systemName: "plus") }
                    .disabled(group.sessions.count >= 16)
                    .help("New independent shell").accessibilityLabel("New terminal")
                Menu {
                    Picker("Layout", selection: $group.layout) {
                        Text("Tabs").tag(WorkspaceTerminalGroup.Layout.tabs)
                        Text("Side by side").tag(WorkspaceTerminalGroup.Layout.sideBySide)
                        Text("Stacked").tag(WorkspaceTerminalGroup.Layout.stacked)
                    }
                    Divider()
                    Button("Add selection to chat") { appendContext(group.selected.selectedText()) }
                    Button("Add scrollback to chat") { appendContext(group.selected.captureHistory()) }
                    Button("Save scrollback snapshot") { group.saveHistory(from: group.selected) }
                    Button("Show saved snapshot") { showingHistory = true }.disabled(group.savedHistory.isEmpty)
                    Button("Clear terminal scrollback") { group.selected.clearHistory() }
                    Divider()
                    Button("Restart shell") {
                        if group.selected.needsCloseConfirmation { restarting = group.selected }
                        else { group.selected.restart() }
                    }
                    Button("Close shell", role: .destructive) { requestClose(group.selected) }
                } label: { Image(systemName: "ellipsis") }
                .menuStyle(.borderlessButton).menuIndicator(.hidden).fixedSize()
                Button {
                    if placement == .drawer { store.showWorkspaceTerminal() }
                    else { store.toggleTerminalDrawer() }
                } label: {
                    Image(systemName: placement == .drawer ? "sidebar.right" : "rectangle.bottomthird.inset.filled")
                }
                .help(placement == .drawer ? "Move terminals to right pane" : "Move terminals to bottom (⌘J)")
                if placement == .drawer {
                    Button { store.toggleTerminalDrawer() } label: { Image(systemName: "chevron.down") }
                        .help("Hide terminals (⌘J)")
                }
            }
            .font(.system(size: 12)).buttonStyle(.plain).padding(8).background(.bar)
            Divider()
            if group.layout == .sideBySide {
                HSplitView { panes }
            } else if group.layout == .stacked {
                VSplitView { panes }
            } else { panes }
            if let error = contextError ?? group.historyError {
                Text(error).font(.caption).foregroundStyle(.secondary).padding(8)
            }
        }
        .alert("End this terminal?", isPresented: Binding(get: { closing != nil }, set: { if !$0 { closing = nil } })) {
            Button("End Terminal", role: .destructive) { if let closing { group.remove(closing) }; closing = nil }
            Button("Cancel", role: .cancel) { closing = nil }
        } message: { Text("This ends this shell and its running processes. Other terminals stay open.") }
        .alert("Restart this terminal?", isPresented: Binding(get: { restarting != nil }, set: { if !$0 { restarting = nil } })) {
            Button("Restart", role: .destructive) {
                if let restarting { group.saveHistory(from: restarting); restarting.close(); restarting.restart() }
                restarting = nil
            }
            Button("Cancel", role: .cancel) { restarting = nil }
        } message: { Text("This ends the current shell and its processes, then starts a new shell in the same workspace.") }
        .sheet(isPresented: $showingHistory) {
            VStack(alignment: .leading, spacing: 12) {
                Text("Saved terminal snapshot").font(.headline)
                Text("Captured output only. Shell processes do not survive quitting Memex.").foregroundStyle(.secondary)
                ScrollView { Text(group.savedHistory).font(.system(.caption, design: .monospaced)).textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading) }
                Button("Done") { showingHistory = false }
            }.padding().frame(minWidth: 560, minHeight: 360)
        }
    }

    private var panes: some View {
        ForEach(group.visible, id: \.id) { session in
            VStack(spacing: 0) {
                if group.layout != .tabs {
                    HStack {
                        Text(session.title).lineLimit(1)
                        Spacer()
                        Button { requestClose(session) } label: { Image(systemName: "xmark") }
                            .help("Close this shell")
                    }.font(.caption).padding(6).background(.bar)
                }
                if session.isExited && !session.isStarted && !session.capturedHistory.isEmpty {
                    ScrollView {
                        Text(session.capturedHistory).font(.system(.caption, design: .monospaced))
                            .textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading).padding(8)
                    }
                    Text("Saved output · shell ended when Memex closed").font(.caption).foregroundStyle(.secondary)
                } else {
                    WorkspaceTerminalSurface(session: session, isActive: true,
                                             focusRequest: store.terminalFocusRequest, wantsFocus: group.selectedID == session.id)
                        .id(session.id).frame(minWidth: 120, minHeight: 100)
                }
                if let error = session.error { Text(error).font(.caption).textSelection(.enabled).padding(6) }
                if session.isExited { Button("Restart shell") { session.restart() }.padding(6) }
            }
            .onChange(of: session.closeRequested) { _, requested in
                if requested { session.closeRequested = false; requestClose(session) }
            }
        }
    }

    private func requestClose(_ session: WorkspaceTerminalSession) {
        if session.needsCloseConfirmation { closing = session } else { group.remove(session) }
    }

    private func appendContext(_ text: String) {
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else {
            contextError = "Select terminal text or wait for output before adding context."; return
        }
        guard let live = store.selectedLiveConversation else {
            contextError = "Resume this conversation before adding terminal context."; return
        }
        contextError = live.appendContext(title: "Terminal: " + group.selected.title, text: text,
                                         source: group.directory.path) ? nil : "The composer could not accept terminal context."
    }
}

/// The reader keeps its identity when the drawer opens. Its height changes,
/// while the terminal's session is owned separately by the workspace registry.
struct WorkspaceTerminalDrawer<Content: View>: View {
    @Bindable var store: Store
    @ViewBuilder let content: () -> Content
    @AppStorage("workspace-terminal-drawer-height") private var preferredHeight = 280.0
    @State private var dragStart: CGFloat?
    @State private var hoveringDivider = false

    var body: some View {
        GeometryReader { geometry in
            VStack(spacing: 0) {
                content().frame(maxWidth: .infinity, maxHeight: .infinity)
                if store.showingTerminalDrawer {
                    VStack(spacing: 0) {
                        resizeHandle(totalHeight: geometry.size.height)
                        WorkspaceTerminalView(store: store, placement: .drawer)
                    }
                    .frame(height: drawerHeight(in: geometry.size.height))
                }
            }
        }
    }

    private func drawerHeight(in totalHeight: CGFloat) -> CGFloat {
        min(max(140, preferredHeight), max(140, totalHeight - 180))
    }

    private func resizeHandle(totalHeight: CGFloat) -> some View {
        Color.clear.frame(height: 5)
            .overlay { Divider() }
            .contentShape(Rectangle())
            .onHover { hovering in
                guard hovering != hoveringDivider else { return }
                hoveringDivider = hovering
                if hovering { NSCursor.resizeUpDown.push() } else { NSCursor.pop() }
            }
            .onDisappear {
                if hoveringDivider { NSCursor.pop(); hoveringDivider = false }
                dragStart = nil
            }
            .gesture(DragGesture(minimumDistance: 1).onChanged { value in
                if dragStart == nil { dragStart = drawerHeight(in: totalHeight) }
                preferredHeight = min(max(140, (dragStart ?? preferredHeight) - value.translation.height),
                                      max(140, totalHeight - 180))
            }.onEnded { _ in dragStart = nil })
            .accessibilityLabel("Terminal drawer height")
            .accessibilityAdjustableAction { direction in
                preferredHeight = drawerHeight(in: totalHeight) + (direction == .increment ? 40 : -40)
            }
    }
}
