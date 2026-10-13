import SwiftUI

struct WorkspacePanelView: View {
    @Bindable var store: Store

    var body: some View {
        VStack(spacing: 0) {
            if store.selectedID != nil {
                WorkspacePanelTabs(panels: store.openWorkspacePanels, selection: store.workspacePanel,
                                   select: store.selectWorkspacePanel, closeTab: store.closeWorkspacePanel)
            }
            Divider()
            // Keep opened panes mounted so the launcher and tab switches retain selections and edits.
            // File drafts also survive closing the inspector or changing chats.
            ZStack {
                if store.openWorkspacePanels.contains(.changes) {
                    Group {
                        if let remote = store.selectedRemoteWorkspace {
                            RemoteWorkspacePanel(connection: remote.connection, workspaceID: remote.id, panel: .changes)
                        } else if let directory = store.selectedWorkspace {
                            WorkspaceChangesView(directory: directory, isWorking: store.selectedLiveConversation?.isWorking == true,
                                                 initialSelectedPath: store.selectedWorkspaceChange,
                                                 reviewRequest: store.workspaceChangeReviewRequest,
                                                 conversationID: store.selectedID,
                                                 isolation: { store.selected.flatMap { store.workspaceIsolation(for: $0) } },
                                                 rewindConversation: { try await store.rewindConversation(to: $0) },
                                                 addReviewContext: store.selectedLiveConversation.map { live in
                                                     { context in live.appendContext(title: "Code review", text: context.promptText, source: directory.path) }
                                                 },
                                                 setupCommand: store.selected.flatMap { session in
                                                     store.createdConversations.contexts[session.id]?.projectID
                                                 }.flatMap { id in store.localProjects.projects.first { $0.id == id }?.setupCommand })
                        } else {
                            ContentUnavailableView("Workspace unavailable", systemImage: "folder",
                                description: Text("Git changes are available for conversations with a local workspace."))
                        }
                    }
                    .opacity(store.workspacePanel == .changes ? 1 : 0)
                    .allowsHitTesting(store.workspacePanel == .changes)
                    .accessibilityHidden(store.workspacePanel != .changes)
                }
                if store.openWorkspacePanels.contains(.files), let remote = store.selectedRemoteWorkspace {
                    RemoteWorkspacePanel(connection: remote.connection, workspaceID: remote.id, panel: .files)
                        .id(remote.connection.id + remote.id)
                        .opacity(store.workspacePanel == .files ? 1 : 0)
                        .allowsHitTesting(store.workspacePanel == .files)
                        .accessibilityHidden(store.workspacePanel != .files)
                } else if store.openWorkspacePanels.contains(.files), let directory = store.selectedWorkspace {
                    WorkspaceFilesView(directory: directory, addContext: store.selectedLiveConversation.map { live in
                        { text in live.appendContext(title: "Workspace file", text: text, source: directory.path) }
                    })
                        .id(directory)
                        .opacity(store.workspacePanel == .files ? 1 : 0)
                        .allowsHitTesting(store.workspacePanel == .files)
                        .accessibilityHidden(store.workspacePanel != .files)
                }
                if store.openWorkspacePanels.contains(.browser), let sessionID = store.selectedID {
                    WorkspaceBrowserTabView(tabs: store.workspaceBrowser.tabs(for: sessionID),
                                            automation: store.workspaceBrowser.automation,
                                            live: store.selectedLiveConversation,
                                            isActive: store.workspacePanel == .browser)
                        .opacity(store.workspacePanel == .browser ? 1 : 0)
                        .allowsHitTesting(store.workspacePanel == .browser)
                        .accessibilityHidden(store.workspacePanel != .browser)
                }
                if store.workspacePanel == .terminal && !store.showingTerminalDrawer {
                    if let remote = store.selectedRemoteWorkspace {
                        RemoteWorkspacePanel(connection: remote.connection, workspaceID: remote.id, panel: .terminal)
                    } else { WorkspaceTerminalView(store: store, placement: .rightPane) }
                }
                if store.workspacePanel == .tools {
                    WorkspaceToolsView(hasWorkspace: store.hasSelectedWorkspace, select: store.selectWorkspacePanel)
                }
            }
        }
    }
}

private struct WorkspacePanelTabs: View {
    let panels: [Store.WorkspacePanel]
    let selection: Store.WorkspacePanel
    let select: (Store.WorkspacePanel) -> Void
    let closeTab: (Store.WorkspacePanel) -> Void

    var body: some View {
        HStack(spacing: 4) {
            ScrollViewReader { proxy in
                ScrollView(.horizontal) {
                    HStack(spacing: 4) {
                        ForEach(panels) { panel in tab(panel).id(panel) }
                        if selection == .tools {
                            Label("Tools", systemImage: Store.WorkspacePanel.tools.symbol)
                                .font(.system(size: 12, weight: .medium))
                                .padding(.horizontal, 10).frame(height: 28)
                                .background(Color.primary.opacity(0.08), in: RoundedRectangle(cornerRadius: 6))
                                .accessibilityAddTraits(.isSelected)
                                .id(Store.WorkspacePanel.tools)
                        }
                    }
                }
                .scrollIndicators(.hidden)
                .onChange(of: selection) { _, panel in proxy.scrollTo(panel, anchor: .trailing) }
                .onChange(of: panels) { _, _ in proxy.scrollTo(selection, anchor: .trailing) }
            }
            .frame(height: 28)
            if !panels.isEmpty {
                Button { select(.tools) } label: { Image(systemName: "plus").frame(width: 28, height: 28) }
                    .buttonStyle(.plain).foregroundStyle(.secondary)
                    .help("Open a workspace tool").accessibilityLabel("Open a workspace tool")
            }
            Spacer(minLength: 4)
        }
        .padding(.horizontal, 6).padding(.vertical, 5)
        .background(.bar)
    }

    private func tab(_ panel: Store.WorkspacePanel) -> some View {
        HStack(spacing: 0) {
            Button { select(panel) } label: {
                Label(panel.title, systemImage: panel.symbol)
                    .lineLimit(1).padding(.leading, 10).padding(.trailing, 6).frame(height: 28)
                    .contentShape(Rectangle())
            }
            .help(panel == .terminal ? "Workspace terminal (⌘J for drawer)" : panel.title)
            .accessibilityLabel(panel.title)
            .accessibilityValue(selection == panel ? "Selected" : "")
            .accessibilityAddTraits(selection == panel ? .isSelected : [])
            Button { closeTab(panel) } label: {
                Image(systemName: "xmark").font(.system(size: 9)).frame(width: 22, height: 28)
                    .contentShape(Rectangle())
            }
            .help("Close \(panel.title)").accessibilityLabel("Close \(panel.title)")
        }
        .font(.system(size: 12, weight: selection == panel ? .medium : .regular))
        .foregroundStyle(selection == panel ? .primary : .secondary)
        .background(selection == panel ? Color.primary.opacity(0.08) : .clear,
                    in: RoundedRectangle(cornerRadius: 6))
        .buttonStyle(.plain)
    }
}

private struct WorkspaceToolsView: View {
    let hasWorkspace: Bool
    let select: (Store.WorkspacePanel) -> Void

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                Text("Tools").font(.headline)
                LazyVGrid(columns: [GridItem(.flexible()), GridItem(.flexible())], spacing: 8) {
                    tool(.terminal)
                    tool(.files)
                    tool(.browser)
                    tool(.changes)
                }
                if !hasWorkspace {
                    Text("Files, changes, and terminal need an accessible workspace for this session.")
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
            .padding(24)
            .frame(maxWidth: 720, alignment: .leading)
            .frame(maxWidth: .infinity, alignment: .topLeading)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .background(Color(nsColor: .windowBackgroundColor))
    }

    private func tool(_ panel: Store.WorkspacePanel) -> some View {
        Button { select(panel) } label: {
            HStack(spacing: 12) {
                Image(systemName: panel.symbol).font(.system(size: 17)).frame(width: 22)
                    .foregroundStyle(.secondary)
                Text(panel.title).font(.system(size: 14))
                Spacer(minLength: 0)
            }
            .padding(.horizontal, 16).frame(height: 56)
            .contentShape(RoundedRectangle(cornerRadius: 10))
        }
        .buttonStyle(WorkspaceToolButtonStyle())
        .disabled(panel != .browser && !hasWorkspace)
        .accessibilityLabel("Open \(panel.title)")
    }
}

private struct WorkspaceToolButtonStyle: ButtonStyle {
    @Environment(\.isEnabled) private var isEnabled
    @State private var hovering = false

    func makeBody(configuration: Configuration) -> some View {
        configuration.label
            .background(Color.primary.opacity(configuration.isPressed ? 0.1 : hovering ? 0.07 : 0.04),
                        in: RoundedRectangle(cornerRadius: 10))
            .opacity(isEnabled ? 1 : 0.45)
            .onHover { hovering = $0 }
    }
}
