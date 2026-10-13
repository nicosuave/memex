import Foundation

/// What the reader shows between a Home send and the created conversation.
struct PendingConversationStart: Identifiable, Equatable {
    enum Phase: Equatable { case preparingWorkspace, startingProvider, applyingSettings, attaching }
    let id = UUID()
    let prompt: String
    let attachmentNames: [String]
    let providerName: String
    let projectName: String?
    var phase: Phase
    var error: String?

    var status: String {
        switch phase {
        case .preparingWorkspace: "Preparing workspace…"
        case .startingProvider: "Starting \(providerName)…"
        case .applyingSettings: "Applying conversation settings…"
        case .attaching: "Attaching files…"
        }
    }
}

extension Store {
    func projectName(for session: Session) -> String {
        guard let context = createdConversations.contexts[session.id] else { return session.projectName }
        return localProjects.projects.first { $0.id == context.projectID }?.name ?? context.projectName
    }

    var newConversationProject: LocalProject? {
        localProjects.projects.first { $0.id == newConversationDraft.value.projectID }
    }

    var canStartConversation: Bool {
        canPrepareConversation && newConversationDraft.value.text.nilIfBlank != nil
    }

    /// Preparing a native session loads its real settings without sending a prompt.
    var canPrepareConversation: Bool {
        !startingConversation && (newConversationDraft.value.projectID == nil || newConversationProject != nil)
            && newConversationDraft.value.createdSessionID == nil
            && newConversationDraft.value.preparedWorkspace?.state != .failed
            && newConversationDraft.error == nil
            && (newConversationDraft.value.projectID == nil || localProjects.error == nil)
    }

    func startConversationFromHome(sendImmediately: Bool = true, attachments: [URL] = [],
                                   preferences: ConversationComposerPreferences.Selection? = nil,
                                   onPrepared: ((LiveConversation) -> Void)? = nil) async {
        guard sendImmediately ? canStartConversation : canPrepareConversation else { return }
        let project = newConversationProject
        startingConversation = true
        newConversationError = nil
        defer { startingConversation = false }
        let draft = newConversationDraft.value
        // Leave Home immediately; the reader shows the prompt while creation runs.
        var pendingID: UUID?
        if sendImmediately {
            let pending = PendingConversationStart(prompt: draft.text,
                attachmentNames: attachments.map(\.lastPathComponent),
                providerName: ConversationProviderCatalog.name(for: draft.provider), projectName: project?.name,
                phase: draft.preparedWorkspace == nil ? .preparingWorkspace : .startingProvider)
            pendingID = pending.id
            pendingConversationStart = pending
            if scope == .home { scope = .all }
            selectedID = nil
        }
        func advance(_ phase: PendingConversationStart.Phase) {
            if let pendingID, pendingConversationStart?.id == pendingID { pendingConversationStart?.phase = phase }
        }
        // Navigate only while the pending page is still what the user is looking at.
        func reveal(_ session: Session) {
            guard let pendingID else { return revealCreatedConversation(session) }
            if pendingConversationStart?.id == pendingID, selectedID == nil, scope != .home {
                pendingConversationStart = nil
                revealCreatedConversation(session)
            } else {
                if pendingConversationStart?.id == pendingID { pendingConversationStart = nil }
                sessions.removeAll { $0.id == session.id }
                sessions.insert(session, at: 0)
            }
        }
        do {
            await newConversationDraft.flush()
            if let error = newConversationDraft.error { throw ConversationRuntimeError(message: error) }
            if let error = createdConversations.error ?? liveConversations.drafts.error {
                throw ConversationRuntimeError(message: error)
            }

            let workspace: ConversationWorkspace
            if let prepared = draft.preparedWorkspace {
                workspace = prepared
            } else {
                if let project {
                    workspace = try await workspaceClient.prepare(
                        directory: URL(fileURLWithPath: project.directoryPath, isDirectory: true),
                        mode: draft.workspaceMode, baseRef: draft.baseRef)
                } else {
                    workspace = try await workspaceClient.prepareTemporaryDirectory()
                }
                newConversationDraft.value.preparedWorkspace = workspace
                await newConversationDraft.flush()
                if let error = newConversationDraft.error { throw ConversationRuntimeError(message: error) }
            }
            advance(.startingProvider)

            let context = CreatedConversationCatalog.Context(projectID: project?.id, projectName: project?.name ?? "No project", workspace: workspace)
            let conversation = try await createConversation(
                .init(provider: draft.provider, workingDirectory: workspace.workingDirectory),
                context: context, initialText: draft.text, navigate: false)
            // The native session and its draft must both survive relaunch before
            // retiring the Home draft or crossing the provider prompt boundary.
            if let error = createdConversations.error ?? newConversationDraft.error ?? liveConversations.drafts.error {
                throw ConversationRuntimeError(message: error)
            }
            advance(.applyingSettings)
            let preferences = preferences ?? ConversationComposerPreferences.selection(provider: draft.provider)
            if !(await conversation.applySettings(modelID: preferences.modelID, configurationValues: preferences.configurations)) {
                reveal(conversation.session)
                throw ConversationRuntimeError(message: conversation.error ?? "Conversation settings could not be confirmed. Review this draft before sending.")
            }
            if !attachments.isEmpty {
                advance(.attaching)
                await conversation.attachFiles(attachments)
                await liveConversations.drafts.flush()
                if let error = conversation.attachmentError ?? conversation.draftSaveError {
                    // Keep the created identity and open its editable draft. Retrying
                    // attachment capture must never allocate another native session.
                    reveal(conversation.session)
                    throw ConversationRuntimeError(message: error)
                }
            }
            try await newConversationDraft.finishTransfer()
            reveal(conversation.session)
            conversation.focus()
            onPrepared?(conversation)
            if sendImmediately { await conversation.send() }
        } catch {
            if let failed = (error as? ConversationWorkspacePreparationError)?.retainedWorkspace {
                newConversationDraft.value.preparedWorkspace = failed
                await newConversationDraft.flush()
            }
            newConversationError = error.localizedDescription
            if let pendingID, pendingConversationStart?.id == pendingID {
                pendingConversationStart?.error = error.localizedDescription
            }
        }
    }

    /// Leave a failed pending start for Home, where the retained draft can be edited or retried.
    func returnToHomeDraft() {
        pendingConversationStart = nil
        scope = .home
    }

    /// Recover the already-created chat after a save failure or app exit. This
    /// never sends: its durable draft or uncertain delivery remains authoritative.
    func openCreatedConversationFromHome() async {
        guard !startingConversation, let id = newConversationDraft.value.createdSessionID else { return }
        guard let session = createdConversations.sessions.first(where: { $0.id == id }) else {
            newConversationError = "The created conversation is missing from the saved list. Your prompt and workspace have been retained."
            return
        }
        createdConversations.retrySave()
        if let error = createdConversations.error {
            newConversationError = error
            return
        }
        if liveConversations.drafts.drafts[id] == nil, !newConversationDraft.value.text.isEmpty {
            liveConversations.drafts.set(.init(text: newConversationDraft.value.text), for: id)
            await liveConversations.drafts.flush()
        }
        await liveConversations.drafts.retrySave()
        if let error = liveConversations.drafts.error {
            newConversationError = error
            return
        }
        do { try await newConversationDraft.finishTransfer() }
        catch {
            newConversationError = error.localizedDescription
            return
        }
        newConversationError = nil
        revealCreatedConversation(session)
    }
}
