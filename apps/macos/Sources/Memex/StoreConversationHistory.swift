import Foundation

extension Store {
    func captureTurnCheckpoint(session: Session, turnID: String, moment: WorkspaceCheckpoint.Moment) async {
        guard canAccessLocalFiles(for: session), let cwd = session.cwd else { return }
        let directory = URL(fileURLWithPath: cwd)
        do {
            guard try await workspaceClient.inspect(directory: directory) != nil else { return }
            _ = try await WorkspaceCheckpointClient.shared.capture(directory: directory, conversationID: session.id,
                turnID: turnID, moment: moment,
                label: "\(moment == .beforeTurn ? "Before" : "After") turn · \(Date().formatted(date: .abbreviated, time: .shortened))")
            workspaceCheckpointWarning = nil
        } catch { workspaceCheckpointWarning = "No workspace checkpoint was captured: \(error.localizedDescription)" }
    }

    var selectedHistoryBoundaries: [ConversationHistoryBoundary] {
        ConversationHistoryBoundary.choices(in: selectedLiveConversation?.snapshot.records ?? records)
    }

    /// Read complete history for an explicit context transfer. A size bound is
    /// an error, never a silently shortened representation of the conversation.
    func contextRecords(for session: Session) async throws -> [TranscriptRecord] {
        if let live = liveConversations.sessions[session.id], live.hasSnapshot { return live.snapshot.records }
        if createdConversations.contains(session), session.machineID == "local" {
            return try await NewConversationRuntime.records(for: session)
        }
        var result: [TranscriptRecord] = []
        var bytes = 0
        while true {
            let page = try await client.records(for: session, offset: result.count)
            bytes += page.reduce(0) { $0 + $1.rawTranscriptBody.utf8.count }
            guard bytes <= ConversationContextCapture.maximumBytes else {
                throw ConversationRuntimeError(message: "This conversation exceeds the context limit. Attach a smaller selected excerpt instead.")
            }
            result += page
            if page.count < MemexClient.pageSize { return result }
            try Task.checkCancellation()
        }
    }

    @discardableResult
    func branchWithContext(source: Session, provider: String, isolatedWorkspace: Bool,
                           plan: ConversationWork.Plan? = nil) async throws -> Session {
        guard !historyActionInProgress else { throw ConversationRuntimeError(message: "Another history action is in progress.") }
        historyActionInProgress = true
        defer { historyActionInProgress = false }
        let records = try await contextRecords(for: source)
        let context = try ConversationContextCapture.text(session: source, records: records)
        // Validate capture before creating a new provider session.
        let attachment = try ConversationAttachment.text(title: "Context from " + source.title, text: context,
                                                       source: "memex-conversation://" + source.sessionID)
        var attachments = [attachment]
        if let plan {
            attachments.append(try ConversationAttachment.text(title: "Plan", text: plan.text,
                source: plan.provenance(session: source)))
        }
        let workspace: ConversationWorkspace
        if canAccessLocalFiles(for: source), let cwd = source.cwd?.nilIfBlank {
            workspace = try await workspaceClient.prepare(directory: URL(fileURLWithPath: cwd),
                mode: isolatedWorkspace ? .newWorktree : .existingDirectory)
        } else { workspace = try await workspaceClient.prepareTemporaryDirectory() }
        let originalContext = createdConversations.contexts[source.id]
        let operation = try conversationRelationships.begin(source: source, operation: "context-branch")
        let live = try await createConversation(.init(provider: provider, workingDirectory: workspace.workingDirectory),
            context: .init(projectID: originalContext?.projectID, projectName: projectName(for: source), workspace: workspace),
            navigate: false)
        try conversationRelationships.recordResult(live.session, for: operation)
        guard live.appendCapturedContext(attachments) else {
            openNotifiedConversation(live.session.id)
            throw ConversationRuntimeError(message: "The new conversation was retained, but its provider could not accept the captured context. Open it to inspect the draft before sending.")
        }
        if plan != nil { live.draft = "Implement the attached plan." }
        await liveConversations.drafts.flush()
        if let error = live.draftSaveError ?? createdConversations.error { throw ConversationRuntimeError(message: error) }
        try conversationRelationships.finish(operation, link: .init(parent: source, child: live.session,
            kind: source.source == provider ? .contextBranch : .providerTransition, sourceRecordID: records.last?.sourceID, createdAt: Date()))
        revealCreatedConversation(live.session)
        live.focus()
        return live.session
    }

    @discardableResult
    func mutateConversation(source: Session, operation: ConversationHistoryMutation.Operation,
                            boundary: ConversationHistoryBoundary) async throws -> Session {
        guard !historyActionInProgress, let live = liveConversations.sessions[source.id], live.canMutateHistory else {
            throw ConversationRuntimeError(message: "Connect an idle conversation before changing native history.")
        }
        historyActionInProgress = true
        defer { historyActionInProgress = false }
        let pending = try conversationRelationships.begin(source: source, operation: operation.rawValue, boundary: boundary)
        let result = try await live.mutateHistory(.init(operation: operation, boundary: boundary))
        try conversationRelationships.recordResult(result, for: pending)
        if result.id != source.id {
            createdConversations.save(result, context: createdConversations.contexts[source.id])
            if let error = createdConversations.error { throw ConversationRuntimeError(message: error) }
        }
        try conversationRelationships.finish(pending, link: result.id == source.id ? nil :
            .init(parent: source, child: result, kind: operation == .fork ? .nativeFork : .rewind,
                  sourceRecordID: boundary.recordID, createdAt: Date()))
        revealCreatedConversation(result)
        liveConversations.prepare(result)
        if operation == .revert, result.id == source.id { _ = await live.connect() }
        return result
    }

    func mergeContextToParent(from source: Session) async throws {
        guard let relationship = conversationRelationships.parent(of: source.id) else {
            throw ConversationRuntimeError(message: "This conversation has no saved parent.")
        }
        let context = try ConversationContextCapture.text(session: source, records: await contextRecords(for: source))
        let parent = relationship.parent
        openRelatedConversation(parent)
        liveConversations.prepare(parent)
        guard let live = liveConversations.sessions[parent.id],
              live.appendContext(title: "Context from " + source.title, text: context,
                                 source: "memex-conversation://" + source.sessionID) else {
            throw ConversationRuntimeError(message: "The parent draft could not accept this context. Its existing draft has been preserved.")
        }
        await liveConversations.drafts.flush()
        if let error = live.draftSaveError { throw ConversationRuntimeError(message: error) }
        live.focus()
    }

    func openRelatedConversation(_ session: Session) {
        if !sessions.contains(where: { $0.id == session.id }) { sessions.append(session) }
        openNotifiedConversation(session.id)
    }

    func workspaceIsolation(for session: Session) -> WorkspaceIsolation? {
        guard let workspace = createdConversations.contexts[session.id]?.workspace else { return nil }
        let known = sessions + catalog + createdConversations.sessions + conversationLibrary.entries.values.map(\.session)
            + liveConversations.sessions.values.map(\.session)
        let directories = known.filter { $0.id != session.id && $0.machineID == "local" }
            .compactMap { $0.cwd }.map { URL(fileURLWithPath: $0) }
        return WorkspaceIsolation(workspace: workspace, otherWorkspaceDirectories: directories,
            isBusy: liveConversations.sessions[session.id]?.isWorking == true || workspaceTerminals.needsCloseConfirmation)
    }

    func rewindConversation(to checkpoint: WorkspaceCheckpoint) async throws {
        guard let live = liveConversations.sessions[checkpoint.conversationID], let turn = checkpoint.turnID else {
            throw ConversationRuntimeError(message: "This checkpoint has no verified native conversation boundary.")
        }
        let nativeTurn: String?
        if turn.hasPrefix("command:") {
            nativeTurn = live.snapshot.deliveries.first { $0.commandID == String(turn.dropFirst(8)) }?.nativeTurnID
        } else { nativeTurn = turn }
        let boundaries = ConversationHistoryBoundary.choices(in: live.snapshot.records)
        guard let index = boundaries.firstIndex(where: { $0.turnID == nativeTurn && nativeTurn != nil }) else {
            throw ConversationRuntimeError(message: "Reload the native history to resolve this checkpoint's turn before rewinding.")
        }
        let targetIndex = checkpoint.moment == .afterTurn ? index + 1 : index
        if targetIndex == boundaries.count { return } // Already at the selected end of history.
        _ = try await mutateConversation(source: live.session, operation: .revert, boundary: boundaries[targetIndex])
    }
}
