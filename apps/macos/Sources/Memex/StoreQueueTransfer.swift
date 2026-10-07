import Foundation

extension Store {
    /// Called only after the queue's review sheet. This transaction does not use
    /// the Home draft and never calls send, steer, or resumeQueue.
    func transferQueuedPrompt(source: Session, entry: ConversationQueuedPrompt) async throws {
        guard !historyActionInProgress else {
            throw ConversationRuntimeError(message: "Another conversation operation is in progress. Try moving this draft when it finishes.")
        }
        historyActionInProgress = true
        defer { historyActionInProgress = false }
        guard canAccessLocalFiles(for: source), let cwd = source.cwd?.nilIfBlank, cwd.hasPrefix("/") else {
            throw ConversationRuntimeError(message: "Moving a queued message to a side-chat draft is not available on this execution host. The original queued message is retained.")
        }
        createdConversations.retrySave()
        await liveConversations.drafts.retrySave()
        if let error = createdConversations.error ?? liveConversations.drafts.error {
            throw ConversationRuntimeError(message: error)
        }
        let existing = conversationRelationships.queueTransfers.first { $0.id == source.id + ":" + entry.id }
        let context: CreatedConversationCatalog.Context
        if let existing { context = existing.context }
        else {
            let workspace = try await workspaceClient.prepare(directory: URL(fileURLWithPath: cwd), mode: .existingDirectory)
            let original = createdConversations.contexts[source.id]
            context = .init(projectID: original?.projectID, projectName: projectName(for: source), workspace: workspace)
        }
        // Resolve the owning home before reserving: validation failure has not
        // crossed a provider boundary and can safely be corrected and retried.
        let home: URL?
        if source.source.hasPrefix("acp:") { home = nil }
        else { home = try InAppResumeTarget.providerHome(for: URL(fileURLWithPath: source.sourcePath), provider: source.source) }
        let (transfer, isNew) = try conversationRelationships.reserveQueueTransfer(source: source, entry: entry, context: context)
        let session: Session
        var created: CreatedConversation?
        if let result = transfer.result {
            session = result
        } else {
            guard isNew else {
                throw ConversationRuntimeError(message: "The previous side-chat creation could not be confirmed. Inspect the provider's native conversations before retrying; another conversation has not been created. The queued message is retained.")
            }
            let result = try await makeConversation(.init(provider: source.source,
                workingDirectory: context.workspace.workingDirectory, providerHome: home))
            session = result.session
            created = result
            // Record identity before any adoption or draft-writing suspension.
            do { try conversationRelationships.recordQueueTransferResult(session, transferID: transfer.id) }
            catch {
                createdConversations.save(session, context: context)
                throw ConversationRuntimeError(message: "Side chat \(session.sessionID) was created, but its transfer identity could not be saved. Inspect that conversation before retrying. The original queued message is retained.")
            }
        }
        createdConversations.save(session, context: context)
        if let error = createdConversations.error { throw ConversationRuntimeError(message: error) }
        if !transfer.completed {
            let desired = ConversationDraftStore.Draft(text: transfer.entry.text, attachments: transfer.entry.attachments)
            if let current = liveConversations.drafts.drafts[session.id], current != desired {
                throw ConversationRuntimeError(message: "The side-chat draft changed during transfer recovery. Its edits and the original queue are retained; review both before sending.")
            }
            liveConversations.drafts.set(desired, for: session.id)
            await liveConversations.drafts.retrySave()
            if let error = liveConversations.drafts.error { throw ConversationRuntimeError(message: error) }
            guard liveConversations.drafts.drafts[session.id] == desired else {
                throw ConversationRuntimeError(message: "The side-chat draft changed while saving. Its edits and the queued original are retained for review.")
            }
            try conversationRelationships.finishQueueTransfer(transfer, result: session)
        }
        if let created { _ = await liveConversations.adopt(created) }
        else { liveConversations.prepare(session) }
        revealCreatedConversation(session)
        liveConversations.sessions[session.id]?.focus()
    }
}
