import Foundation
import Testing
@testable import Memex

private actor HomeConversationRuntime: ConversationRuntime {
    var creations: [URL] = []
    var prompts: [String] = []
    var failCreation = false

    func failNextCreation() { failCreation = true }
    func create(_ request: NewConversationRequest) throws -> CreatedConversation {
        creations.append(request.workingDirectory)
        if failCreation {
            failCreation = false
            throw ConversationRuntimeError(message: "Creation failed before native session allocation")
        }
        let session = Session(source: request.provider, sessionID: "home-native-\(creations.count)",
            sourcePath: "/tmp/home-native/sessions/new.jsonl", project: request.workingDirectory.lastPathComponent,
            cwd: request.workingDirectory.path)
        let target = InAppResumeTarget(session: session, sourceURL: URL(fileURLWithPath: session.sourcePath),
            workingDirectory: request.workingDirectory, providerHome: URL(fileURLWithPath: "/tmp/home-native"),
            executableURL: URL(fileURLWithPath: "/bin/echo"), helperURL: nil,
            storageURL: URL(fileURLWithPath: "/tmp/home-native-runtime"))
        return CreatedConversation(session: session, runtime: self, target: target)
    }
    func connect(_ target: InAppResumeTarget,
                 receive: @escaping @Sendable (Result<ConversationSnapshot, ConversationRuntimeError>) -> Void) {
        receive(.success(ConversationSnapshot(connected: true, ready: true, canCancel: true)))
    }
    func perform(_ command: ConversationCommand) { if command.action == .prompt { prompts.append(command.text) } }
    func disconnect() {}
}

@Suite(.serialized) @MainActor struct HomeConversationTests {
    private func directory() throws -> URL {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        return root
    }

    @Test func homeSubmitCreatesOnceTransfersDurableDraftAndRecordsProject() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let projects = LocalProjects(directory: root.appendingPathComponent("projects"))
        let project = try projects.save(name: "My project", directory: root)
        let home = NewConversationDraft(directory: root.appendingPathComponent("home"))
        home.selectProject(project)
        home.value.text = "Build the requested feature"
        let runtime = HomeConversationRuntime()
        let drafts = ConversationDraftStore(directory: root.appendingPathComponent("drafts"))
        let catalogRoot = root.appendingPathComponent("catalog")
        let store = Store(draftStore: drafts, createdConversations: CreatedConversationCatalog(directory: catalogRoot),
            localProjects: projects, newConversationDraft: home, makeConversation: { try await runtime.create($0) })
        async let first: Void = store.startConversationFromHome()
        async let duplicate: Void = store.startConversationFromHome()
        _ = await (first, duplicate)
        #expect(await runtime.creations.count == 1)
        #expect(await runtime.prompts == ["Build the requested feature"])
        let session = try #require(store.selected)
        #expect(store.scope == .all)
        #expect(home.value.text.isEmpty)
        #expect(home.value.createdSessionID == nil)
        #expect(home.value.projectID == project.id)
        #expect(NewConversationDraft(directory: root.appendingPathComponent("home")).value.text.isEmpty)
        let reopened = CreatedConversationCatalog(directory: catalogRoot)
        #expect(reopened.contexts[session.id]?.projectName == "My project")
        #expect(reopened.contexts[session.id]?.workspace.workingDirectory == root.resolvingSymlinksInPath())
        #expect(drafts.drafts[session.id]?.pendingPrompt?.text == "Build the requested feature")
        let list = ConversationListController()
        list.update(sessions: [session], selectedID: session.id, projectNames: store.conversationProjectNames,
                    select: { _ in }, loadMore: { _ in })
        #expect(list.rows[0].metadata == "My project · codex")
        #expect(list.rows[0].session == session)
        var renamed = project
        renamed.name = "Renamed project"
        try projects.update(renamed)
        list.update(sessions: [session], selectedID: session.id, projectNames: store.conversationProjectNames,
                    select: { _ in }, loadMore: { _ in })
        #expect(list.rows[0].metadata == "Renamed project · codex")
        #expect(store.projectName(for: session) == "Renamed project")
        try projects.remove(id: project.id)
        #expect(store.projectName(for: session) == "My project")
        #expect(store.createdConversations.contexts[session.id]?.workspace.workingDirectory == root.resolvingSymlinksInPath())
        await store.liveConversations.disconnectAll()
    }

    @Test func openBlankDraftCreatesOnceWithoutSendingAndCanBeConfiguredBeforeFirstPrompt() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let home = NewConversationDraft(directory: root.appendingPathComponent("home"))
        let runtime = HomeConversationRuntime()
        let store = Store(draftStore: ConversationDraftStore(directory: root.appendingPathComponent("drafts")),
            createdConversations: CreatedConversationCatalog(directory: root.appendingPathComponent("catalog")),
            newConversationDraft: home, makeConversation: { try await runtime.create($0) })
        #expect(!store.canStartConversation)
        #expect(store.canPrepareConversation)
        await store.startConversationFromHome(sendImmediately: false, preferences: .init())
        #expect(await runtime.creations.count == 1)
        #expect(await runtime.prompts.isEmpty)
        let session = try #require(store.selected)
        let conversation = try #require(store.liveConversations.sessions[session.id])
        #expect(conversation.canChangeSettings)
        #expect(home.value.createdSessionID == nil)
        conversation.draft = "Only the explicit send runs this"
        await conversation.send()
        #expect(await runtime.prompts == ["Only the explicit send runs this"])
        await store.liveConversations.disconnectAll()
    }

    @Test func providerFailureReusesPreparedWorktreeAndKeepsPrompt() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let projectRoot = root.appendingPathComponent("project")
        try FileManager.default.createDirectory(at: projectRoot, withIntermediateDirectories: true)
        let run = CommandRun()
        func git(_ arguments: [String]) throws {
            _ = try run.execute(executable: URL(fileURLWithPath: "/usr/bin/git"),
                arguments: ["-C", projectRoot.path] + arguments, timeout: 10)
        }
        try git(["init", "-b", "main"])
        try git(["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "Initial"])
        let projects = LocalProjects()
        let project = try projects.save(name: "Project", directory: projectRoot, defaultWorkspace: .newWorktree, defaultBaseRef: "main")
        let home = NewConversationDraft(directory: root.appendingPathComponent("home"))
        home.selectProject(project)
        home.value.text = "Retain this request"
        let runtime = HomeConversationRuntime()
        await runtime.failNextCreation()
        let store = Store(localProjects: projects, newConversationDraft: home,
            workspaceClient: ConversationWorkspaceClient(managedRoot: root.appendingPathComponent("worktrees")),
            makeConversation: { try await runtime.create($0) })
        await store.startConversationFromHome()
        #expect(store.newConversationError != nil)
        let prepared = try #require(home.value.preparedWorkspace)
        #expect(prepared.state == .ready)
        #expect(home.value.text == "Retain this request")
        #expect(NewConversationDraft(directory: root.appendingPathComponent("home")).value.preparedWorkspace == prepared)
        await store.startConversationFromHome()
        #expect(await runtime.creations == [prepared.workingDirectory, prepared.workingDirectory])
        #expect(await runtime.prompts == ["Retain this request"])
        #expect(try FileManager.default.contentsOfDirectory(atPath: root.appendingPathComponent("worktrees").path).count == 1)
        await store.liveConversations.disconnectAll()
    }

    @Test func failedDraftSaveKeepsCreatedIdentityAndRecoveryNeverResends() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let projects = LocalProjects()
        let project = try projects.save(name: "Project", directory: root)
        let homeRoot = root.appendingPathComponent("home")
        let home = NewConversationDraft(directory: homeRoot)
        home.selectProject(project)
        home.value.text = "Saved in Home"
        let draftsRoot = root.appendingPathComponent("drafts")
        let drafts = ConversationDraftStore(directory: draftsRoot)
        // A storage failure after initialization is recoverable without recreating the chat.
        try Data("blocking file".utf8).write(to: draftsRoot)
        let runtime = HomeConversationRuntime()
        let store = Store(draftStore: drafts, localProjects: projects, newConversationDraft: home,
            makeConversation: { try await runtime.create($0) })
        await store.startConversationFromHome()
        #expect(await runtime.creations.count == 1)
        #expect(await runtime.prompts.isEmpty)
        #expect(home.value.createdSessionID != nil)
        #expect(NewConversationDraft(directory: homeRoot).value.createdSessionID == home.value.createdSessionID)
        #expect(!store.canStartConversation)
        await store.startConversationFromHome()
        #expect(await runtime.creations.count == 1)
        try FileManager.default.removeItem(at: draftsRoot)
        await store.openCreatedConversationFromHome()
        #expect(store.newConversationError == nil)
        #expect(store.selectedLiveConversation?.draft == "Saved in Home")
        #expect(home.value.createdSessionID == nil)
        #expect(await runtime.prompts.isEmpty)
        await store.liveConversations.disconnectAll()
    }

    @Test func failedHomeClearPreservesRecoveryMarkerAndUnreadableDraftIsNotReplaced() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let homeRoot = root.appendingPathComponent("home")
        let home = NewConversationDraft(directory: homeRoot)
        home.value.text = "Pending transfer"
        home.value.createdSessionID = "exact-native-identity"
        await home.flush()
        try FileManager.default.removeItem(at: homeRoot)
        try Data("blocking file".utf8).write(to: homeRoot)
        await #expect(throws: ConversationRuntimeError.self) { try await home.finishTransfer() }
        #expect(home.value.createdSessionID == "exact-native-identity")
        #expect(home.value.text == "Pending transfer")
        await home.flush()
        let corrupt = NewConversationDraft(directory: homeRoot)
        corrupt.value.text = "New draft"
        await corrupt.flush()
        #expect(corrupt.error != nil)
        #expect(try Data(contentsOf: homeRoot) == Data("blocking file".utf8))
    }

    @Test func projectlessRetryAfterRelaunchReusesSavedFolderAndIgnoresBrokenProjectCatalog() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let projectRoot = root.appendingPathComponent("projects")
        try FileManager.default.createDirectory(at: projectRoot, withIntermediateDirectories: true)
        let corrupt = Data("corrupt project catalog".utf8)
        try corrupt.write(to: projectRoot.appendingPathComponent("projects.json"))
        let projects = LocalProjects(directory: projectRoot)
        #expect(projects.error != nil)
        let homeRoot = root.appendingPathComponent("home")
        let home = NewConversationDraft(directory: homeRoot)
        home.selectNoProject()
        home.value.text = "Work without a saved project"
        let runtime = HomeConversationRuntime()
        await runtime.failNextCreation()
        let client = ConversationWorkspaceClient(temporaryRoot: root.appendingPathComponent("chat-folders"))
        let store = Store(localProjects: projects, newConversationDraft: home, workspaceClient: client,
            makeConversation: { request in
                // The provider is never called before the folder identity reaches disk.
                let saved = await MainActor.run { NewConversationDraft(directory: homeRoot).value }
                #expect(saved.preparedWorkspace?.workingDirectory == request.workingDirectory)
                return try await runtime.create(request)
            })
        #expect(store.canStartConversation)
        await store.startConversationFromHome()
        #expect(store.newConversationError != nil)
        let prepared = try #require(home.value.preparedWorkspace)
        try Data("user file".utf8).write(to: prepared.workingDirectory.appendingPathComponent("retained.txt"))
        let reopenedHome = NewConversationDraft(directory: homeRoot)
        #expect(reopenedHome.value.projectID == nil)
        #expect(reopenedHome.value.preparedWorkspace == prepared)
        let catalogRoot = root.appendingPathComponent("catalog")
        let reopened = Store(createdConversations: CreatedConversationCatalog(directory: catalogRoot),
            localProjects: projects, newConversationDraft: reopenedHome, workspaceClient: client,
            makeConversation: { try await runtime.create($0) })
        async let first: Void = reopened.startConversationFromHome()
        async let second: Void = reopened.startConversationFromHome()
        _ = await (first, second)
        #expect(await runtime.creations == [prepared.workingDirectory, prepared.workingDirectory])
        #expect(await runtime.prompts == ["Work without a saved project"])
        let session = try #require(reopened.selected)
        let context = try #require(CreatedConversationCatalog(directory: catalogRoot).contexts[session.id])
        #expect(context.projectID == nil && context.projectName == "No project")
        #expect(context.workspace == prepared)
        #expect(reopened.projectName(for: session) == "No project")
        #expect(session.project == prepared.workingDirectory.lastPathComponent)
        #expect(session.cwd == prepared.workingDirectory.path)
        #expect(try String(contentsOf: prepared.workingDirectory.appendingPathComponent("retained.txt"), encoding: .utf8) == "user file")
        #expect(try FileManager.default.contentsOfDirectory(atPath: client.temporaryRoot.path).count == 1)
        #expect(try Data(contentsOf: projectRoot.appendingPathComponent("projects.json")) == corrupt)
        await reopened.liveConversations.disconnectAll()
    }

    @Test func missingProjectDoesNotBecomeProjectlessAndSelectionProtectsRecovery() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let draft = NewConversationDraft()
        draft.value.projectID = "removed-project"
        draft.value.text = "Retained request"
        draft.value.workspaceMode = .newWorktree
        draft.value.baseRef = "trunk"
        let runtime = HomeConversationRuntime()
        let client = ConversationWorkspaceClient(temporaryRoot: root.appendingPathComponent("chats"))
        let prepared = try await client.prepareTemporaryDirectory()
        draft.value.preparedWorkspace = prepared
        let store = Store(newConversationDraft: draft, workspaceClient: client, makeConversation: { try await runtime.create($0) })
        #expect(!store.canStartConversation)
        await store.startConversationFromHome()
        #expect(await runtime.creations.isEmpty)
        draft.value.createdSessionID = "already-created"
        let recovery = draft.value
        draft.selectNoProject()
        #expect(draft.value == recovery)
        draft.value.createdSessionID = nil
        draft.selectNoProject()
        #expect(draft.value.projectID == nil)
        #expect(draft.value.workspaceMode == .existingDirectory)
        #expect(draft.value.baseRef == nil && draft.value.preparedWorkspace == nil)
        #expect(draft.value.text == "Retained request")
        #expect(store.canStartConversation)
        draft.selectWorkspace(.newWorktree, baseRef: "trunk")
        #expect(draft.value.workspaceMode == .existingDirectory)
        #expect(FileManager.default.fileExists(atPath: prepared.workingDirectory.path))
    }

    @Test func existingSavedProjectContextAndDraftRemainReadable() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let workspace = try await ConversationWorkspaceClient().prepare(directory: root, mode: .existingDirectory)
        let encodedWorkspace = try JSONSerialization.jsonObject(with: JSONEncoder().encode(workspace))
        let catalog: [String: Any] = ["version": 1, "sessions": [], "contexts": ["legacy-session": [
            "projectID": "existing-project", "projectName": "Existing project", "workspace": encodedWorkspace]]]
        try JSONSerialization.data(withJSONObject: catalog).write(to: root.appendingPathComponent("conversations.json"))
        let reopened = CreatedConversationCatalog(directory: root)
        #expect(reopened.error == nil)
        #expect(reopened.contexts["legacy-session"]?.projectID == "existing-project")
        #expect(reopened.contexts["legacy-session"]?.workspace == workspace)
        let draft: [String: Any] = ["version": 1, "value": ["text": "Existing request", "provider": "codex",
            "projectID": "existing-project", "workspaceMode": "existingDirectory", "preparedWorkspace": encodedWorkspace]]
        try JSONSerialization.data(withJSONObject: draft).write(to: root.appendingPathComponent("new-conversation.json"))
        let restored = NewConversationDraft(directory: root)
        #expect(restored.error == nil)
        #expect(restored.value.projectID == "existing-project")
        #expect(restored.value.preparedWorkspace == workspace)
    }

    @Test func projectlessCreatedSessionRecoveryKeepsFolderAndNeverResends() async throws {
        let root = try directory()
        defer { try? FileManager.default.removeItem(at: root) }
        let homeRoot = root.appendingPathComponent("home")
        let home = NewConversationDraft(directory: homeRoot)
        home.value.text = "Retain my projectless request"
        let draftsRoot = root.appendingPathComponent("drafts")
        let drafts = ConversationDraftStore(directory: draftsRoot)
        try Data("blocking file".utf8).write(to: draftsRoot)
        let runtime = HomeConversationRuntime()
        let client = ConversationWorkspaceClient(temporaryRoot: root.appendingPathComponent("chats"))
        let store = Store(draftStore: drafts, newConversationDraft: home, workspaceClient: client,
            makeConversation: { try await runtime.create($0) })
        await store.startConversationFromHome()
        let created = try #require(home.value.createdSessionID)
        let workspace = try #require(home.value.preparedWorkspace)
        #expect(NewConversationDraft(directory: homeRoot).value.createdSessionID == created)
        #expect(await runtime.creations.count == 1)
        #expect(await runtime.prompts.isEmpty)
        home.selectNoProject()
        #expect(home.value.preparedWorkspace == workspace)
        #expect(home.value.createdSessionID == created)
        await store.startConversationFromHome()
        #expect(await runtime.creations.count == 1)
        try FileManager.default.removeItem(at: draftsRoot)
        await store.openCreatedConversationFromHome()
        #expect(store.newConversationError == nil)
        #expect(store.selected?.id == created)
        #expect(store.selectedLiveConversation?.draft == "Retain my projectless request")
        #expect(await runtime.prompts.isEmpty)
        #expect(FileManager.default.fileExists(atPath: workspace.workingDirectory.path))
        #expect(store.createdConversations.contexts[created]?.workspace == workspace)
        await store.liveConversations.disconnectAll()
    }
}
