import Foundation
import Observation

@MainActor @Observable
final class Store {
    let liveConversations: LiveConversations
    let createdConversations: CreatedConversationCatalog
    let conversationLibrary: ConversationLibrary
    let conversationNotifications: ConversationNotifications
    let conversationRelationships: ConversationRelationships
    let executionHosts: ExecutionHostConnections
    var historyActionInProgress = false
    var historyActionError: String?
    var workspaceCheckpointWarning: String?
    var conversationLibraryScope: ConversationLibrary.Scope = .active
    let localProjects: LocalProjects
    let newConversationDraft: NewConversationDraft
    let workspaceClient: ConversationWorkspaceClient
    @ObservationIgnored let workspaceBrowser = WorkspaceBrowserStore()
    @ObservationIgnored let workspaceBrowserExecution = WorkspaceBrowserExecutionBridge()
    @ObservationIgnored let workspaceTerminals = WorkspaceTerminalStore()
    @ObservationIgnored let makeConversation: @Sendable (NewConversationRequest) async throws -> CreatedConversation
    var sessions: [Session] = []
    var catalog: [Session] = []
    private(set) var projects: [ProjectSummary] = []
    private(set) var projectSort = ProjectSort.recent
    private(set) var loadingProjects = false
    private(set) var projectsError: String?
    private(set) var projectsUpdatedAt: Date?
    private var projectSnapshot: ProjectCatalogSnapshot?
    private var projectGeneration = UUID()
    private var projectLoadScope = ""
    private var sessionMachineScope = ""
    private var sessionBatchesCriteria: String?
    private var sessionBatches: [String: [Session]] = [:]
    private var sortTask: Task<Void, Never>?
    private var projectSortWasSelected = false
    let projectCatalog: ProjectCatalog
    var machines: [MachineChoice] = [.local]
    var machineSelection: MachineSelection = .all
    var machineError: String?
    var loadingMachines = false
    var findConversationRequest = 0
    var showingProjectSetup = false
    var showingExecutionHosts = false
    var executionHostSelection: String?
    var executionHostError: String?
    var addingProject = false
    var sidebarMode: SidebarMode = .projects {
        didSet { filterPreferences?.set(sidebarMode.rawValue, forKey: "sidebar-mode") }
    }
    var startingConversation = false
    var newConversationError: String?
    var showingWorkspaceChanges = false
    private var workspacePanelNavigation: [String: WorkspacePanelNavigation] = [:]
    var workspacePanel: WorkspacePanel {
        get { selectedID.flatMap { workspacePanelNavigation[$0]?.selection } ?? .tools }
        set {
            guard let selectedID else { return }
            var navigation = workspacePanelNavigation[selectedID] ?? WorkspacePanelNavigation()
            navigation.selection = newValue
            if newValue != .tools, !navigation.panels.contains(newValue) { navigation.panels.append(newValue) }
            workspacePanelNavigation[selectedID] = navigation
        }
    }
    var openWorkspacePanels: [WorkspacePanel] {
        selectedID.flatMap { workspacePanelNavigation[$0]?.panels } ?? []
    }
    var showingTerminalDrawer = false
    private(set) var terminalFocusRequest = 0
    private(set) var workspaceChangeReviewRequest = UUID()
    private var workspaceChangeSelections: [String: String] = [:]
    var selectedID: String?
    var records: [TranscriptRecord] = []
    var query = ""
    var homeProject: String?
    var scope: Scope = .home {
        didSet {
            if scope == .home { selectedID = nil }
            else if oldValue == .home && selectedID == nil { selectedID = sessions.first?.id }
        }
    }
    var filters = ConversationFilters.defaults {
        didSet {
            guard filters != oldValue else { return }
            sessionLimit = 200
            filterReferenceDate = Date()
            listGeneration = UUID()
            sessions = []
            selectedID = nil
            hasMoreSessions = false
            if let data = try? JSONEncoder().encode(filters) { filterPreferences?.set(data, forKey: Self.filterPreferencesKey) }
        }
    }
    private let filterPreferences: UserDefaults?
    private static let filterPreferencesKey = "conversation-filters"
    private var filterReferenceDate = Date()
    private var activeSessionCriteria = ""
    var loadingSessions = false
    var loadingHomeActivity = false
    var homeActivityMetric = HomeActivityMetric.sessions
    var homeActivityCache: HomeActivityCache? {
        didSet {
            guard let cache = homeActivityCache else { return }
            homeActivityCaches[cache.criteria] = cache
            homeActivityCacheOrder.removeAll { $0 == cache.criteria }
            homeActivityCacheOrder.append(cache.criteria)
            while homeActivityCacheOrder.count > 8 {
                homeActivityCaches.removeValue(forKey: homeActivityCacheOrder.removeFirst())
            }
        }
    }
    private var homeActivityCaches: [String: HomeActivityCache] = [:]
    private var homeActivityCacheOrder: [String] = []
    var homeActivityGeneration = UUID()
    private var refreshingHome = false
    private var lastHomeRefresh = Date()
    private var lastSessionsLoadedAt = Date()
    var loadingRecords = false
    var listError: String?
    var readerError: String?
    var loadingSessionMetadata = false
    var sessionMetadataError: String?
    private var sessionMetadataGeneration = UUID()
    var hasMoreRecords = false
    private(set) var recordsOffset = 0
    private(set) var hasEarlierRecords = false
    var loadedReaderKey: String?
    private var recordsTotal = 0
    private var failedPageWasEarlier: Bool?
    private struct ReaderWindow {
        let records: [TranscriptRecord]
        let offset: Int
        let total: Int
    }
    private var readerWindows: [String: ReaderWindow] = [:]
    private var readerWindowOrder: [String] = []
    var hasMoreSessions = false
    var sessionLimit = 200
    private var countGeneration = UUID()
    private var countRequestKey: String?
    private var countResultKey: String?
    private var countValue: Int?
    private var countRefresh = 0
    private var activityRefresh = 0
    private var listGeneration = UUID()
    private var readerGeneration = UUID()
    let client: MemexClient

    init(client: MemexClient = MemexClient(), projectCatalog: ProjectCatalog? = nil, filterPreferences: UserDefaults? = nil,
         draftStore: ConversationDraftStore = ConversationDraftStore(), liveConversations: LiveConversations? = nil,
         createdConversations: CreatedConversationCatalog = CreatedConversationCatalog(),
         conversationLibrary: ConversationLibrary = ConversationLibrary(),
         conversationNotifications: ConversationNotifications = ConversationNotifications(),
         conversationRelationships: ConversationRelationships = ConversationRelationships(),
         executionHosts: ExecutionHostConnections = ExecutionHostConnections(),
         localProjects: LocalProjects = LocalProjects(), newConversationDraft: NewConversationDraft = NewConversationDraft(),
         workspaceClient: ConversationWorkspaceClient = ConversationWorkspaceClient(),
         makeConversation: @escaping @Sendable (NewConversationRequest) async throws -> CreatedConversation = { try await NewConversationRuntime.create($0) }) {
        self.liveConversations = liveConversations ?? LiveConversations(drafts: draftStore, executionHosts: executionHosts)
        self.executionHosts = executionHosts
        self.createdConversations = createdConversations
        self.conversationLibrary = conversationLibrary
        self.conversationNotifications = conversationNotifications
        self.conversationRelationships = conversationRelationships
        self.localProjects = localProjects
        self.newConversationDraft = newConversationDraft
        self.workspaceClient = workspaceClient
        self.makeConversation = makeConversation
        self.client = client
        self.projectCatalog = projectCatalog ?? ProjectCatalog(client: client)
        self.filterPreferences = filterPreferences
        if let value = filterPreferences?.string(forKey: "sidebar-mode"), let mode = SidebarMode(rawValue: value) {
            sidebarMode = mode
        }
        if let data = filterPreferences?.data(forKey: Self.filterPreferencesKey),
           let saved = try? JSONDecoder().decode(ConversationFilters.self, from: data) { filters = saved }
        self.liveConversations.onNotificationState = { [weak self] session, state in
            guard let self else { return }
            self.conversationNotifications.receive(self.conversationLibrary.presenting(session), state: state)
        }
        self.liveConversations.beforePrompt = { [weak self] session, command in
            await self?.captureTurnCheckpoint(session: session, turnID: "command:" + command.id, moment: .beforeTurn)
        }
        self.liveConversations.afterTurn = { [weak self] session, snapshot in
            guard let turnID = snapshot.records.last(where: { $0.record.sourceTurnID != nil })?.record.sourceTurnID else { return }
            await self?.captureTurnCheckpoint(session: session, turnID: turnID, moment: .afterTurn)
        }
        self.liveConversations.onTransferQueuedPrompt = { [weak self] source, entry in
            guard let self else { throw ConversationRuntimeError(message: "The workspace closed before the draft could be saved.") }
            try await self.transferQueuedPrompt(source: source, entry: entry)
        }
    }

    enum Scope: Hashable {
        case home, all, project(String)
        var title: String {
            switch self { case .home: "Home"; case .all: "All conversations"; case .project(let value): value }
        }
        var project: String? { if case .project(let value) = self { value } else { nil } }
    }

    enum SidebarMode: String, CaseIterable {
        case projects, recent
        var title: String { self == .projects ? "Group by project" : "Most recent chats" }
    }

    func addNewProject() {
        addingProject = true
        showingProjectSetup = true
    }

    func manageProjects() {
        addingProject = false
        showingProjectSetup = true
    }

    enum WorkspacePanel: String, CaseIterable, Identifiable {
        case tools, changes, files, browser, terminal
        var id: String { rawValue }
        var title: String {
            switch self {
            case .tools: "Tools"
            case .changes: "Changes"
            case .files: "Files"
            case .browser: "Browser"
            case .terminal: "Terminal"
            }
        }
        var symbol: String {
            switch self {
            case .tools: "square.grid.2x2"
            case .changes: "doc.text.magnifyingglass"
            case .files: "folder"
            case .browser: "globe"
            case .terminal: "terminal"
            }
        }
    }

    private struct WorkspacePanelNavigation {
        var selection = WorkspacePanel.tools
        var panels: [WorkspacePanel] = []
    }

    var selected: Session? {
        guard let selectedID,
              let session = sessions.first(where: { $0.id == selectedID })
                ?? conversationLibrary.savedSession(id: selectedID)
                ?? createdConversations.sessions.first(where: { $0.id == selectedID }) else { return nil }
        return conversationLibrary.presenting(session)
    }
    var selectedLiveConversation: LiveConversation? { selectedID.flatMap { liveConversations.sessions[$0] } }
    var selectedWorkspace: URL? {
        guard let selected, canAccessLocalFiles(for: selected), let cwd = selected.cwd?.nilIfBlank,
              cwd.hasPrefix("/") else { return nil }
        return URL(fileURLWithPath: cwd, isDirectory: true)
    }

    var selectedRemoteWorkspace: (connection: ExecutionHostConnection, id: String)? {
        guard let selected, let cwd = selected.cwd?.nilIfBlank,
              let connection = executionHosts.connection(for: selected) else { return nil }
        return (connection, cwd)
    }
    var hasSelectedWorkspace: Bool { selectedWorkspace != nil || selectedRemoteWorkspace != nil }

    func canAccessLocalFiles(for session: Session) -> Bool {
        session.machineID == "local" && liveConversations.sessions[session.id]?.isServerOwned != true
            && executionHosts.connection(for: session) == nil
    }

    func openHostedConversation(_ session: Session, connection: ExecutionHostConnection, conversationID: String) {
        Task {
            do { try await adoptHostedConversation(session, connection: connection, conversationID: conversationID) }
            catch is CancellationError { }
            catch { executionHostError = error.localizedDescription; showingExecutionHosts = true }
        }
    }

    func adoptHostedConversation(_ session: Session, connection: ExecutionHostConnection, conversationID: String) async throws {
        try await liveConversations.refreshHosted(session, connection: connection, conversationID: conversationID)
        guard conversationLibrary.retain(session) else {
            throw ConversationRuntimeError(message: conversationLibrary.error ?? "The updated conversation location could not be saved.")
        }
        if let index = sessions.firstIndex(where: { $0.id == session.id }) { sessions[index] = session }
        else { sessions.insert(session, at: 0) }
        if let index = catalog.firstIndex(where: { $0.id == session.id }) { catalog[index] = session }
        showingExecutionHosts = false
        openNotifiedConversation(session.id)
    }
    var selectedWorkspaceChange: String? {
        selectedWorkspace.flatMap { workspaceChangeSelections[$0.path] }
    }
    func reviewWorkspaceChange(_ path: String?) {
        guard hasSelectedWorkspace else { return }
        if let directory = selectedWorkspace, let path { workspaceChangeSelections[directory.path] = path }
        workspaceChangeReviewRequest = UUID()
        workspacePanel = .changes
        showingWorkspaceChanges = true
    }

    func showWorkspaceBrowser() {
        workspacePanel = .browser
        showingWorkspaceChanges = true
    }

    func selectWorkspacePanel(_ panel: WorkspacePanel) {
        workspacePanel = panel
        if panel == .terminal {
            showingTerminalDrawer = false
            terminalFocusRequest += 1
        }
    }

    func closeWorkspacePanel(_ panel: WorkspacePanel) {
        guard let selectedID, var navigation = workspacePanelNavigation[selectedID],
              let index = navigation.panels.firstIndex(of: panel) else { return }
        navigation.panels.remove(at: index)
        let wasSelected = navigation.selection == panel
        if wasSelected { navigation.selection = navigation.panels.last ?? .tools }
        workspacePanelNavigation[selectedID] = navigation
        if wasSelected { selectWorkspacePanel(navigation.selection) }
    }

    func toggleWorkspacePanel() {
        showingWorkspaceChanges.toggle()
        if showingWorkspaceChanges && workspacePanel == .terminal {
            showingTerminalDrawer = false
            terminalFocusRequest += 1
        }
    }

    func showWorkspaceTerminal() {
        guard hasSelectedWorkspace else { return }
        selectWorkspacePanel(.terminal)
        showingWorkspaceChanges = true
    }

    func toggleTerminalDrawer() {
        if showingTerminalDrawer {
            showingTerminalDrawer = false
        } else {
            guard hasSelectedWorkspace else { return }
            if selectedRemoteWorkspace != nil {
                showWorkspaceTerminal()
                return
            }
            // A terminal has one native surface. Move it out of the inspector
            // rather than hosting the same shell in two places at once.
            if workspacePanel == .terminal { showingWorkspaceChanges = false }
            showingTerminalDrawer = true
            terminalFocusRequest += 1
        }
    }

    func beginNewConversation(project: LocalProject? = nil) {
        if let project { newConversationDraft.selectProject(project) }
        scope = .home
        query = ""
        newConversationDraft.focusRequest += 1
    }
    var selectedMachineIDs: [String] {
        switch machineSelection {
        case .all: machines.map(\.id)
        case .machine(let id): [id]
        }
    }
    var machineRequestID: String { "\(machineSelection)|\(selectedMachineIDs.joined(separator: "|"))" }
    var selectedProject: String? { scope == .home ? homeProject : scope.project }
    private var sessionCriteriaID: String { "\(machineRequestID)|\(selectedProject ?? "")|\(filters)|\(query)" }
    var requestID: String { "\(sessionCriteriaID)|\(sessionLimit)" }
    var sessionCountRequestID: String { "\(sessionCriteriaID)|\(countRefresh)" }
    var homeActivityCriteriaID: String { sessionCriteriaID }
    var homeActivityRequestID: String { "\(sessionCriteriaID)|\(activityRefresh)" }
    func cachedHomeActivity(for criteria: String) -> HomeActivityCache? {
        homeActivityCaches[criteria]
    }
    var sessionTotal: Int? {
        guard countResultKey == sessionCountRequestID, let countValue,
              countValue >= sessions.count else { return nil }
        return countValue
    }
    var sessionCountLabel: String {
        if let total = sessionTotal { return total.formatted() }
        if loadingSessions && sessions.isEmpty { return "Loading…" }
        return "\(sessions.count.formatted())+"
    }
    var sessionCountHelp: String {
        sessionTotal == nil ? "Conversations currently loaded; total unavailable or still loading" :
            "Total matching conversations across the selected machines"
    }

    var readerAnchorID: String? { query.nilIfBlank == nil ? nil : selected?.searchRecordID }
    // An indexed hit names its original source record. Runtime entity/part IDs
    // are a different namespace and cannot safely stand in for that evidence.
    var readerUsesLiveSnapshot: Bool { readerAnchorID == nil && selectedLiveConversation?.hasSnapshot == true }
    var readerTranscriptKey: String { readerUsesLiveSnapshot ? (selectedID ?? "") + ":live" : readerPositionKey }
    var readerStartsAtEnd: Bool { readerAnchorID == nil }
    var readerPositionKey: String {
        // Length-prefixed components avoid collisions with arbitrary query text.
        [selectedID ?? "", query.nilIfBlank ?? "", readerAnchorID ?? ""].map { "\($0.utf8.count):\($0)" }.joined()
    }
    var readerRequestID: String { readerPositionKey }

    func openConversation(_ session: Session) {
        conversationLibrary.markRead([nativeLibrarySession(session)], read: true)
        if scope == .home { scope = homeProject.map(Scope.project) ?? .all }
        selectedID = session.id
    }

    func openNotifiedConversation(_ id: String) {
        guard let session = sessions.first(where: { $0.id == id })
            ?? liveConversations.sessions[id]?.session
            ?? createdConversations.sessions.first(where: { $0.id == id })
            ?? conversationLibrary.savedSession(id: id) else { return }
        let entry = conversationLibrary.entries[id]
        conversationLibraryScope = entry?.removed == true ? .removed : entry?.archived == true ? .archived : .active
        // Reveal the exact identity without changing archive/removal metadata.
        query = ""
        filters = .defaults
        homeProject = nil
        scope = .all
        machineSelection = .machine(session.machineID)
        if !sessions.contains(where: { $0.id == id }) { sessions.insert(session, at: 0) }
        selectedID = id
    }

    @discardableResult
    func createConversation(_ request: NewConversationRequest,
                            context: CreatedConversationCatalog.Context? = nil,
                            initialText: String? = nil, navigate: Bool = true) async throws -> LiveConversation {
        if let error = createdConversations.error { throw ConversationRuntimeError(message: error) }
        let created = try await makeConversation(request)
        // Once creation succeeds, retain its native identity even if adoption or
        // local persistence fails. Retrying Create would create a different chat.
        createdConversations.save(created.session, context: context)
        if let initialText {
            newConversationDraft.value.createdSessionID = created.session.id
            await newConversationDraft.flush()
            liveConversations.drafts.set(.init(text: initialText), for: created.session.id)
            await liveConversations.drafts.flush()
        }
        let conversation = await liveConversations.adopt(created)
        if navigate {
            revealCreatedConversation(created.session)
            conversation.focus()
        }
        return conversation
    }

    func revealCreatedConversation(_ session: Session) {
        query = ""
        filters = .defaults
        scope = .all
        machineSelection = .machine("local")
        sessionMachineScope = machineRequestID
        listGeneration = UUID()
        sessionBatchesCriteria = nil
        sessions.removeAll { $0.id == session.id }
        sessions.insert(session, at: 0)
        catalog.removeAll { $0.id == session.id }
        catalog.append(session)
        selectedID = session.id
    }

    func updateCreatedConversationTitle() {
        guard let session = selected, createdConversations.contains(session),
              let live = selectedLiveConversation,
              let title = Session.openingTitle(live.snapshot.records),
              session.label?.nilIfBlank == nil else { return }
        var updated = session
        updated.label = title
        updated.lastAt = Date().formatted(.iso8601)
        createdConversations.save(updated)
        if let index = sessions.firstIndex(where: { $0.id == session.id }) { sessions[index] = updated }
        catalog.removeAll { $0.id == session.id }
        catalog.append(updated)
    }

    func loadMachines() async {
        guard !loadingMachines else { return }
        loadingMachines = true
        defer { loadingMachines = false }
        if let cached = await projectCatalog.loadMachineCache() { machines = cached }
        do {
            machines = try await projectCatalog.refreshMachines()
            machineError = nil
        } catch is CancellationError {} catch { machineError = error.localizedDescription }
    }

    func loadProjects() async {
        let scope = machineRequestID
        guard !loadingProjects || projectLoadScope != scope else { return }
        let generation = UUID()
        projectGeneration = generation
        if projectLoadScope != scope {
            projects = []; projectSnapshot = nil; projectsUpdatedAt = nil
        }
        projectLoadScope = scope
        loadingProjects = true
        projectsError = nil
        defer { if projectGeneration == generation { loadingProjects = false } }
        let ids = selectedMachineIDs
        if let snapshot = await projectCatalog.loadCaches(machines: ids) {
            guard projectGeneration == generation, !Task.isCancelled else { return }
            if !projectSortWasSelected { projectSort = snapshot.sort }
            applyProjects(snapshot)
        }
        let service = projectCatalog
        await withTaskGroup(of: (String, String?).self) { group in
            for machine in ids {
                group.addTask {
                    do { return (machine, try await service.refresh(machine: machine).cacheWarning) }
                    catch { return (machine, error.localizedDescription) }
                }
            }
            var errors: [String: String] = [:]
            for await (machine, error) in group {
                guard projectGeneration == generation, !Task.isCancelled else { group.cancelAll(); return }
                if let error { errors[machine] = machine == "local" ? error : "\(machine): \(error)" }
                if let snapshot = await service.combinedSnapshot(machines: ids) {
                    guard projectGeneration == generation, !Task.isCancelled else { group.cancelAll(); return }
                    applyProjects(snapshot)
                }
                projectsError = errors.keys.sorted().compactMap { errors[$0] }.joined(separator: "\n").nilIfBlank
            }
        }
    }

    func setProjectSort(_ sort: ProjectSort) {
        projectSortWasSelected = true
        projectSort = sort
        projects = projectSnapshot?[sort] ?? []
        sortTask?.cancel()
        sortTask = Task { await projectCatalog.setSort(sort) }
    }

    private func applyProjects(_ snapshot: ProjectCatalogSnapshot) {
        projectSnapshot = snapshot
        projects = snapshot[projectSort]
        projectsUpdatedAt = snapshot.updatedAt
        projectsError = snapshot.cacheWarning
    }

    func refresh(refreshActivity: Bool = true) async {
        conversationLibrary.reload()
        filterReferenceDate = Date()
        countRefresh += 1
        if refreshActivity { activityRefresh += 1 }
        async let sessions: Void = loadSessions()
        async let projects: Void = loadProjects()
        async let machines: Void = loadMachines()
        async let count: Void = loadSessionCount()
        _ = await (sessions, projects, machines, count)
        await loadSelectedSessionMetadata()
    }

    func refreshHomeIfStale(isVisible: Bool, now: Date = Date()) async {
        guard isVisible, scope == .home, !refreshingHome,
              !loadingSessions, !loadingProjects, !loadingMachines,
              now.timeIntervalSince(lastHomeRefresh) >= 60,
              now.timeIntervalSince(lastSessionsLoadedAt) >= 60 else { return }
        refreshingHome = true
        lastHomeRefresh = now
        defer { refreshingHome = false }
        await refresh(refreshActivity: !loadingHomeActivity)
    }

    func loadMoreSessionsIfNeeded(visibleID: String) {
        guard hasMoreSessions, !loadingSessions,
              sessions.suffix(5).contains(where: { $0.id == visibleID }) else { return }
        // Several rows can appear before the next .task begins. Claim this page
        // synchronously so they cannot all request another page.
        hasMoreSessions = false
        sessionLimit += 200
    }

    private func prepareSessionCriteria() {
        let criteria = sessionCriteriaID
        if activeSessionCriteria != criteria {
            activeSessionCriteria = criteria
            filterReferenceDate = Date()
        }
    }

    func loadSessionCount() async {
        prepareSessionCriteria()
        let request = sessionCountRequestID
        guard countRequestKey != request else { return }
        let generation = UUID()
        countGeneration = generation
        countRequestKey = request
        countValue = nil
        countResultKey = nil
        defer {
            if Task.isCancelled, countGeneration == generation { countRequestKey = nil }
        }
        let ids = selectedMachineIDs
        let query = query.nilIfBlank
        let project = selectedProject
        let source = filters.provider.argument
        let since = filters.timeframe.since(relativeTo: filterReferenceDate)
        let origin = filters.origin
        let client = client
        if query != nil {
            do { try await Task.sleep(for: .milliseconds(250)) } catch { return }
        }
        let total = await withTaskGroup(of: Int?.self) { group -> Int? in
            for machine in ids {
                group.addTask {
                    try? await client.sessionCount(query: query, project: project, source: source,
                        machine: machine, since: since, origin: origin)
                }
            }
            var sum = 0
            for await count in group {
                guard let count else { group.cancelAll(); return nil }
                let addition = sum.addingReportingOverflow(count)
                guard !addition.overflow else { group.cancelAll(); return nil }
                sum = addition.partialValue
            }
            return ids.isEmpty ? nil : sum
        }
        guard countGeneration == generation, sessionCountRequestID == request, !Task.isCancelled else { return }
        countValue = total
        countResultKey = request
    }

    func loadSessions() async {
        prepareSessionCriteria()
        let criteria = sessionCriteriaID
        if sessionBatchesCriteria != criteria {
            sessionBatchesCriteria = criteria
            sessionBatches = [:]
        }
        let previousBatches = sessionBatches
        let generation = UUID()
        listGeneration = generation
        loadingSessions = true
        listError = nil
        if sessionMachineScope != machineRequestID {
            sessionMachineScope = machineRequestID
            sessions = []; catalog = []; selectedID = nil
        }
        defer {
            if listGeneration == generation {
                loadingSessions = false
                if !Task.isCancelled { lastSessionsLoadedAt = Date() }
            }
        }
        let ids = selectedMachineIDs
        let query = query.nilIfBlank
        let project = selectedProject
        let source = filters.provider.argument
        let origin = filters.origin
        let since = filters.timeframe.since(relativeTo: filterReferenceDate)
        let limit = sessionLimit
        let known = catalog
        let client = client
        if query != nil {
            do { try await Task.sleep(for: .milliseconds(250)) } catch { return }
        }
        await withTaskGroup(of: MachineSessionBatch.self) { group in
            for machine in ids {
                group.addTask {
                    await fetchMachineSessions(client: client, machine: machine, query: query,
                        project: project, source: source, since: since, origin: origin, limit: limit, known: known)
                }
            }
            // Keep each peer's previous page until its replacement arrives.
            // Otherwise a fast peer temporarily removes the rows being scrolled
            // on a slower peer, losing the native list's visible-row anchor.
            var batches = previousBatches
            var errors: [String: String] = [:]
            for await batch in group {
                guard listGeneration == generation, sessionCriteriaID == criteria, !Task.isCancelled else { group.cancelAll(); return }
                if let error = batch.error { errors[batch.machine] = "\(batch.machine): \(error)" }
                else { batches[batch.machine] = batch.rows }
                sessionBatches = batches
                let rows = await mergeMachineSessions(batches: ids.compactMap { batches[$0] }, limit: limit, ranked: query != nil)
                guard listGeneration == generation, sessionCriteriaID == criteria, !Task.isCancelled else { group.cancelAll(); return }
                if query != nil {
                    let metadata = Dictionary(catalog.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
                    sessions = rows.map { row in metadata[row.id].map { row.applyingMetadata($0) } ?? row }
                } else { sessions = rows }
                sessions = createdConversations.merging(sessions, machines: ids, project: project,
                    filters: filters, query: query, since: since, limit: limit)
                if query == nil {
                    // Search results need metadata from every previously browsed
                    // filter, including subagents explicitly shown by the user.
                    let refreshedIDs = Set(rows.map(\.id))
                    catalog.removeAll { refreshedIDs.contains($0.id) }
                    catalog.append(contentsOf: rows)
                }
                hasMoreSessions = rows.count >= limit
                if scope != .home && !sessions.contains(where: { $0.id == selectedID }) { selectedID = sessions.first?.id }
                listError = errors.keys.sorted().compactMap { errors[$0] }.joined(separator: "\n").nilIfBlank
            }
        }
    }

    func loadSelectedSessionMetadata() async {
        let generation = UUID()
        sessionMetadataGeneration = generation
        loadingSessionMetadata = false
        sessionMetadataError = nil
        // Created chats already have their native identity and working directory.
        // The CLI cannot supply external resume metadata until it indexes them.
        if let selected, createdConversations.contains(selected), selected.searchRecordID == nil { return }
        guard let session = selected,
              session.label?.nilIfBlank == nil || (session.searchRecordID != nil && session.machineID == "local" && session.resumeCommand == nil) else { return }
        let request = readerRequestID
        loadingSessionMetadata = true
        defer { if sessionMetadataGeneration == generation { loadingSessionMetadata = false } }
        do {
            var detail = try await client.sessionDetails(for: session)
            if detail.label?.nilIfBlank == nil { detail.label = session.label?.nilIfBlank }
            if detail.label?.nilIfBlank == nil {
                let opening = try await client.records(for: session, offset: 0, limit: 16)
                detail.label = Session.openingTitle(opening)
            }
            try Task.checkCancellation()
            guard sessionMetadataGeneration == generation, readerRequestID == request,
                  let index = sessions.firstIndex(where: { $0.id == session.id }) else { return }
            sessions[index] = sessions[index].applyingMetadata(detail)
            catalog.removeAll { $0.id == detail.id }
            catalog.append(detail)
        } catch is CancellationError {} catch {
            if sessionMetadataGeneration == generation, readerRequestID == request {
                sessionMetadataError = error.localizedDescription
            }
        }
    }

    private func cacheReaderWindow() {
        guard let key = loadedReaderKey else { return }
        readerWindows[key] = ReaderWindow(records: records, offset: recordsOffset, total: recordsTotal)
        readerWindowOrder.removeAll { $0 == key }
        readerWindowOrder.append(key)
        while readerWindowOrder.count > 20 { readerWindows.removeValue(forKey: readerWindowOrder.removeFirst()) }
    }

    private func updateRecordBounds() {
        hasEarlierRecords = recordsOffset > 0
        hasMoreRecords = recordsOffset + records.count < recordsTotal
    }

    func loadRecords() async {
        cacheReaderWindow()
        let generation = UUID()
        readerGeneration = generation
        let request = readerRequestID
        let key = readerPositionKey
        records = []
        recordsOffset = 0
        recordsTotal = 0
        loadedReaderKey = nil
        readerError = nil
        failedPageWasEarlier = nil
        hasMoreRecords = false
        hasEarlierRecords = false
        guard let selected else { loadingRecords = false; return }
        if createdConversations.contains(selected), readerAnchorID == nil, InAppAgentRuntime.isAvailable {
            liveConversations.prepare(selected)
            if let live = selectedLiveConversation {
                loadingRecords = true
                defer { if readerGeneration == generation { loadingRecords = false } }
                if !live.hasSnapshot {
                    do {
                        let history = try await NewConversationRuntime.records(for: selected)
                        guard readerGeneration == generation, readerRequestID == request, !Task.isCancelled else { return }
                        live.showHistory(history)
                    } catch {
                        if readerGeneration == generation, readerRequestID == request { readerError = error.localizedDescription }
                    }
                }
                return
            }
        }
        if let window = readerWindows[key] {
            records = window.records
            recordsOffset = window.offset
            recordsTotal = window.total
            loadedReaderKey = key
            updateRecordBounds()
            loadingRecords = false
            return
        }
        loadingRecords = true
        defer { if readerGeneration == generation { loadingRecords = false } }
        let anchor = readerAnchorID
        do {
            let page = try await client.initialRecords(for: selected, anchor: anchor)
            try Task.checkCancellation()
            guard readerGeneration == generation, readerRequestID == request else { return }
            records = page.records
            recordsOffset = page.offset
            recordsTotal = page.total
            loadedReaderKey = key
            updateRecordBounds()
            cacheReaderWindow()
        } catch is CancellationError {} catch {
            if readerGeneration == generation, readerRequestID == request { readerError = error.localizedDescription }
        }
    }

    func revealRecord(_ recordID: String, offset: Int? = nil) async {
        guard let selected else { return }
        if loadedReaderKey == readerPositionKey, records.contains(where: { $0.id == recordID }) { return }
        let generation = UUID()
        readerGeneration = generation
        let request = readerRequestID
        let key = readerPositionKey
        loadingRecords = true
        readerError = nil
        failedPageWasEarlier = nil
        defer { if readerGeneration == generation { loadingRecords = false } }
        do {
            let page: TranscriptPage
            if let offset {
                page = try await client.recordPage(for: selected, offset: max(0, offset - MemexClient.pageSize / 2))
            } else {
                page = try await client.initialRecords(for: selected, anchor: recordID)
            }
            try Task.checkCancellation()
            guard readerGeneration == generation, readerRequestID == request else { return }
            records = page.records
            recordsOffset = page.offset
            recordsTotal = page.total
            loadedReaderKey = key
            updateRecordBounds()
            cacheReaderWindow()
        } catch is CancellationError {} catch {
            if readerGeneration == generation, readerRequestID == request { readerError = error.localizedDescription }
        }
    }

    func retryRecords() async {
        if let earlier = failedPageWasEarlier, loadedReaderKey == readerPositionKey {
            await loadRecordPage(earlier: earlier)
        } else { await loadRecords() }
    }

    func loadEarlierRecords() async { await loadRecordPage(earlier: true) }
    func loadMoreRecords() async { await loadRecordPage(earlier: false) }

    private func loadRecordPage(earlier: Bool) async {
        guard let selected, !loadingRecords, loadedReaderKey == readerPositionKey,
              earlier ? hasEarlierRecords : hasMoreRecords else { return }
        let generation = readerGeneration
        let request = readerRequestID
        let offset = earlier ? max(0, recordsOffset - MemexClient.pageSize) : recordsOffset + records.count
        let limit = earlier ? recordsOffset - offset : MemexClient.pageSize
        failedPageWasEarlier = earlier
        loadingRecords = true
        readerError = nil
        defer { if readerGeneration == generation { loadingRecords = false } }
        do {
            let page = try await client.records(for: selected, offset: offset, limit: limit)
            try Task.checkCancellation()
            guard readerGeneration == generation, readerRequestID == request else { return }
            if earlier {
                records = page + records
                recordsOffset = offset
            } else {
                records += page
                if page.count < limit { recordsTotal = recordsOffset + records.count }
            }
            updateRecordBounds()
            cacheReaderWindow()
        } catch is CancellationError {} catch {
            if readerGeneration == generation, readerRequestID == request { readerError = error.localizedDescription }
        }
    }

}

private struct MachineSessionBatch: Sendable {
    let machine: String
    let rows: [Session]
    let error: String?
}

private func fetchMachineSessions(client: MemexClient, machine: String, query: String?,
    project: String?, source: String?, since: String?, origin: ConversationOrigin, limit: Int, known: [Session]) async -> MachineSessionBatch {
    do {
        let rows: [Session]
        if let query {
            let hits = try await client.search(query, project: project, source: source, limit: limit, machine: machine, since: since, origin: origin)
            let byID = Dictionary(known.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
            var seen = Set<String>()
            rows = hits.map { $0.session(known: byID) }.filter { seen.insert($0.id).inserted }
        } else {
            rows = try await client.sessions(limit: limit, project: project, source: source, machine: machine, since: since, origin: origin)
        }
        return MachineSessionBatch(machine: machine, rows: rows, error: nil)
    } catch { return MachineSessionBatch(machine: machine, rows: [], error: error.localizedDescription) }
}

/// Interleave machine-local search ranks rather than compare BM25 scores from
/// different indexes. Ordinary browsing is globally ordered by last activity.
private func mergeMachineSessions(batches: [[Session]], limit: Int, ranked: Bool) async -> [Session] {
    let plain = ISO8601DateFormatter()
    let fractional = ISO8601DateFormatter()
    fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
    let rows = batches.flatMap { rows in rows.enumerated().map { rank, row in
        (row: row, rank: rank, time: row.lastAt.flatMap { fractional.date(from: $0) ?? plain.date(from: $0) }?.timeIntervalSince1970 ?? -.infinity)
    } }
    return rows.sorted {
        if ranked && $0.rank != $1.rank { return $0.rank < $1.rank }
        return $0.time == $1.time ? $0.row.id < $1.row.id : $0.time > $1.time
    }.prefix(limit).map(\.row)
}
