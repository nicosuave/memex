import Foundation
import Observation

@MainActor @Observable
final class Store {
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
    private var sortTask: Task<Void, Never>?
    private var projectSortWasSelected = false
    let projectCatalog: ProjectCatalog
    var machines: [MachineChoice] = [.local]
    var machineSelection: MachineSelection = .all
    var machineError: String?
    var loadingMachines = false
    var selectedID: String?
    var records: [TranscriptRecord] = []
    var query = ""
    var scope: Scope = .all
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
    private var listGeneration = UUID()
    private var readerGeneration = UUID()
    let client: MemexClient

    init(client: MemexClient = MemexClient(), projectCatalog: ProjectCatalog? = nil, filterPreferences: UserDefaults? = nil) {
        self.client = client
        self.projectCatalog = projectCatalog ?? ProjectCatalog(client: client)
        self.filterPreferences = filterPreferences
        if let data = filterPreferences?.data(forKey: Self.filterPreferencesKey),
           let saved = try? JSONDecoder().decode(ConversationFilters.self, from: data) { filters = saved }
    }

    enum Scope: Hashable {
        case all, project(String)
        var title: String {
            switch self { case .all: "All conversations"; case .project(let value): value }
        }
        var project: String? { if case .project(let value) = self { value } else { nil } }
    }

    var selected: Session? { sessions.first { $0.id == selectedID } }
    var selectedMachineIDs: [String] {
        switch machineSelection {
        case .all: machines.map(\.id)
        case .machine(let id): [id]
        }
    }
    var machineRequestID: String { "\(machineSelection)|\(selectedMachineIDs.joined(separator: "|"))" }
    private var sessionCriteriaID: String { "\(machineRequestID)|\(scope)|\(filters)|\(query)" }
    var requestID: String { "\(sessionCriteriaID)|\(sessionLimit)" }

    var readerAnchorID: String? { query.nilIfBlank == nil ? nil : selected?.searchRecordID }
    var readerStartsAtEnd: Bool { readerAnchorID == nil }
    var readerPositionKey: String {
        // Length-prefixed components avoid collisions with arbitrary query text.
        [selectedID ?? "", query.nilIfBlank ?? "", readerAnchorID ?? ""].map { "\($0.utf8.count):\($0)" }.joined()
    }
    var readerRequestID: String { readerPositionKey }

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

    func refresh() async {
        filterReferenceDate = Date()
        async let sessions: Void = loadSessions()
        async let projects: Void = loadProjects()
        async let machines: Void = loadMachines()
        _ = await (sessions, projects, machines)
        await loadSelectedSessionMetadata()
    }

    func loadMoreSessionsIfNeeded(visibleID: String) {
        guard hasMoreSessions, !loadingSessions,
              sessions.suffix(5).contains(where: { $0.id == visibleID }) else { return }
        // Several rows can appear before the next .task begins. Claim this page
        // synchronously so they cannot all request another page.
        hasMoreSessions = false
        sessionLimit += 200
    }

    func loadSessions() async {
        let criteria = sessionCriteriaID
        if activeSessionCriteria != criteria {
            activeSessionCriteria = criteria
            filterReferenceDate = Date()
        }
        let generation = UUID()
        listGeneration = generation
        loadingSessions = true
        listError = nil
        if sessionMachineScope != machineRequestID {
            sessionMachineScope = machineRequestID
            sessions = []; catalog = []; selectedID = nil
        }
        defer { if listGeneration == generation { loadingSessions = false } }
        let ids = selectedMachineIDs
        let query = query.nilIfBlank
        let project = scope.project
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
            var batches: [String: [Session]] = [:]
            var errors: [String: String] = [:]
            for await batch in group {
                guard listGeneration == generation, sessionCriteriaID == criteria, !Task.isCancelled else { group.cancelAll(); return }
                if let error = batch.error { errors[batch.machine] = "\(batch.machine): \(error)" }
                else { batches[batch.machine] = batch.rows }
                let rows = await mergeMachineSessions(batches: ids.compactMap { batches[$0] }, limit: limit, ranked: query != nil)
                guard listGeneration == generation, sessionCriteriaID == criteria, !Task.isCancelled else { group.cancelAll(); return }
                if query != nil {
                    let metadata = Dictionary(catalog.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
                    sessions = rows.map { row in metadata[row.id].map { row.applyingMetadata($0) } ?? row }
                } else { sessions = rows }
                if scope == .all && query == nil && !filters.isActive { catalog = rows }
                hasMoreSessions = rows.count >= limit
                if !rows.contains(where: { $0.id == selectedID }) { selectedID = rows.first?.id }
                listError = errors.keys.sorted().compactMap { errors[$0] }.joined(separator: "\n").nilIfBlank
            }
        }
    }

    func loadSelectedSessionMetadata() async {
        let generation = UUID()
        sessionMetadataGeneration = generation
        loadingSessionMetadata = false
        sessionMetadataError = nil
        guard let session = selected, session.machineID == "local",
              session.searchRecordID != nil, session.resumeCommand == nil else { return }
        let request = readerRequestID
        loadingSessionMetadata = true
        defer { if sessionMetadataGeneration == generation { loadingSessionMetadata = false } }
        do {
            let detail = try await client.sessionDetails(for: session)
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
            let start = try await client.initialRecordOffset(for: selected, anchor: anchor)
            try Task.checkCancellation()
            guard readerGeneration == generation, readerRequestID == request else { return }
            let page = try await client.records(for: selected, offset: start.offset)
            try Task.checkCancellation()
            guard readerGeneration == generation, readerRequestID == request else { return }
            records = page
            recordsOffset = start.offset
            recordsTotal = start.total
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
            let start: (offset: Int, total: Int)
            if let offset {
                let total: Int
                if loadedReaderKey == key { total = recordsTotal }
                else { total = try await client.recordMetadata(for: selected, offset: 0, limit: 1).total }
                start = (max(0, offset - MemexClient.pageSize / 2), total)
            } else {
                start = try await client.initialRecordOffset(for: selected, anchor: recordID)
            }
            try Task.checkCancellation()
            guard readerGeneration == generation, readerRequestID == request else { return }
            let page = try await client.records(for: selected, offset: start.offset)
            try Task.checkCancellation()
            guard readerGeneration == generation, readerRequestID == request else { return }
            records = page
            recordsOffset = start.offset
            recordsTotal = start.total
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
