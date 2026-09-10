import AppKit
import SwiftUI
import Observation

/// AppKit owns transcript scrolling and row reuse. SwiftUI never estimates the
/// height of a lazy transcript row or feeds those estimates back into layout.
struct NativeTranscript: NSViewControllerRepresentable {
    let sessionID: String
    let records: [TranscriptRecord]
    let provider: String
    let hasMore: Bool
    let isLoading: Bool
    let onLoadMore: () -> Void
    var hasEarlier = false
    var startsAtEnd = false
    var anchorID: String?
    var navigation: TranscriptNavigationState? = nil
    var onLoadEarlier: (() -> Void)? = nil
    var findQuery = ""
    var findHit: ConversationFindHit?
    var findGeneration = 0
    var rawTranscript = false
    var isLocalHost = false
    var sourcePath = ""
    var requestedRecordID: String?
    var requestGeneration = 0

    func makeNSViewController(context: Context) -> TranscriptController { TranscriptController() }
    func updateNSViewController(_ controller: TranscriptController, context: Context) {
        controller.update(sessionID: sessionID, records: records, provider: provider,
                          hasMore: hasMore, isLoading: isLoading, onLoadMore: onLoadMore,
                          hasEarlier: hasEarlier, startsAtEnd: startsAtEnd, anchorID: anchorID,
                          navigation: navigation, onLoadEarlier: onLoadEarlier,
                          findQuery: findQuery, findHit: findHit, findGeneration: findGeneration,
                          rawTranscript: rawTranscript, isLocalHost: isLocalHost, sourcePath: sourcePath,
                          requestedRecordID: requestedRecordID, requestGeneration: requestGeneration)
    }
}

/// In-memory reading positions outlive the controller when the selection is empty.
@Observable @MainActor final class TranscriptNavigationState {
    struct Position {
        let recordID: String
        let offset: CGFloat
        let expanded: Set<String>
        var fullBodies: Set<String> = []
        var rawRows: Set<String> = []
        var rowID: String? = nil
    }
    @ObservationIgnored var positions: [String: Position] = [:]
    var visibleRecordID: String?
    @ObservationIgnored private var order: [String] = []

    func save(_ position: Position, for key: String) {
        positions[key] = position
        order.removeAll { $0 == key }
        order.append(key)
        if order.count > 20 { positions.removeValue(forKey: order.removeFirst()) }
    }
}

@MainActor
final class TranscriptController: NSViewController, NSTableViewDataSource, NSTableViewDelegate {
    let table = TranscriptTableView()
    let scrollView = TranscriptScrollView()
    private(set) var rows: [Row] = []
    private var sessionID = ""
    private var records: [TranscriptRecord] = []
    private var groupedItems: [TranscriptItem] = []
    private var provider = ""
    private var expanded = Set<String>()
    private var rawTools = Set<String>()
    private var fullBodies = Set<String>()
    private var rawTranscript = false
    private var isLocalHost = false
    private var sourcePath = ""
    private var appliedRequestGeneration = -1
    private var measurements: [String: Measurement] = [:]
    private var measuredWidth: CGFloat = 0
    private var notifiedWidth: CGFloat = 0
    private var textLayouts: [String: TranscriptTextLayout] = [:]
    private var richLayouts: [String: RichContentView] = [:]
    private var findRecordBodies: [String: String] = [:]
    private var hasMore = false
    private var isLoading = false
    private var pageRequested = false
    private var updatingRows = false
    private var onLoadMore: (() -> Void)?
    private var onLoadEarlier: (() -> Void)?
    private var hasEarlier = false
    private var startsAtEnd = false
    private var anchorID: String?
    private var navigation = TranscriptNavigationState()
    private var needsInitialPosition = true
    private var findQuery = ""
    private var findHit: ConversationFindHit?
    private var findGeneration = 0
    private var appliedFindGeneration: Int?
    private(set) var selectedFindRange: NSRange?
    private weak var selectedFindCell: TranscriptCell?

    enum Row {
        case message(TranscriptRecord)
        case group(TranscriptItem)
        case activity(TranscriptActivity, nested: Bool)
        var id: String {
            switch self {
            case .message(let value): "message:\(value.id)"
            case .group(let value): "group:\(value.id)"
            case .activity(let value, _): "activity:\(value.id)"
            }
        }
        var records: [TranscriptRecord] {
            switch self {
            case .message(let value): [value]
            case .group(let value): value.records
            case .activity(let value, _): value.records
            }
        }
    }

    struct Measurement {
        let rowID: String
        let title: String
        let body: String
        let attributedBody: NSAttributedString
        let font: NSFont
        let textHeight: CGFloat
        let height: CGFloat
        let contentX: CGFloat
        let contentWidth: CGFloat
        let isUser: Bool
        let isDisclosure: Bool
        let isExpanded: Bool
        let showsRawControl: Bool
        let showsRaw: Bool
        let finding: Bool
        let symbolName: String?
        let hasFailure: Bool
        var isLong = false
        var showsFullBody = false
        var fullTextHeight: CGFloat = 0
        var originalRecords: [TranscriptRecord] = []
        var originalBody: String { originalRecords.map { $0.rawTranscriptBody }.joined(separator: "\n\n") }
        var richContent: RichContentView?
        var isLocalHost = false
        var hasBody: Bool { !body.isEmpty || richContent != nil }
    }

    override func loadView() {
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.drawsBackground = false
        table.headerView = nil
        table.backgroundColor = .clear
        table.intercellSpacing = .zero
        table.selectionHighlightStyle = .none
        table.focusRingType = .none
        table.usesAutomaticRowHeights = false
        table.columnAutoresizingStyle = .uniformColumnAutoresizingStyle
        table.style = .plain
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("transcript"))
        column.minWidth = 100
        column.width = 700
        column.resizingMask = .autoresizingMask
        table.addTableColumn(column)
        table.delegate = self
        table.dataSource = self
        scrollView.documentView = table
        scrollView.onScroll = { [weak self] in self?.loadNextPageIfNeeded() }
        scrollView.contentView.postsBoundsChangedNotifications = true
        NotificationCenter.default.addObserver(self, selector: #selector(scrolled),
            name: NSView.boundsDidChangeNotification, object: scrollView.contentView)
        for name in [NSWindow.didBecomeKeyNotification, NSWindow.didResignKeyNotification] {
            NotificationCenter.default.addObserver(self, selector: #selector(refreshVisibleActions), name: name, object: nil)
        }
        view = scrollView
    }

    func update(sessionID: String, records: [TranscriptRecord], provider: String,
                hasMore: Bool = false, isLoading: Bool = false, onLoadMore: (() -> Void)? = nil,
                hasEarlier: Bool = false, startsAtEnd: Bool = false, anchorID: String? = nil,
                navigation: TranscriptNavigationState? = nil, onLoadEarlier: (() -> Void)? = nil,
                findQuery: String = "", findHit: ConversationFindHit? = nil, findGeneration: Int = 0,
                rawTranscript: Bool = false, isLocalHost: Bool = false, sourcePath: String = "",
                requestedRecordID: String? = nil, requestGeneration: Int = 0) {
        _ = view
        let changedSession = self.sessionID != sessionID
        let changedMode = self.rawTranscript != rawTranscript
        self.rawTranscript = rawTranscript
        self.isLocalHost = isLocalHost
        self.sourcePath = sourcePath
        let changedQuery = self.findQuery != findQuery
        let changedFind = self.findQuery != findQuery || self.findHit != findHit || self.findGeneration != findGeneration
        self.findQuery = findQuery
        self.findHit = findHit
        self.findGeneration = findGeneration
        if changedFind || changedSession {
            appliedFindGeneration = nil
            selectedFindRange = nil
            selectedFindCell?.clearFindSelection()
            selectedFindCell = nil
        }
        if changedQuery {
            measurements.removeAll(keepingCapacity: true)
            textLayouts.removeAll(keepingCapacity: true)
        }
        defer {
            applyFindPosition()
            if let requestedRecordID, appliedRequestGeneration != requestGeneration,
               let row = rowIndex(for: requestedRecordID) {
                table.scrollRowToVisible(row)
                appliedRequestGeneration = requestGeneration
                savePosition()
            }
            savePosition()
        }
        if changedSession { savePosition() }
        if let navigation { self.navigation = navigation }
        self.hasEarlier = hasEarlier
        self.startsAtEnd = startsAtEnd
        self.anchorID = anchorID
        self.onLoadEarlier = onLoadEarlier
        if changedSession || records.count != self.records.count || (self.isLoading && !isLoading) {
            pageRequested = false
        }
        self.hasMore = hasMore
        self.isLoading = isLoading
        self.onLoadMore = onLoadMore
        guard changedSession || changedMode || changedFind || self.records != records || self.provider != provider else {
            if changedQuery { table.reloadData() }
            return
        }
        updatingRows = true
        defer { updatingRows = false }
        let appendOnly = !changedSession && self.provider == provider && records.starts(with: self.records)
        let prependOnly = !changedSession && self.provider == provider
            && records.suffix(self.records.count).elementsEqual(self.records)
        let oldOrigin = scrollView.contentView.bounds.origin
        let visiblePosition = changedSession ? nil : currentPosition()
        if appendOnly, self.records != records, let last = rows.last {
            // The final tool/instruction group may gain records across pages.
            measurements.removeValue(forKey: last.id)
            textLayouts.removeValue(forKey: last.id)
            richLayouts.removeValue(forKey: last.id)
        }
        findRecordBodies.removeAll(keepingCapacity: true)
        self.sessionID = sessionID
        self.records = records
        self.provider = provider
        if changedSession {
            table.minimumDocumentHeight = 0
            needsInitialPosition = true
            rawTools = self.navigation.positions[sessionID]?.rawRows ?? []
            fullBodies = self.navigation.positions[sessionID]?.fullBodies ?? []
            appliedRequestGeneration = -1
            expanded = self.navigation.positions[sessionID]?.expanded ?? []
        }
        if needsInitialPosition, self.navigation.positions[sessionID] == nil, let anchorID,
           let item = TranscriptItem.group(records).first(where: { $0.records.contains { $0.id == anchorID } }),
           item.isActivity || item.isInstructions {
            expanded.insert("group:\(item.id)")
            if let activity = item.activities.first(where: { $0.records.contains { $0.id == anchorID } }) {
                expanded.insert("activity:\(activity.id)")
            }
        }
        // Both paging directions retain unchanged layouts; rebuildRows invalidates
        // boundary groups whose records or nesting changed.
        rebuildRows(resetMeasurements: !(appendOnly || prependOnly) || changedMode || changedQuery)
        if needsInitialPosition {
            applyInitialPosition()
        } else if let visiblePosition {
            restore(visiblePosition)
        } else {
            scrollView.contentView.scroll(to: oldOrigin)
            scrollView.reflectScrolledClipView(scrollView.contentView)
        }
    }

    private func applyFindPosition() {
        guard !findQuery.isEmpty, let hit = findHit, appliedFindGeneration != findGeneration,
              records.contains(where: { $0.id == hit.recordID }) else { return }
        let wasUpdating = updatingRows
        updatingRows = true
        defer { updatingRows = wasUpdating }
        var opened = Set<String>()
        if let item = TranscriptItem.group(records).first(where: { $0.records.contains { $0.id == hit.recordID } }) {
            var disclosureIDs: [String] = []
            if item.isActivity || item.isInstructions {
                disclosureIDs.append("group:\(item.id)")
                if let activity = item.activities.first(where: { $0.records.contains { $0.id == hit.recordID } }) {
                    disclosureIDs.append("activity:\(activity.id)")
                }
            } else if !["user", "assistant"].contains(item.records[0].record.role) {
                disclosureIDs.append("message:\(hit.recordID)")
            }
            for id in disclosureIDs where expanded.insert(id).inserted { opened.insert(id) }
        }
        if !opened.isEmpty {
            for id in opened {
                measurements.removeValue(forKey: id)
                textLayouts.removeValue(forKey: id)
            }
            rebuildRows()
        }
        guard let row = rowIndex(for: hit.recordID) else { return }
        needsInitialPosition = false
        let value = measurement(at: row)
        // Locate each record inside the displayed logical row. Input/Output
        // labels added by pairing are not searchable transcript occurrences.
        let rendered = value.attributedBody.string as NSString
        var searchOffset = 0
        selectedFindRange = nil
        for record in rows[row].records {
            let body: String
            if rows[row].records.count == 1 {
                body = value.attributedBody.string
            } else if let cached = findRecordBodies[record.id] {
                body = cached
            } else {
                body = record.record.isActivity && !record.record.isInstruction && record.record.role != "reasoning"
                    ? ToolContentRenderer.render([record], raw: true).string
                    : TranscriptTextLayout(text: ConversationMatcher.body(record), font: value.font).attributedText.string
                findRecordBodies[record.id] = body
            }
            let section = rendered.range(of: body, options: .literal,
                                         range: NSRange(location: searchOffset, length: rendered.length - searchOffset))
            guard section.location != NSNotFound else { continue }
            if record.id == hit.recordID {
                let matches = ConversationMatcher.ranges(in: body, query: findQuery)
                // Markdown can hide source matches (for example a link target).
                // If occurrence counts differ, reveal the message rather than
                // falsely selecting another visible occurrence of the same word.
                let sourceCount = ConversationMatcher.ranges(in: ConversationMatcher.body(record), query: findQuery).count
                if matches.count == sourceCount, matches.indices.contains(hit.occurrence) {
                    let range = matches[hit.occurrence]
                    selectedFindRange = NSRange(location: section.location + range.location, length: range.length)
                }
                break
            }
            searchOffset = NSMaxRange(section)
        }
        table.scrollRowToVisible(row)
        if let cell = table.view(atColumn: 0, row: row, makeIfNecessary: true) as? TranscriptCell,
           let range = selectedFindRange {
            cell.layoutSubtreeIfNeeded()
            cell.reveal(range)
            selectedFindCell = cell
        }
        appliedFindGeneration = findGeneration
        savePosition()
    }

    private func recordID(at row: Int) -> String { rows[row].records[0].id }

    private func rowIndex(for recordID: String) -> Int? {
        // Prefer an expanded logical operation over its outer summary.
        rows.firstIndex { row in
            if case .group = row { return false }
            return row.records.contains { $0.id == recordID }
        } ?? rows.firstIndex { $0.records.contains { $0.id == recordID } }
          ?? rows.firstIndex { $0.records.contains { $0.sourceID == recordID } }
    }

    private func currentPosition() -> TranscriptNavigationState.Position? {
        guard !needsInitialPosition, !rows.isEmpty else { return nil }
        // Rubber-banding can put the clip origin above the first row while
        // an earlier page arrives. Anchor to that row, not an invalid hit-test
        // that would restore the old pixel origin into the newly prepended page.
        let y = max(0, scrollView.contentView.bounds.minY)
        let row = table.row(at: NSPoint(x: 1, y: y + 1))
        guard rows.indices.contains(row) else { return nil }
        return .init(recordID: rows[row].records[0].sourceID, offset: y - table.rect(ofRow: row).minY,
                     expanded: expanded, fullBodies: fullBodies, rawRows: rawTools, rowID: rows[row].id)
    }

    private func savePosition() {
        if let position = currentPosition() {
            navigation.save(position, for: sessionID)
            let navigation = navigation
            Task { @MainActor in
                if navigation.visibleRecordID != position.recordID { navigation.visibleRecordID = position.recordID }
            }
        }
    }

    private func restore(_ position: TranscriptNavigationState.Position) {
        guard let row = rows.firstIndex(where: { $0.id == position.rowID }) ?? rowIndex(for: position.recordID) else { return }
        scrollView.contentView.scroll(to: NSPoint(x: 0, y: max(0, table.rect(ofRow: row).minY + position.offset)))
        scrollView.reflectScrolledClipView(scrollView.contentView)
    }

    private func applyInitialPosition() {
        guard needsInitialPosition, !rows.isEmpty, scrollView.contentView.bounds.height > 0 else { return }
        needsInitialPosition = false
        let wasUpdating = updatingRows
        updatingRows = true
        defer { updatingRows = wasUpdating }
        if let saved = navigation.positions[sessionID] {
            restore(saved)
        } else if let anchorID, let row = rowIndex(for: anchorID) {
            scrollView.contentView.scroll(to: NSPoint(x: 0, y: table.rect(ofRow: row).minY))
        } else if startsAtEnd {
            let end = table.rect(ofRow: rows.count - 1).maxY
            scrollView.contentView.scroll(to: NSPoint(x: 0, y: max(0, end - scrollView.contentView.bounds.height)))
        } else {
            scrollView.contentView.scroll(to: .zero)
        }
        scrollView.reflectScrolledClipView(scrollView.contentView)
    }

    private func rebuildRows(resetMeasurements: Bool = false, regroup: Bool = true) {
        if regroup {
            groupedItems = rawTranscript ? [] : TranscriptItem.group(records).map { entry in
                var item = entry
                if item.isActivity || item.isInstructions { item.groupedActivities = item.activities }
                return item
            }
        }
        let previous = Dictionary(rows.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
        let openedRecords = Set(rows.filter {
            if case .activity = $0 { return expanded.contains($0.id) }
            return false
        }.flatMap(\.records).map(\.id))
        let openedSingletons = Set(rows.filter {
            if case .activity(_, false) = $0 { return expanded.contains($0.id) }
            return false
        }.flatMap(\.records).map(\.id))
        let openedGroupRecords = Set(rows.filter {
            if case .group = $0 { return expanded.contains($0.id) }
            return false
        }.flatMap(\.records).map(\.id))
        rows = rawTranscript ? records.map { .message($0) } : groupedItems.flatMap { item -> [Row] in
            guard item.isActivity || item.isInstructions else { return [.message(item.records[0])] }
            let activities = item.activities
            for activity in activities where activity.records.contains(where: { openedRecords.contains($0.id) }) {
                expanded.insert("activity:\(activity.id)")
            }
            if activities.count == 1 && !item.isCompletedWork { return [.activity(activities[0], nested: false)] }
            let group = Row.group(item)
            if item.records.contains(where: { openedSingletons.contains($0.id) || openedGroupRecords.contains($0.id) }) {
                expanded.insert(group.id)
            }
            return expanded.contains(group.id) && !rawTools.contains(group.id)
                ? [group] + activities.map {
                    item.isCompletedWork && $0.records.count == 1 && $0.records[0].record.role == "assistant"
                        ? .message($0.records[0]) : .activity($0, nested: true)
                } : [group]
        }
        // Searching injected text reveals the original mixed record once, so a
        // source occurrence cannot be confused with a projected request segment.
        if !rawTranscript, let hit = findHit, !findQuery.isEmpty,
           let original = records.first(where: { $0.id == hit.recordID }),
           TranscriptPresentation.project([original]) != [original] {
            let insertion = rows.firstIndex { $0.records.contains { $0.sourceID == hit.recordID } } ?? rows.count
            rows = rows.compactMap { row in
                let remaining = row.records.filter { $0.sourceID != hit.recordID }
                if remaining.count == row.records.count { return row }
                if remaining.isEmpty { return nil }
                switch row {
                case .group: return .group(TranscriptItem(records: remaining))
                case .activity(_, let nested): return .activity(TranscriptActivity(records: remaining), nested: nested)
                case .message: return nil
                }
            }
            rows.insert(.message(original), at: min(insertion, rows.count))
        }
        if resetMeasurements {
            measurements.removeAll(keepingCapacity: true)
            textLayouts.removeAll(keepingCapacity: true)
            richLayouts.removeAll(keepingCapacity: true)
        } else {
            // A result can join an already visible call after paging, and a
            // singleton can become nested. Retain only unchanged row layouts.
            for row in rows {
                if (previous[row.id]?.records ?? measurements[row.id]?.originalRecords) != row.records {
                    measurements.removeValue(forKey: row.id)
                    textLayouts.removeValue(forKey: row.id)
                    richLayouts.removeValue(forKey: row.id)
                } else if case .activity(_, let nested) = row {
                    if case .activity(_, let wasNested)? = previous[row.id], nested != wasNested {
                        measurements.removeValue(forKey: row.id)
                    } else if let measurement = measurements[row.id], measurement.contentX != (nested ? 50 : 30) {
                        measurements.removeValue(forKey: row.id)
                    }
                }
            }
        }
        table.reloadData()
    }

    func toggle(_ id: String) {
        updateDisclosure(id, resetText: false) {
            if expanded.contains(id) { expanded.remove(id) } else { expanded.insert(id) }
        }
    }

    func toggleRaw(_ id: String) {
        updateDisclosure(id) {
            if rawTools.contains(id) { rawTools.remove(id) } else { rawTools.insert(id) }
            expanded.insert(id)
        }
    }

    func toggleFullBody(_ id: String) {
        updateDisclosure(id, resetText: false) {
            if fullBodies.contains(id) { fullBodies.remove(id) } else { fullBodies.insert(id) }
        }
    }

    private func updateDisclosure(_ id: String, resetText: Bool = true, change: () -> Void) {
        guard let row = rows.firstIndex(where: { $0.id == id }) else { return }
        view.layoutSubtreeIfNeeded()
        // Anchor the clicked header, not the first record in the viewport: a
        // group and its first expanded operation can share that record ID.
        let offset = table.rect(ofRow: row).minY - scrollView.contentView.bounds.minY
        let wasUpdating = updatingRows
        updatingRows = true
        defer {
            updatingRows = wasUpdating
            savePosition()
        }
        change()
        // Collapsing the final row must not clamp the clip view upward. Keep
        // only enough trailing space to retain this viewport; scrolling upward
        // releases it again without moving the reader's content.
        table.minimumDocumentHeight = scrollView.contentView.bounds.maxY
        measurements.removeValue(forKey: id)
        if resetText { textLayouts.removeValue(forKey: id) }
        // A tool's formatted contents do not change when hidden or shown. Keep
        // its rich view, and refresh only this row unless a group changes membership.
        if case .group = rows[row] {
            rebuildRows(regroup: false)
        } else {
            NSAnimationContext.runAnimationGroup { context in
                context.duration = 0
                table.noteHeightOfRows(withIndexesChanged: IndexSet(integer: row))
            }
            if let cell = table.view(atColumn: 0, row: row, makeIfNecessary: false) as? TranscriptCell {
                configure(cell, row: row)
            }
        }
        view.layoutSubtreeIfNeeded()
        guard let updatedRow = rows.firstIndex(where: { $0.id == id }) else { return }
        scrollView.contentView.scroll(to: NSPoint(x: 0, y: max(0, table.rect(ofRow: updatedRow).minY - offset)))
        scrollView.reflectScrolledClipView(scrollView.contentView)
    }

    func numberOfRows(in tableView: NSTableView) -> Int { rows.count }
    func tableView(_ tableView: NSTableView, shouldSelectRow row: Int) -> Bool { false }
    func tableView(_ tableView: NSTableView, heightOfRow row: Int) -> CGFloat { measurement(at: row).height }

    @objc private func scrolled(_ notification: Notification) {
        refreshVisibleActions()
        if !updatingRows {
            if table.minimumDocumentHeight > scrollView.contentView.bounds.maxY {
                table.minimumDocumentHeight = scrollView.contentView.bounds.maxY
                let contentHeight = rows.isEmpty ? 0 : table.rect(ofRow: rows.count - 1).maxY
                table.setFrameSize(NSSize(width: table.frame.width, height: contentHeight))
            }
            savePosition()
        }
        loadNextPageIfNeeded()
    }

    @objc private func refreshVisibleActions() {
        let visible = table.rows(in: scrollView.documentVisibleRect)
        guard visible.location != NSNotFound else { return }
        for row in visible.location..<min(rows.count, NSMaxRange(visible)) {
            (table.view(atColumn: 0, row: row, makeIfNecessary: false) as? TranscriptCell)?.refreshActions()
        }
    }

    func loadNextPageIfNeeded() {
        guard !updatingRows, !needsInitialPosition, hasMore || hasEarlier, !isLoading, !pageRequested, !records.isEmpty else { return }
        let viewport = scrollView.documentVisibleRect
        guard viewport.height > 0 else { return }
        let threshold = max(200, viewport.height * 0.5)
        if hasEarlier && viewport.minY < threshold {
            pageRequested = true
            onLoadEarlier?()
        } else if hasMore && table.bounds.height - viewport.maxY < threshold {
            pageRequested = true
            onLoadMore?()
        }
    }

    override func viewDidLayout() {
        super.viewDidLayout()
        resizeRowsIfNeeded()
        applyInitialPosition()
    }

    func tableViewColumnDidResize(_ notification: Notification) { resizeRowsIfNeeded() }

    private func resizeRowsIfNeeded() {
        let width = table.bounds.width
        guard abs(width - notifiedWidth) > 0.5 else { return }
        let position = currentPosition()
        notifiedWidth = width
        measuredWidth = width
        measurements.removeAll(keepingCapacity: true)
        guard !rows.isEmpty else { return }
        table.noteHeightOfRows(withIndexesChanged: IndexSet(integersIn: rows.indices))
        let visible = table.rows(in: table.visibleRect)
        guard visible.location != NSNotFound else { return }
        for row in visible.location..<min(rows.count, NSMaxRange(visible)) {
            if let cell = table.view(atColumn: 0, row: row, makeIfNecessary: false) as? TranscriptCell {
                configure(cell, row: row)
            }
        }
        if let position { restore(position) }
    }

    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        let identifier = NSUserInterfaceItemIdentifier("transcript-row")
        let cell = tableView.makeView(withIdentifier: identifier, owner: nil) as? TranscriptCell ?? TranscriptCell()
        cell.identifier = identifier
        configure(cell, row: row)
        return cell
    }

    private func configure(_ cell: TranscriptCell, row: Int) {
        let id = rows[row].id
        cell.configure(measurement(at: row), toggle: { [weak self] in self?.toggle(id) },
                       toggleRaw: { [weak self] in self?.toggleRaw(id) },
                       toggleFull: { [weak self] in self?.toggleFullBody(id) })
    }

    func measurement(at index: Int) -> Measurement {
        let row = rows[index]
        let width = max(200, table.bounds.width)
        if abs(width - measuredWidth) > 0.5 {
            measuredWidth = width
            measurements.removeAll(keepingCapacity: true)
        }
        if let cached = measurements[row.id] { return cached }
        let isExpanded = expanded.contains(row.id)
        let title: String
        var fullText = ""
        let isUser: Bool
        let isDisclosure: Bool
        let isTool: Bool
        let indent: CGFloat
        var symbolName: String?
        var hasFailure = false
        switch row {
        case .group(let item):
            title = item.activitySummary
            symbolName = item.isInstructions ? "doc.text" : "list.bullet"
            isUser = false; isDisclosure = true; isTool = true; indent = 0
        case .activity(let entry, let nested):
            let presentation = entry.presentation
            title = presentation.title
            symbolName = presentation.symbolName
            hasFailure = presentation.needsAttention
            isUser = false; isDisclosure = true
            isTool = entry.records[0].record.isActivity && entry.records[0].record.role != "reasoning"
            indent = nested ? 20 : 0
            if isExpanded { fullText = entry.body }
        case .message(let entry):
            isUser = !rawTranscript && entry.record.role == "user"
            isDisclosure = !rawTranscript && !["user", "assistant"].contains(entry.record.role)
            isTool = false; indent = 0
            if let event = entry.record.lifecycleEvent {
                switch event {
                case "task_complete": title = "Completed"; symbolName = "checkmark.circle"
                case "turn_aborted": title = "Interrupted"; symbolName = "pause.circle"; hasFailure = true
                case "task_started": title = "Turn started"; symbolName = "clock"
                default: title = entry.record.text; symbolName = "info.circle"
                }
            } else {
                title = isUser ? "You" : (entry.record.role == "assistant" ? provider : entry.record.role.capitalized)
                symbolName = isDisclosure ? "doc.text" : nil
            }
            if !isDisclosure || isExpanded { fullText = entry.record.text }
        }
        let rawMessage: Bool
        if case .activity = row { rawMessage = rawTranscript || (rawTools.contains(row.id) && !isTool) }
        else { rawMessage = rawTranscript || rawTools.contains(row.id) }
        if rawMessage { fullText = row.records.map { $0.rawTranscriptBody }.joined(separator: "\n\n") }
        let body = fullText
        let font: NSFont = isTool ? .monospacedSystemFont(ofSize: 12, weight: .regular) : .systemFont(ofSize: 14)
        let available = max(120, min(800, width - 60) - indent)
        let maximumContentWidth = isUser ? available * 0.77 : available
        let showsRaw = rawMessage || rawTools.contains(row.id) || !findQuery.isEmpty
        let attachments = !showsRaw && !isTool ? row.records.flatMap { SourceContent.blocks($0.record) } : []
        let textLayout: TranscriptTextLayout
        var renderedTool: NSAttributedString?
        if body.isEmpty { textLayout = TranscriptTextLayout(text: "", font: font) }
        else if let cached = textLayouts[row.id] { textLayout = cached }
        else if rawMessage {
            textLayout = TranscriptTextLayout(rendered: NSAttributedString(string: body, attributes: [.font: NSFont.monospacedSystemFont(ofSize: 12, weight: .regular), .foregroundColor: NSColor.labelColor]), trimEdges: false)
        } else if isTool && isExpanded && !body.isEmpty {
            let rendered = ToolContentRenderer.render(row.records, raw: showsRaw)
            renderedTool = rendered
            textLayout = TranscriptTextLayout(rendered: rendered, trimEdges: !showsRaw)
        } else if !findQuery.isEmpty {
            // Source-only matches (Markdown destinations and context wrappers)
            // remain selectable at their exact occurrence in plain source text.
            textLayout = TranscriptTextLayout(rendered: NSAttributedString(string: body, attributes: [.font: font, .foregroundColor: NSColor.labelColor]), trimEdges: false)
        } else { textLayout = TranscriptTextLayout(text: body, font: font) }
        if !body.isEmpty { textLayouts[row.id] = textLayout }
        let contentWidth = isUser && attachments.isEmpty
            ? min(maximumContentWidth, max(44, textLayout.width(for: maximumContentWidth - 24) + 24))
            : maximumContentWidth
        let contentX = isUser ? width - 30 - contentWidth : 30 + indent
        let bodyWidth = contentWidth - (isUser || isDisclosure ? 24 : 0)
        var richContent: RichContentView?
        let mayHaveRichBlocks = !PromptSections.hasOpeningSection(body) && (body.contains("```") || body.contains("~~~") || body.contains("![") || body.contains("](/") || body.contains("](file:"))
        if !showsRaw && (!attachments.isEmpty || (!body.isEmpty && (isTool || (mayHaveRichBlocks && RichContentDocument(body).hasRichBlocks)))) {
            if let cached = richLayouts[row.id] { richContent = cached }
            else {
                let view = RichContentView()
                if isTool {
                    view.configure(blocks: ToolContentRenderer.richBlocks(row.records, rendered: renderedTool), font: font, context: RichContentContext(isLocalHost: isLocalHost))
                } else {
                    view.configure(blocks: RichContentDocument(body).blocks + attachments, font: font, context: RichContentContext(isLocalHost: isLocalHost))
                }
                richLayouts[row.id] = view
                richContent = view
            }
        }
        let fullTextHeight = richContent?.height(for: bodyWidth) ?? textLayout.height(for: bodyWidth)
        let isLong = fullTextHeight > 440
        let showsFullBody = fullBodies.contains(row.id) || !findQuery.isEmpty
        let textHeight = isLong && !showsFullBody ? 360 : fullTextHeight
        let hasBody = !body.isEmpty || richContent != nil
        let showsRawControl = isTool && isExpanded && !body.isEmpty && !findQuery.isEmpty
        let bodyY: CGFloat = isDisclosure ? (showsRawControl ? 74 : 44) : 16
        let bodyBottom = bodyY + textHeight + (isUser ? 16 : 0)
        let height: CGFloat = hasBody ? bodyBottom + (isLong ? 26 : 0) + (isDisclosure ? 12 : 0) + 4 + 18 + 10 : 38
        let highlighted: NSAttributedString
        if findQuery.isEmpty {
            highlighted = textLayout.attributedText
        } else {
            let value = NSMutableAttributedString(attributedString: textLayout.attributedText)
            for range in ConversationMatcher.ranges(in: value.string, query: findQuery) {
                value.addAttribute(.backgroundColor, value: NSColor.systemYellow.withAlphaComponent(0.4), range: range)
            }
            highlighted = value
        }
        var result = Measurement(rowID: row.id, title: title, body: body, attributedBody: highlighted, font: font, textHeight: textHeight, height: height,
            contentX: contentX, contentWidth: contentWidth, isUser: isUser, isDisclosure: isDisclosure,
            isExpanded: isExpanded, showsRawControl: showsRawControl, showsRaw: showsRaw, finding: !findQuery.isEmpty,
            symbolName: symbolName, hasFailure: hasFailure)
        result.isLong = isLong
        result.showsFullBody = showsFullBody
        result.fullTextHeight = fullTextHeight
        result.originalRecords = row.records
        result.richContent = richContent
        result.isLocalHost = isLocalHost
        measurements[row.id] = result
        return result
    }
}

/// NSTableView normally shrinks its document to the last row during reload.
/// A disclosure can temporarily retain the current viewport's trailing space.
@MainActor final class TranscriptTableView: NSTableView {
    var minimumDocumentHeight: CGFloat = 0

    override func setFrameSize(_ newSize: NSSize) {
        super.setFrameSize(NSSize(width: newSize.width, height: max(newSize.height, minimumDocumentHeight)))
    }
}

@MainActor
private final class TranscriptCell: NSTableCellView, NSTextViewDelegate {
    private let disclosure = TranscriptDisclosureButton()
    private let rawDisclosure = NSButton()
    private let message = NSTextView()
    private let bubble = NSView()
    private let activityIcon = NSImageView()
    private let detailPanel = NSView()
    private let richClip = TranscriptContentClip()
    private weak var installedRichContent: RichContentView?
    private let showAll = NSButton()
    private let copyButton = TranscriptActionButton()
    private var tracking: NSTrackingArea?
    private var measurement: TranscriptController.Measurement?
    private var onToggle: (() -> Void)?
    private var onToggleRaw: (() -> Void)?
    private var onToggleFull: (() -> Void)?
    private var displayedText: NSAttributedString?
    override var isFlipped: Bool { true }

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        disclosure.cell = TranscriptDisclosureCell()
        disclosure.isBordered = false
        disclosure.alignment = .left
        disclosure.font = .systemFont(ofSize: 13)
        disclosure.imagePosition = .imageRight
        disclosure.contentTintColor = .secondaryLabelColor
        disclosure.target = self
        disclosure.action = #selector(toggle)
        rawDisclosure.isBordered = false
        rawDisclosure.alignment = .left
        rawDisclosure.font = .systemFont(ofSize: 12)
        rawDisclosure.contentTintColor = .secondaryLabelColor
        rawDisclosure.target = self
        rawDisclosure.action = #selector(toggleRaw)
        message.wantsLayer = true
        message.layer?.masksToBounds = true
        message.isEditable = false
        message.isSelectable = true
        message.drawsBackground = false
        message.textContainerInset = .zero
        message.textContainer?.lineFragmentPadding = 0
        message.isVerticallyResizable = false
        message.isHorizontallyResizable = false
        message.textContainer?.widthTracksTextView = true
        message.delegate = self
        bubble.wantsLayer = true
        bubble.layer?.cornerRadius = 16
        detailPanel.wantsLayer = true
        detailPanel.layer?.cornerRadius = 8
        activityIcon.imageScaling = .scaleProportionallyDown
        addSubview(detailPanel)
        addSubview(bubble)
        addSubview(activityIcon)
        addSubview(disclosure)
        addSubview(rawDisclosure)
        addSubview(message)
        richClip.wantsLayer = true
        richClip.layer?.masksToBounds = true
        addSubview(richClip)
        copyButton.image = NSImage(systemSymbolName: "doc.on.doc", accessibilityDescription: "Copy message")
        copyButton.imagePosition = .imageOnly
        copyButton.title = ""
        copyButton.setAccessibilityLabel("Copy message")
        copyButton.toolTip = "Copy message"
        copyButton.isBordered = false
        copyButton.contentTintColor = .secondaryLabelColor
        copyButton.target = self
        copyButton.action = #selector(copyBody)
        copyButton.wantsLayer = true
        copyButton.onFocus = { [weak self] in self?.refreshActions() }
        addSubview(copyButton)
        showAll.isBordered = false
        showAll.alignment = .left
        showAll.font = .systemFont(ofSize: 12)
        showAll.target = self
        showAll.action = #selector(toggleFull)
        addSubview(showAll)
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    func configure(_ value: TranscriptController.Measurement, toggle: @escaping () -> Void, toggleRaw: @escaping () -> Void,
                   toggleFull: @escaping () -> Void) {
        measurement = value
        onToggle = toggle
        onToggleRaw = toggleRaw
        onToggleFull = toggleFull
        showAll.isHidden = !value.isLong
        showAll.title = value.showsFullBody ? "Show less" : "Show all"
        showAll.isEnabled = !value.finding
        refreshActions()
        rawDisclosure.isHidden = !value.showsRawControl
        rawDisclosure.title = value.finding ? "Raw content shown for Find" : (value.showsRaw ? "Show formatted content" : "Show raw content")
        rawDisclosure.isEnabled = !value.finding
        message.setAccessibilityLabel(value.title)
        activityIcon.isHidden = value.isDisclosure || value.symbolName == nil
        activityIcon.image = value.symbolName.flatMap { NSImage(systemSymbolName: $0, accessibilityDescription: nil) }
        activityIcon.contentTintColor = value.hasFailure ? .systemOrange : .tertiaryLabelColor
        disclosure.restingTint = value.hasFailure ? .systemOrange : .secondaryLabelColor
        disclosure.toolTip = value.title
        detailPanel.isHidden = !value.isDisclosure || !value.hasBody
        disclosure.isHidden = !value.isDisclosure
        disclosure.title = value.title
        disclosure.image = NSImage(systemSymbolName: value.isExpanded ? "chevron.down" : "chevron.right",
                                   accessibilityDescription: nil)?.withSymbolConfiguration(
                                    NSImage.SymbolConfiguration(pointSize: 10, weight: .light))
        disclosure.setAccessibilityLabel(value.title)
        disclosure.setAccessibilityValue(value.isExpanded ? "Expanded" : "Collapsed")
        if displayedText !== value.attributedBody {
            let rendered = NSMutableAttributedString(attributedString: value.attributedBody)
            rendered.enumerateAttribute(RichTextRenderer.sourceLocationAttribute, in: NSRange(location: 0, length: rendered.length)) { source, range, _ in
                if value.isLocalHost, let source = source as? String {
                    rendered.addAttributes([.link: source, .foregroundColor: NSColor.linkColor], range: range)
                }
            }
            message.textStorage?.setAttributedString(rendered)
            message.setSelectedRange(NSRange(location: 0, length: 0))
            displayedText = value.attributedBody
        }
        if installedRichContent !== value.richContent {
            installedRichContent?.removeFromSuperview()
            if let rich = value.richContent { richClip.addSubview(rich) }
            installedRichContent = value.richContent
        }
        richClip.isHidden = value.richContent == nil
        message.isHidden = !value.hasBody || value.richContent != nil
        bubble.isHidden = !value.isUser || !value.hasBody
        updateBubbleColor()
        needsLayout = true
        message.alphaValue = 1
    }

    override func viewDidChangeEffectiveAppearance() {
        super.viewDidChangeEffectiveAppearance()
        updateBubbleColor()
    }

    private func updateBubbleColor() {
        effectiveAppearance.performAsCurrentDrawingAppearance {
            bubble.layer?.backgroundColor = NSColor.labelColor.withAlphaComponent(0.05).cgColor
            detailPanel.layer?.backgroundColor = NSColor.labelColor.withAlphaComponent(0.035).cgColor
        }
    }

    override func layout() {
        super.layout()
        guard let value = measurement else { return }
        let x = value.contentX
        let width = value.contentWidth
        activityIcon.frame = NSRect(x: x, y: 12, width: 14, height: 14)
        // Reserve the action gutter before hover so the title and chevron never move.
        let actionGutter: CGFloat = value.isDisclosure ? 32 : 0
        disclosure.frame = NSRect(x: x, y: 8, width: max(0, width - actionGutter), height: 22)
        rawDisclosure.frame = NSRect(x: x + 12, y: 44, width: width - 24, height: 22)
        let bodyY: CGFloat = value.isDisclosure ? (value.showsRawControl ? 74 : 44) : 16
        let inset: CGFloat = value.isUser || value.isDisclosure ? 12 : 0
        let verticalInset: CGFloat = value.isUser ? 8 : 0
        message.frame = NSRect(x: x + inset, y: bodyY + verticalInset, width: width - 2 * inset, height: value.textHeight)
        richClip.frame = message.frame
        installedRichContent?.frame = NSRect(x: 0, y: 0, width: message.frame.width, height: value.fullTextHeight)
        bubble.frame = NSRect(x: x, y: bodyY, width: width, height: value.textHeight + 2 * verticalInset)
        let bodyBottom = message.frame.maxY + verticalInset
        let footerY = bodyBottom + (value.isLong ? 26 : 0) + (value.isDisclosure ? 12 : 0) + 4
        detailPanel.frame = NSRect(x: x, y: 32, width: width, height: max(0, footerY - 4 - 32))
        showAll.frame = NSRect(x: x + inset, y: bodyBottom + 4, width: 100, height: 22)
        let actionY = value.hasBody ? footerY : 10
        copyButton.frame = NSRect(x: x + width - 24, y: actionY - 3, width: 24, height: 24)
        refreshActions()
    }

    func clearFindSelection() { message.setSelectedRange(NSRange(location: 0, length: 0)) }

    func reveal(_ range: NSRange) {
        message.setSelectedRange(range)
        guard let manager = message.layoutManager, let container = message.textContainer else { return }
        manager.ensureLayout(for: container)
        let glyphs = manager.glyphRange(forCharacterRange: range, actualCharacterRange: nil)
        let rect = manager.boundingRect(forGlyphRange: glyphs, in: container)
        // Scroll the enclosing transcript, including matches far down a tall row.
        message.scrollToVisible(rect.insetBy(dx: 0, dy: -30))
    }

    @objc private func toggle() { onToggle?() }
    @objc private func toggleRaw() { onToggleRaw?() }
    @objc private func toggleFull() { onToggleFull?() }
    @objc private func copyBody() {
        guard let measurement else { return }
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(measurement.body.isEmpty ? measurement.originalBody : measurement.body, forType: .string)
    }
    func textView(_ textView: NSTextView, clickedOnLink link: Any, at charIndex: Int) -> Bool {
        let source = (link as? String) ?? (link as? URL)?.absoluteString
        guard let source, let location = ContentLocation.parse(source),
              location.canOpen(in: RichContentContext(isLocalHost: measurement?.isLocalHost == true)) else { return true }
        location.open(in: RichContentContext(isLocalHost: measurement?.isLocalHost == true))
        return true
    }
    override func updateTrackingAreas() {
        super.updateTrackingAreas()
        if let tracking { removeTrackingArea(tracking) }
        let area = NSTrackingArea(rect: .zero, options: [.mouseEnteredAndExited, .activeInKeyWindow, .inVisibleRect], owner: self)
        tracking = area
        addTrackingArea(area)
        refreshActions()
    }
    override func mouseEntered(with event: NSEvent) { refreshActions() }
    override func mouseExited(with event: NSEvent) { refreshActions() }
    func refreshActions() {
        let hovered = window.map { $0.isKeyWindow && visibleRect.contains(convert($0.mouseLocationOutsideOfEventStream, from: nil)) } ?? false
        let focused = window?.firstResponder === copyButton
        copyButton.alphaValue = hovered || focused ? 1 : 0
    }
}

@MainActor private final class TranscriptContentClip: NSView {
    override var isFlipped: Bool { true }
}

@MainActor private final class TranscriptActionButton: NSButton {
    var onFocus: (() -> Void)?
    override func becomeFirstResponder() -> Bool {
        let result = super.becomeFirstResponder()
        onFocus?()
        return result
    }
    override func resignFirstResponder() -> Bool {
        let result = super.resignFirstResponder()
        onFocus?()
        return result
    }
}

@MainActor private final class TranscriptDisclosureButton: NSButton {
    var restingTint: NSColor = .secondaryLabelColor { didSet { refreshTint() } }
    private var hovering = false
    private var tracking: NSTrackingArea?

    override func updateTrackingAreas() {
        super.updateTrackingAreas()
        if let tracking { removeTrackingArea(tracking) }
        let area = NSTrackingArea(rect: .zero, options: [.mouseEnteredAndExited, .activeInKeyWindow, .inVisibleRect],
                                  owner: self, userInfo: nil)
        tracking = area
        addTrackingArea(area)
    }

    override func mouseEntered(with event: NSEvent) { hovering = true; refreshTint() }
    override func mouseExited(with event: NSEvent) { hovering = false; refreshTint() }
    override func becomeFirstResponder() -> Bool {
        let accepted = super.becomeFirstResponder()
        refreshTint()
        return accepted
    }
    override func resignFirstResponder() -> Bool {
        let accepted = super.resignFirstResponder()
        contentTintColor = restingTint
        return accepted
    }
    private func refreshTint() {
        contentTintColor = hovering || window?.firstResponder === self ? .labelColor : restingTint
    }
}

/// Reserve identical text and symbol geometry in both disclosure states. The
/// chevron follows the label rather than aligning with the far edge of the row.
@MainActor private final class TranscriptDisclosureCell: NSButtonCell {
    override func titleRect(forBounds rect: NSRect) -> NSRect {
        let width = min(attributedTitle.size().width.rounded(.up), max(0, rect.width - 18))
        return NSRect(x: rect.minX, y: rect.midY - 9, width: width, height: 18)
    }

    override func imageRect(forBounds rect: NSRect) -> NSRect {
        NSRect(x: titleRect(forBounds: rect).maxX + 6, y: rect.midY - 5, width: 10, height: 10)
    }
}

/// Retain glyph shaping across width changes and use the same TextKit settings
/// as the displayed NSTextView. Width changes only recompute line wrapping.
@MainActor private final class TranscriptTextLayout {
    let attributedText: NSAttributedString
    private let storage: NSTextStorage
    private let manager = NSLayoutManager()
    private let container = NSTextContainer(size: .zero)

    convenience init(text: String, font: NSFont) {
        self.init(rendered: RichTextRenderer.render(text, font: font))
    }

    init(rendered: NSAttributedString, trimEdges: Bool = true) {
        // Markdown block separators belong between paragraphs, not at the
        // bubble edges where TextKit would measure an extra empty line.
        let value = rendered.string as NSString
        let content = CharacterSet.newlines.inverted
        let first = value.rangeOfCharacter(from: content)
        let last = value.rangeOfCharacter(from: content, options: .backwards)
        if !trimEdges {
            attributedText = rendered
        } else if first.location != NSNotFound {
            attributedText = rendered.attributedSubstring(from: NSRange(
                location: first.location, length: NSMaxRange(last) - first.location))
        } else {
            attributedText = NSAttributedString(string: "")
        }
        storage = NSTextStorage(attributedString: attributedText)
        container.lineFragmentPadding = 0
        manager.addTextContainer(container)
        storage.addLayoutManager(manager)
    }

    func height(for width: CGFloat) -> CGFloat {
        guard storage.length > 0 else { return 0 }
        container.containerSize = NSSize(width: width, height: .greatestFiniteMagnitude)
        manager.ensureLayout(for: container)
        return ceil(manager.usedRect(for: container).height)
    }

    func width(for width: CGFloat) -> CGFloat {
        _ = height(for: width)
        return ceil(manager.usedRect(for: container).width)
    }
}

/// A wheel gesture still requests a page when collapsed rows don't fill the
/// viewport, where the clip view's bounds cannot change any further.
@MainActor final class TranscriptScrollView: NSScrollView {
    var onScroll: (() -> Void)?
    override func scrollWheel(with event: NSEvent) {
        super.scrollWheel(with: event)
        onScroll?()
    }
}
