import AppKit
import SwiftUI

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

    func makeNSViewController(context: Context) -> TranscriptController { TranscriptController() }
    func updateNSViewController(_ controller: TranscriptController, context: Context) {
        controller.update(sessionID: sessionID, records: records, provider: provider,
                          hasMore: hasMore, isLoading: isLoading, onLoadMore: onLoadMore,
                          hasEarlier: hasEarlier, startsAtEnd: startsAtEnd, anchorID: anchorID,
                          navigation: navigation, onLoadEarlier: onLoadEarlier,
                          findQuery: findQuery, findHit: findHit, findGeneration: findGeneration)
    }
}

/// In-memory reading positions outlive the controller when the selection is empty.
@MainActor final class TranscriptNavigationState {
    struct Position {
        let recordID: String
        let offset: CGFloat
        let expanded: Set<String>
    }
    var positions: [String: Position] = [:]
    private var order: [String] = []

    func save(_ position: Position, for key: String) {
        positions[key] = position
        order.removeAll { $0 == key }
        order.append(key)
        if order.count > 20 { positions.removeValue(forKey: order.removeFirst()) }
    }
}

@MainActor
final class TranscriptController: NSViewController, NSTableViewDataSource, NSTableViewDelegate {
    let table = NSTableView()
    let scrollView = TranscriptScrollView()
    private(set) var rows: [Row] = []
    private var sessionID = ""
    private var records: [TranscriptRecord] = []
    private var provider = ""
    private var expanded = Set<String>()
    private var rawTools = Set<String>()
    private var measurements: [String: Measurement] = [:]
    private var measuredWidth: CGFloat = 0
    private var notifiedWidth: CGFloat = 0
    private var textLayouts: [String: TranscriptTextLayout] = [:]
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
        view = scrollView
    }

    func update(sessionID: String, records: [TranscriptRecord], provider: String,
                hasMore: Bool = false, isLoading: Bool = false, onLoadMore: (() -> Void)? = nil,
                hasEarlier: Bool = false, startsAtEnd: Bool = false, anchorID: String? = nil,
                navigation: TranscriptNavigationState? = nil, onLoadEarlier: (() -> Void)? = nil,
                findQuery: String = "", findHit: ConversationFindHit? = nil, findGeneration: Int = 0) {
        _ = view
        let changedSession = self.sessionID != sessionID
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
        defer { applyFindPosition() }
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
        guard changedSession || self.records != records || self.provider != provider else {
            if changedQuery { table.reloadData() }
            return
        }
        updatingRows = true
        defer { updatingRows = false }
        let appendOnly = !changedSession && self.provider == provider && records.starts(with: self.records)
        let oldOrigin = scrollView.contentView.bounds.origin
        let visiblePosition = changedSession ? nil : currentPosition()
        if appendOnly, self.records != records, let last = rows.last {
            // The final tool/instruction group may gain records across pages.
            measurements.removeValue(forKey: last.id)
            textLayouts.removeValue(forKey: last.id)
        }
        findRecordBodies.removeAll(keepingCapacity: true)
        self.sessionID = sessionID
        self.records = records
        self.provider = provider
        if changedSession {
            needsInitialPosition = true
            rawTools.removeAll()
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
        rebuildRows(resetMeasurements: !appendOnly)
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
    }

    private func currentPosition() -> TranscriptNavigationState.Position? {
        guard !needsInitialPosition, !rows.isEmpty else { return nil }
        let y = scrollView.contentView.bounds.minY
        let row = table.row(at: NSPoint(x: 1, y: y + 1))
        guard rows.indices.contains(row) else { return nil }
        return .init(recordID: recordID(at: row), offset: y - table.rect(ofRow: row).minY,
                     expanded: expanded)
    }

    private func savePosition() {
        if let position = currentPosition() { navigation.save(position, for: sessionID) }
    }

    private func restore(_ position: TranscriptNavigationState.Position) {
        guard let row = rowIndex(for: position.recordID) else { return }
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

    private func rebuildRows(resetMeasurements: Bool = false) {
        let previous = Dictionary(rows.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
        let openedRecords = Set(rows.filter {
            if case .activity = $0 { return expanded.contains($0.id) }
            return false
        }.flatMap(\.records).map(\.id))
        let openedSingletons = Set(rows.filter {
            if case .activity(_, false) = $0 { return expanded.contains($0.id) }
            return false
        }.flatMap(\.records).map(\.id))
        rows = TranscriptItem.group(records).flatMap { item -> [Row] in
            guard item.isActivity || item.isInstructions else { return [.message(item.records[0])] }
            let activities = item.activities
            for activity in activities where activity.records.contains(where: { openedRecords.contains($0.id) }) {
                expanded.insert("activity:\(activity.id)")
            }
            if activities.count == 1 { return [.activity(activities[0], nested: false)] }
            let group = Row.group(item)
            if item.records.contains(where: { openedSingletons.contains($0.id) }) { expanded.insert(group.id) }
            return expanded.contains(group.id)
                ? [group] + activities.map { .activity($0, nested: true) } : [group]
        }
        if resetMeasurements {
            measurements.removeAll(keepingCapacity: true)
            textLayouts.removeAll(keepingCapacity: true)
        } else {
            // A result can join an already visible call after paging, and a
            // singleton can become nested. Retain only unchanged row layouts.
            for row in rows {
                if previous[row.id]?.records != row.records {
                    measurements.removeValue(forKey: row.id)
                    textLayouts.removeValue(forKey: row.id)
                } else if case .activity(_, let nested) = row,
                          case .activity(_, let wasNested)? = previous[row.id], nested != wasNested {
                    measurements.removeValue(forKey: row.id)
                }
            }
        }
        table.reloadData()
    }

    func toggle(_ id: String) {
        if expanded.contains(id) { expanded.remove(id) } else { expanded.insert(id) }
        measurements.removeValue(forKey: id)
        textLayouts.removeValue(forKey: id)
        rebuildRows()
    }

    func toggleRaw(_ id: String) {
        if rawTools.contains(id) { rawTools.remove(id) } else { rawTools.insert(id) }
        measurements.removeValue(forKey: id)
        textLayouts.removeValue(forKey: id)
        rebuildRows()
    }

    func numberOfRows(in tableView: NSTableView) -> Int { rows.count }
    func tableView(_ tableView: NSTableView, shouldSelectRow row: Int) -> Bool { false }
    func tableView(_ tableView: NSTableView, heightOfRow row: Int) -> CGFloat { measurement(at: row).height }

    @objc private func scrolled(_ notification: Notification) {
        if !updatingRows { savePosition() }
        loadNextPageIfNeeded()
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
                       toggleRaw: { [weak self] in self?.toggleRaw(id) })
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
        switch row {
        case .group(let item):
            title = item.title
            isUser = false; isDisclosure = true; isTool = true; indent = 0
        case .activity(let entry, let nested):
            title = entry.title
            isUser = false; isDisclosure = true
            isTool = !entry.records[0].record.isInstruction && entry.records[0].record.role != "reasoning"
            indent = nested ? 20 : 0
            if isExpanded { fullText = entry.body }
        case .message(let entry):
            isUser = entry.record.role == "user"
            isDisclosure = !["user", "assistant"].contains(entry.record.role)
            isTool = false; indent = 0
            title = isUser ? "You" : (entry.record.role == "assistant" ? provider : entry.record.role.capitalized)
            if !isDisclosure || isExpanded { fullText = entry.record.text }
        }
        let body = fullText
        let font: NSFont = isTool ? .monospacedSystemFont(ofSize: 12, weight: .regular) : .systemFont(ofSize: 14)
        let available = max(120, min(800, width - 60) - indent)
        let contentWidth = isUser ? min(620, available) : available
        let contentX = isUser ? width - 30 - contentWidth : 30 + indent
        let bodyWidth = contentWidth - (isUser ? 30 : 0)
        let showsRaw = rawTools.contains(row.id) || !findQuery.isEmpty
        let textLayout: TranscriptTextLayout
        if let cached = textLayouts[row.id] { textLayout = cached }
        else if isTool && isExpanded && !body.isEmpty {
            textLayout = TranscriptTextLayout(rendered: ToolContentRenderer.render(row.records, raw: showsRaw), trimEdges: false)
        } else { textLayout = TranscriptTextLayout(text: body, font: font) }
        textLayouts[row.id] = textLayout
        let textHeight = textLayout.height(for: bodyWidth)
        let bodyHeight = textHeight + (isUser && !body.isEmpty ? 30 : 0)
        let showsRawControl = isTool && isExpanded && !body.isEmpty
        let height = 38 + bodyHeight + (body.isEmpty ? 0 : 20) + (showsRawControl ? 28 : 0)
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
        let result = Measurement(title: title, body: body, attributedBody: highlighted, font: font, textHeight: textHeight, height: height,
            contentX: contentX, contentWidth: contentWidth, isUser: isUser, isDisclosure: isDisclosure,
            isExpanded: isExpanded, showsRawControl: showsRawControl, showsRaw: showsRaw, finding: !findQuery.isEmpty)
        measurements[row.id] = result
        return result
    }
}

@MainActor
private final class TranscriptCell: NSTableCellView {
    private let titleLabel = NSTextField(labelWithString: "")
    private let disclosure = NSButton()
    private let rawDisclosure = NSButton()
    private let message = NSTextView()
    private let bubble = NSView()
    private var measurement: TranscriptController.Measurement?
    private var onToggle: (() -> Void)?
    private var onToggleRaw: (() -> Void)?
    private var displayedText: NSAttributedString?
    override var isFlipped: Bool { true }

    override init(frame frameRect: NSRect) {
        super.init(frame: frameRect)
        titleLabel.font = .systemFont(ofSize: 12, weight: .medium)
        titleLabel.textColor = .secondaryLabelColor
        titleLabel.lineBreakMode = .byTruncatingTail
        disclosure.isBordered = false
        disclosure.alignment = .left
        disclosure.font = .systemFont(ofSize: 13)
        disclosure.contentTintColor = .secondaryLabelColor
        disclosure.target = self
        disclosure.action = #selector(toggle)
        rawDisclosure.isBordered = false
        rawDisclosure.alignment = .left
        rawDisclosure.font = .systemFont(ofSize: 12)
        rawDisclosure.contentTintColor = .secondaryLabelColor
        rawDisclosure.target = self
        rawDisclosure.action = #selector(toggleRaw)
        message.isEditable = false
        message.isSelectable = true
        message.drawsBackground = false
        message.textContainerInset = .zero
        message.textContainer?.lineFragmentPadding = 0
        message.isVerticallyResizable = false
        message.isHorizontallyResizable = false
        message.textContainer?.widthTracksTextView = true
        bubble.wantsLayer = true
        bubble.layer?.cornerRadius = 16
        addSubview(bubble)
        addSubview(titleLabel)
        addSubview(disclosure)
        addSubview(rawDisclosure)
        addSubview(message)
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }

    func configure(_ value: TranscriptController.Measurement, toggle: @escaping () -> Void, toggleRaw: @escaping () -> Void) {
        measurement = value
        onToggle = toggle
        onToggleRaw = toggleRaw
        rawDisclosure.isHidden = !value.showsRawControl
        rawDisclosure.title = value.finding ? "Raw content shown for Find" : (value.showsRaw ? "Show formatted content" : "Show raw content")
        rawDisclosure.isEnabled = !value.finding
        titleLabel.stringValue = value.title
        titleLabel.alignment = value.isUser ? .right : .left
        titleLabel.isHidden = value.isDisclosure
        disclosure.isHidden = !value.isDisclosure
        disclosure.title = "\(value.isExpanded ? "⌄" : "›")  \(value.title)"
        disclosure.setAccessibilityLabel(value.title)
        disclosure.setAccessibilityValue(value.isExpanded ? "Expanded" : "Collapsed")
        if displayedText !== value.attributedBody {
            message.textStorage?.setAttributedString(value.attributedBody)
            message.setSelectedRange(NSRange(location: 0, length: 0))
            displayedText = value.attributedBody
        }
        message.isHidden = value.body.isEmpty
        bubble.isHidden = !value.isUser || value.body.isEmpty
        bubble.layer?.backgroundColor = NSColor.controlAccentColor.withAlphaComponent(0.10).cgColor
        needsLayout = true
    }

    override func layout() {
        super.layout()
        guard let value = measurement else { return }
        let x = value.contentX
        let width = value.contentWidth
        titleLabel.frame = NSRect(x: x, y: 10, width: width, height: 18)
        disclosure.frame = NSRect(x: x, y: 8, width: width, height: 22)
        rawDisclosure.frame = NSRect(x: x, y: 32, width: width, height: 22)
        let bodyY: CGFloat = value.showsRawControl ? 62 : 34
        let inset: CGFloat = value.isUser ? 15 : 0
        message.frame = NSRect(x: x + inset, y: bodyY + inset, width: width - 2 * inset, height: value.textHeight)
        bubble.frame = NSRect(x: x, y: 34, width: width, height: value.textHeight + 2 * inset)
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
