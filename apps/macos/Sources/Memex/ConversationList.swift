import AppKit
import SwiftUI

struct NativeConversationList: NSViewControllerRepresentable {
    let sessions: [Session]
    let selectedID: String?
    let select: (String?) -> Void
    let loadMore: (String) -> Void

    func makeNSViewController(context: Context) -> ConversationListController { ConversationListController() }
    func updateNSViewController(_ controller: ConversationListController, context: Context) {
        controller.update(sessions: sessions, selectedID: selectedID, select: select, loadMore: loadMore)
    }
}

@MainActor final class ConversationListController: NSViewController, NSTableViewDataSource, NSTableViewDelegate {
    struct Row {
        let session: Session
        let id: String
        let project: String
        let title: String
        let preview: String
        let date: String
        let machine: String?
        init(_ session: Session) {
            self.session = session
            id = session.id
            project = session.projectName
            title = session.title
            preview = session.snippet?.nilIfBlank ?? session.source
            date = session.date?.formatted(.dateTime.month(.abbreviated).day()) ?? ""
            machine = session.machineID == "local" ? nil : session.machineID
        }
    }

    let table = NSTableView()
    let scrollView = NSScrollView()
    private(set) var rows: [Row] = []
    private var select: ((String?) -> Void)?
    private var loadMore: ((String) -> Void)?
    private var updating = false

    override func loadView() {
        scrollView.hasVerticalScroller = true
        scrollView.autohidesScrollers = true
        scrollView.drawsBackground = false
        table.headerView = nil
        table.backgroundColor = .clear
        table.style = .inset
        table.intercellSpacing = NSSize(width: 0, height: 0)
        table.usesAutomaticRowHeights = false
        table.allowsEmptySelection = true
        table.allowsMultipleSelection = false
        table.columnAutoresizingStyle = .uniformColumnAutoresizingStyle
        let column = NSTableColumn(identifier: NSUserInterfaceItemIdentifier("conversation"))
        column.minWidth = 100
        column.width = 260
        column.resizingMask = .autoresizingMask
        table.addTableColumn(column)
        table.dataSource = self
        table.delegate = self
        scrollView.documentView = table
        scrollView.contentView.postsBoundsChangedNotifications = true
        NotificationCenter.default.addObserver(self, selector: #selector(scrolled),
            name: NSView.boundsDidChangeNotification, object: scrollView.contentView)
        view = scrollView
    }

    func update(sessions: [Session], selectedID: String?, select: @escaping (String?) -> Void,
                loadMore: @escaping (String) -> Void) {
        loadViewIfNeeded()
        self.select = select
        self.loadMore = loadMore
        updating = true
        let previous = Dictionary(rows.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
        let next = sessions.map { session -> Row in
            let id = session.id
            if let cached = previous[id], cached.session == session { return cached }
            return Row(session)
        }
        let oldIDs = rows.map(\.id)
        let newIDs = next.map(\.id)
        let appended = newIDs.count >= oldIDs.count && newIDs.prefix(oldIDs.count).elementsEqual(oldIDs)
        let changed = IndexSet(next.indices.filter { $0 < rows.count && next[$0].session != rows[$0].session })
        let anchor = table.row(at: NSPoint(x: 0, y: scrollView.contentView.bounds.minY))
        let anchorID = rows.indices.contains(anchor) ? rows[anchor].id : nil
        let offset = anchor >= 0 ? scrollView.contentView.bounds.minY - table.rect(ofRow: anchor).minY : 0
        rows = next
        if appended {
            if next.count > oldIDs.count {
                table.insertRows(at: IndexSet(integersIn: oldIDs.count..<next.count), withAnimation: [])
            }
            if !changed.isEmpty {
                table.noteHeightOfRows(withIndexesChanged: changed)
                table.reloadData(forRowIndexes: changed, columnIndexes: IndexSet(integer: 0))
            }
        } else {
            table.reloadData()
            if let anchorID, let index = rows.firstIndex(where: { $0.id == anchorID }) {
                scrollView.contentView.scroll(to: NSPoint(x: 0, y: table.rect(ofRow: index).minY + offset))
                scrollView.reflectScrolledClipView(scrollView.contentView)
            } else {
                scrollView.contentView.scroll(to: .zero)
            }
        }
        let selection = selectedID.flatMap { id in rows.firstIndex { $0.id == id } }
        table.selectRowIndexes(selection.map { IndexSet(integer: $0) } ?? [], byExtendingSelection: false)
        updating = false
    }

    func numberOfRows(in tableView: NSTableView) -> Int { rows.count }
    func tableView(_ tableView: NSTableView, heightOfRow row: Int) -> CGFloat { rows[row].machine == nil ? 68 : 84 }
    func tableView(_ tableView: NSTableView, viewFor tableColumn: NSTableColumn?, row: Int) -> NSView? {
        let identifier = NSUserInterfaceItemIdentifier("conversation-cell")
        let cell = tableView.makeView(withIdentifier: identifier, owner: self) as? ConversationCell ?? ConversationCell()
        cell.identifier = identifier
        cell.configure(rows[row])
        return cell
    }
    func tableViewSelectionDidChange(_ notification: Notification) {
        guard !updating else { return }
        select?(rows.indices.contains(table.selectedRow) ? rows[table.selectedRow].id : nil)
    }
    @objc private func scrolled() {
        guard !updating, !rows.isEmpty else { return }
        let visible = table.rows(in: table.visibleRect)
        guard visible.location != NSNotFound, visible.length > 0 else { return }
        let last = min(rows.count - 1, NSMaxRange(visible) - 1)
        if last >= rows.count - 5 { loadMore?(rows[last].id) }
    }
}

@MainActor final class ConversationCell: NSTableCellView {
    private let project = NSTextField(labelWithString: "")
    private let date = NSTextField(labelWithString: "")
    private let title = NSTextField(wrappingLabelWithString: "")
    private let preview = NSTextField(wrappingLabelWithString: "")
    private let machine = NSTextField(labelWithString: "")

    override init(frame: NSRect) {
        super.init(frame: frame)
        for field in [project, date, title, preview, machine] {
            field.font = .systemFont(ofSize: 11)
            field.textColor = .secondaryLabelColor
            field.lineBreakMode = .byTruncatingTail
            addSubview(field)
        }
        project.font = .systemFont(ofSize: 12, weight: .semibold)
        project.textColor = .labelColor
        title.font = .systemFont(ofSize: 13)
        title.textColor = .labelColor
        preview.font = .systemFont(ofSize: 12)
        for field in [title, preview] {
            field.lineBreakMode = .byTruncatingTail
            field.cell?.wraps = false
            field.cell?.isScrollable = false
        }
        title.maximumNumberOfLines = 1
        preview.maximumNumberOfLines = 1
        date.alignment = .right
        setAccessibilityElement(true)
    }
    required init?(coder: NSCoder) { fatalError("init(coder:) has not been implemented") }
    override var isFlipped: Bool { true }
    func configure(_ row: ConversationListController.Row) {
        project.stringValue = row.project
        date.stringValue = row.date
        title.stringValue = row.title
        preview.stringValue = row.preview
        machine.stringValue = row.machine.map { "▣ \($0)" } ?? ""
        machine.isHidden = row.machine == nil
        setAccessibilityLabel([row.project, row.date, row.title, row.preview, row.machine].compactMap { $0 }.joined(separator: ", "))
        needsLayout = true
    }
    override func layout() {
        super.layout()
        let width = max(0, bounds.width - 16)
        let dateWidth = min(width, ceil(date.intrinsicContentSize.width) + 4)
        project.frame = NSRect(x: 8, y: 8, width: max(0, width - dateWidth - 6), height: 15)
        date.frame = NSRect(x: 8 + width - dateWidth, y: 8, width: dateWidth, height: 15)
        title.frame = NSRect(x: 8, y: 25, width: width, height: 17)
        preview.frame = NSRect(x: 8, y: 43, width: width, height: 16)
        machine.frame = NSRect(x: 8, y: 88, width: width, height: 15)
    }
}
