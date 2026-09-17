import QtQuick
import QtQuick.Controls
import QtQuick.Layouts
import QtCore
import memex_qt
import "." as Components

ApplicationWindow {
    id: window
    visible: true
    width: preferences.width
    height: preferences.height
    minimumWidth: 850
    minimumHeight: 560
    title: "Memex"
    color: Theme.background
    font.family: Theme.fontFamily
    font.pixelSize: 13
    property bool sidebarVisible: true
    property string searchQuery: ""
    property bool home: true
    property bool rawMode: false
    property bool findVisible: false
    property var destinations: []
    property string actionError: ""
    property var preview: ({})
    Settings {
        id: preferences
        category: "Window"
        property int width: 1178
        property int height: 768
        property real sidebarWidth: 170
        property real conversationWidth: 240
        property string destination: ""
        location: StandardPaths.writableLocation(StandardPaths.AppConfigLocation) + "/window.ini"
    }
    onClosing: {
        store.action({op: "shutdown"});
        preferences.width = width;
        preferences.height = height;
        preferences.sidebarWidth = sidebar.width;
        if (!home)
            preferences.conversationWidth = conversationPane.width;
    }
    Backend {
        id: backend
    }
    Store {
        id: store
        objectName: "store"
        backend: backend
        onReaderOpened: {
            home = false;
            rawMode = false;
            findVisible = false;
        }
        onPositionRecord: (id, occurrence) => transcript.jumpToRecord(id, occurrence)
        onRestorePosition: (id, offset, expansion) => {
            transcript.expandedItems = expansion;
            transcript.restoreAnchor(id, offset);
        }
    }
    Component.onCompleted: {
        store.initialize();
        store.request({
                          op: "destinations"
                      }, function (value, error) {
                          if (!error)
                              destinations = value;
                      });
    }
    function navigate(isHome, project) {
        home = isHome;
        store.configure({
                            project: project
                        });
    }
    function copy(text) {
        clipboard.text = text;
        clipboard.selectAll();
        clipboard.copy();
        clipboard.clear();
    }
    function link(url) {
        if (/^https?:\/\//i.test(url)) {
            Qt.openUrlExternally(url);
            return;
        }
        store.request({
                          op: "source_preview",
                          session: store.selected,
                          url: url
                      }, function (value, error) {
                          if (error)
                              actionError = error;
                          else {
                              preview = value;
                              sourceDialog.open();
                          }
                      });
    }
    function resume(destination) {
        preferences.destination = destination;
        let s = store.selected;
        store.request({
                          op: "sessions",
                          machine: s.machine || "local",
                          filters: {
                              session_id: s.session_id,
                              source_path: s.source_path,
                              source: s.source,
                              origin: "all",
                              limit: 1
                          }
                      }, function (rows, error) {
                          if (error || rows.length !== 1) {
                              actionError = error || "Resume details are unavailable.";
                              return;
                          }
                          let fresh = Object.assign({}, rows[0], {
                                                        machine: s.machine || "local"
                                                    });
                          store.request({
                                            op: "resume",
                                            session: fresh,
                                            destination: destination
                                        }, function (_, failure) {
                                            if (failure)
                                                actionError = failure;
                                        });
                      });
    }
    TextEdit {
        id: clipboard
        visible: false
    }
    Shortcut {
        sequences: [StandardKey.Find]
        onActivated: {
            if (!home && store.selected) {
                findVisible = true;
                findField.forceActiveFocus();
            }
        }
    }
    Shortcut {
        sequences: [StandardKey.FindNext]
        onActivated: store.navigateFind(1)
    }
    Shortcut {
        sequences: [StandardKey.FindPrevious]
        onActivated: store.navigateFind(-1)
    }
    Shortcut {
        sequences: [StandardKey.Refresh]
        onActivated: store.refresh()
    }
    Timer {
        id: searchDebounce
        interval: 250
        onTriggered: store.configure({
                                         query: window.searchQuery
                                     })
    }
    Timer {
        interval: 60000
        repeat: true
        running: window.active && home
        onTriggered: store.refreshActivity()
    }
    header: ToolBar {
        height: 44
        background: Rectangle {
            color: Theme.background
            Rectangle {
                visible: sidebarVisible
                width: sidebar.width
                height: parent.height
                color: Theme.sidebar
            }
        }
        RowLayout {
            anchors.fill: parent
            anchors.leftMargin: 12
            anchors.rightMargin: 16
            spacing: 5
            IconButton { symbol: "sidebar"; text: "Toggle sidebar"; onClicked: sidebarVisible = !sidebarVisible }
            Item { visible: sidebarVisible; Layout.preferredWidth: Math.max(0, sidebar.width - 52) }
            Label {
                visible: !home
                text: store.project ? store.project.split("/").pop() : "All conversations"
                font.weight: Font.DemiBold
                color: Theme.text
            }
            Label { visible: !home; text: store.total >= 0 ? store.total : ""; color: Theme.tertiary; font.pixelSize: 11 }
            IconButton { visible: !home; objectName: "filtersButton"; symbol: "filter"; text: "Filter conversations"; onClicked: filters.open() }
            IconButton { visible: !home; symbol: "refresh"; text: "Refresh"; onClicked: store.refresh() }
            Item { Layout.fillWidth: true }
            IconButton { visible: !home; symbol: "search"; text: "Find in conversation"; enabled: !!store.selected; onClicked: { findVisible = true; findField.forceActiveFocus(); } }
            IconButton { visible: !home; symbol: "copy"; text: "Copy session ID"; enabled: !!store.selected; onClicked: window.copy(store.selected.session_id) }
            IconButton {
                visible: !home
                symbol: "file"; text: "Reveal source"
                enabled: !!store.selected && (store.selected.machine || "local") === "local"
                onClicked: store.request({op: "reveal", session: store.selected}, function (_, error) { if (error) actionError = error; })
            }
            Button {
                id: resumeButton
                visible: !home
                text: "Resume"
                implicitHeight: 28
                leftPadding: 15; rightPadding: 15
                enabled: !!store.selected && (store.selected.machine || "local") === "local" && destinations.length > 0
                background: Rectangle { radius: 14; color: resumeButton.hovered ? Theme.hover : Theme.codeBackground }
                contentItem: Label { text: parent.text; color: parent.enabled ? Theme.text : Theme.tertiary; horizontalAlignment: Text.AlignHCenter; verticalAlignment: Text.AlignVCenter }
                onClicked: if (preferences.destination && destinations.some(d => d.id === preferences.destination)) resume(preferences.destination); else resumeMenu.open()
            }
            IconButton { visible: !home; symbol: "down"; text: "Resume destination"; enabled: resumeButton.enabled; onClicked: resumeMenu.open() }
            Components.SearchField {
                id: searchField
                objectName: "searchField"
                visible: !home
                Layout.preferredWidth: 190
                Layout.preferredHeight: 28
                text: store.query
                onTextChanged: { window.searchQuery = text; searchDebounce.restart(); }
                onAccepted: if (store.sessions.length) store.openSession(store.sessions[0])
            }
            Menu {
                id: resumeMenu
                Instantiator {
                    model: destinations
                    delegate: MenuItem {
                        required property var modelData
                        text: modelData.title
                        onTriggered: resume(modelData.id)
                    }
                    onObjectAdded: (index, object) => resumeMenu.insertItem(index, object)
                    onObjectRemoved: (index, object) => resumeMenu.removeItem(object)
                }
            }
        }
    }
    Popup {
        id: filters
        x: Math.max(0, window.width - width - 20)
        y: 44
        width: 340
        padding: 20
        modal: false
        closePolicy: Popup.CloseOnEscape | Popup.CloseOnPressOutside
        ColumnLayout {
            width: parent.width
            spacing: 12
            Label {
                text: "Conversation filters"
                font.bold: true
            }
            Label {
                text: "Timeframe"
            }
            ComboBox {
                Layout.fillWidth: true
                model: ["All time", "Last 24 hours", "Last 7 days", "Last 30 days"]
                currentIndex: ["all", "day", "week", "month"].indexOf(store.timeframe)
                onActivated: store.configure({
                                                 timeframe: ["all", "day", "week", "month"][currentIndex]
                                             })
            }
            Label {
                text: "Provider"
            }
            ComboBox {
                Layout.fillWidth: true
                model: ["All providers", "claude", "codex", "cursor", "opencode", "pi", "omp", "openclaw", "copilot",
                    "grok", "hermes", "jcode", "muse", "bob"]
                currentIndex: store.provider ? model.indexOf(store.provider) : 0
                onActivated: store.configure({
                                                 provider: currentIndex ? model[currentIndex] : ""
                                             })
            }
            Label {
                text: "Type"
            }
            ComboBox {
                Layout.fillWidth: true
                model: ["Chats only", "Chats and subagents", "Subagents only"]
                currentIndex: store.origin === "interactive" ? 0 : store.origin === "subagent" ? 2 : 1
                onActivated: store.configure({
                                                 origin: ["interactive", "regular", "subagent"][currentIndex]
                                             })
            }
            CheckBox {
                text: "Show permission reviews"
                enabled: store.origin === "regular" || store.origin === "all"
                checked: store.origin === "all"
                onClicked: store.configure({
                                               origin: checked ? "all" : "regular"
                                           })
            }
            Button {
                text: "Reset filters"
                onClicked: store.resetFilters()
            }
        }
    }
    ColumnLayout {
        anchors.fill: parent
        spacing: 0
        Frame {
            visible: !!actionError || Object.keys(store.errors).length > 0
            Layout.fillWidth: true
            RowLayout {
                anchors.fill: parent
                Label {
                    Layout.fillWidth: true
                    wrapMode: Text.Wrap
                    text: actionError || Object.entries(store.errors).map(e => e[0] + ": " + e[1]).join("\n")
                    color: Theme.text
                }
                Button {
                    text: "Retry"
                    onClicked: {
                        actionError = "";
                        store.refresh();
                    }
                }
                ToolButton {
                    text: "Close"
                    onClicked: {
                        actionError = "";
                        store.errors = {};
                    }
                }
            }
        }
        SplitView {
            Layout.fillWidth: true
            Layout.fillHeight: true
            handle: Rectangle { implicitWidth: 1; implicitHeight: 1; color: Theme.divider }
            Pane {
                id: sidebar
                visible: sidebarVisible
                padding: 9
                background: Rectangle { color: Theme.sidebar }
                SplitView.preferredWidth: preferences.sidebarWidth
                SplitView.minimumWidth: 150
                ColumnLayout {
                    anchors.fill: parent
                    spacing: 6
                    SidebarRow {
                        text: "Home"; symbol: "home"; selected: home
                        Layout.fillWidth: true
                        onClicked: navigate(true, "")
                    }
                    SidebarRow {
                        text: "All conversations"; symbol: "chat"; selected: !home && !store.project
                        count: store.total >= 0 ? String(store.total) : ""
                        Layout.fillWidth: true
                        onClicked: navigate(false, "")
                    }
                    RowLayout {
                        Label {
                            text: "Projects"
                            font.pixelSize: 11
                            color: Theme.secondary
                            Layout.fillWidth: true
                        }
                        IconButton {
                            symbol: "more"
                            text: "Sort projects"
                            onClicked: projectMenu.open()
                        }
                        Menu {
                            id: projectMenu
                            MenuItem {
                                text: "Recent activity"
                                onTriggered: store.configure({
                                                                 projectSort: "activity"
                                                             })
                            }
                            MenuItem {
                                text: "Conversation count"
                                onTriggered: store.configure({
                                                                 projectSort: "count"
                                                             })
                            }
                            MenuItem {
                                text: "Name"
                                onTriggered: store.configure({
                                                                 projectSort: "name"
                                                             })
                            }
                        }
                    }
                    ListView {
                        Layout.fillWidth: true
                        Layout.fillHeight: true
                        clip: true
                        model: store.projects
                        ScrollBar.vertical: ScrollBar {}
                        delegate: SidebarRow {
                            required property var modelData
                            width: ListView.view.width
                            selected: !home && store.project === modelData.project
                            symbol: "folder"
                            text: modelData.project.split("/").filter(Boolean).pop() || "No project"
                            count: String(modelData.session_count)
                            onClicked: navigate(false, modelData.project)
                            ToolTip.visible: hovered
                            ToolTip.text: modelData.project
                        }
                    }
                    ComboBox {
                        Layout.fillWidth: true
                        implicitHeight: 28
                        font.pixelSize: 11
                        background: Rectangle { color: Theme.hover; radius: 6 }
                        model: [
                            {
                                id: "all",
                                label: "All Machines"
                            }
                        ].concat(store.machines)
                        textRole: "label"
                        valueRole: "id"
                        currentIndex: model.findIndex(m => m.id === store.machine)
                        onActivated: store.configure({
                                                         machine: currentValue
                                                     })
                    }
                }
            }
            Pane {
                id: conversationPane
                padding: 6
                background: Rectangle { color: Theme.background }
                visible: !home
                SplitView.preferredWidth: preferences.conversationWidth
                SplitView.minimumWidth: 210
                ColumnLayout {
                    anchors.fill: parent
                    ListView {
                        id: conversationList
                        objectName: "conversationList"
                        Layout.fillWidth: true
                        Layout.fillHeight: true
                        clip: true
                        model: store.sessions
                        ScrollBar.vertical: ScrollBar {}
                        onAtYEndChanged: if (atYEnd && !store.applying)
                                             Qt.callLater(() => store.moreSessions())
                        delegate: SessionRow {
                            required property var modelData
                            width: conversationList.width
                            session: modelData
                            selected: !!store.selected && store.identity(modelData) === store.identity(store.selected)
                            onClicked: store.openSession(modelData)
                        }
                    }
                    BusyIndicator {
                        running: store.pending > 0
                        visible: running
                        Layout.alignment: Qt.AlignHCenter
                        implicitHeight: 28
                    }
                    Label {
                        visible: !store.pending && !store.sessions.length
                        text: "No conversations match these filters."
                        wrapMode: Text.Wrap
                        Layout.fillWidth: true
                    }
                }
            }
            Pane {
                padding: 0
                background: Rectangle { color: Theme.background }
                SplitView.fillWidth: true
                ColumnLayout {
                    anchors.fill: parent
                    Home {
                        onQueryEdited: text => { window.searchQuery = text; searchDebounce.restart(); }
                        onFilterRequested: filters.open()
                        visible: home
                        Layout.fillWidth: true
                        Layout.fillHeight: true
                        store: store
                    }
                    RowLayout {
                        visible: !home && !!store.selected
                        Layout.fillWidth: true
                        Layout.leftMargin: 26
                        Layout.rightMargin: 18
                        Layout.topMargin: 16
                        Layout.bottomMargin: 14
                        ColumnLayout {
                            Layout.fillWidth: true
                            spacing: 5
                            Label {
                                Layout.fillWidth: true
                                text: store.selected ? store.selected.label || "Untitled conversation" : ""
                                font.pixelSize: 14; font.weight: Font.DemiBold; color: Theme.text
                                elide: Text.ElideRight
                            }
                            Label {
                                Layout.fillWidth: true
                                text: store.selected ? [store.selected.project, store.selected.source, store.selected.machine || "local"].filter(Boolean).join(" · ") : ""
                                font.pixelSize: 10; color: Theme.secondary; elide: Text.ElideMiddle
                            }
                        }
                        IconButton {
                            symbol: "more"
                            text: "Reader options"
                            onClicked: readerMenu.open()
                        }
                        Menu {
                            id: readerMenu
                            MenuItem {
                                text: "Raw transcript"
                                checkable: true
                                checked: rawMode
                                onTriggered: rawMode = checked
                            }
                            MenuItem {
                                text: "Find in conversation"
                                onTriggered: {
                                    findVisible = true;
                                    findField.forceActiveFocus();
                                }
                            }
                        }
                    }
                    RowLayout {
                        visible: !home && findVisible
                        Layout.fillWidth: true
                        TextField {
                            id: findField
                            objectName: "findField"
                            Layout.fillWidth: true
                            placeholderText: "Find in entire conversation"
                            onTextChanged: findDebounce.restart()
                            onAccepted: store.navigateFind(1)
                            Keys.onEscapePressed: {
                                findVisible = false;
                                store.find("");
                            }
                        }
                        Label {
                            text: (store.matchIndex + 1) + " / " + store.matches.length + (store.finding
                                                                                           ? " · scanning…" : "")
                        }
                        ToolButton {
                            text: "Previous"
                            onClicked: store.navigateFind(-1)
                        }
                        ToolButton {
                            text: "Next"
                            onClicked: store.navigateFind(1)
                        }
                        ToolButton {
                            text: "Close"
                            onClicked: {
                                findVisible = false;
                                store.find("");
                            }
                        }
                    }
                    Timer {
                        id: findDebounce
                        interval: 150
                        onTriggered: store.find(findField.text)
                    }
                    Label {
                        visible: !home && !!(store.readerError || store.metadataError || store.findError)
                        text: store.readerError || store.metadataError || store.findError
                        wrapMode: Text.Wrap
                        Layout.fillWidth: true
                    }
                    Transcript {
                        id: transcript
                        visible: !home && !!store.selected
                        Layout.fillWidth: true
                        Layout.fillHeight: true
                        records: store.records
                        session: store.selected
                        rawMode: window.rawMode
                        findText: store.findText
                        hasEarlier: store.offset > 0
                        hasLater: store.offset + store.records.length < store.recordTotal
                        loading: store.readerBusy
                        onEarlier: store.earlier()
                        onLater: store.later()
                        onOpenLink: url => window.link(url)
                        onCopyText: text => window.copy(text)
                        onReadingPositionChanged: (id, offset) => { if (!store.applying) store.saveAnchor(id, offset, expandedItems) }
                        onExpandedItemsChanged: if (readingAnchor && !store.applying)
                                                    store.saveAnchor(readingAnchor, readingOffset, expandedItems)
                    }
                    Label {
                        visible: !home && !store.selected
                        Layout.fillWidth: true
                        Layout.fillHeight: true
                        horizontalAlignment: Text.AlignHCenter
                        verticalAlignment: Text.AlignVCenter
                        text: "Select a conversation"
                        font.pixelSize: 22
                        color: palette.placeholderText
                    }
                }
            }
        }
    }
    Dialog {
        id: sourceDialog
        objectName: "sourceDialog"
        title: preview.path || "Source preview"
        width: Math.min(1000, window.width - 80)
        height: Math.min(700, window.height - 80)
        anchors.centerIn: parent
        modal: true
        standardButtons: Dialog.Close
        onOpened: {
            if (preview.line_exists) {
                sourceText.cursorPosition = preview.line_start;
                sourceText.select(preview.line_start, preview.line_end);
            }
        }
        ColumnLayout {
            anchors.fill: parent
            Label {
                text: preview.line_exists ? "Line " + preview.line : "Line " + preview.line + " is outside this file"
            }
            ScrollView {
                Layout.fillWidth: true
                Layout.fillHeight: true
                TextArea {
                    id: sourceText
                    objectName: "sourceText"
                    readOnly: true
                    selectByMouse: true
                    text: preview.text || ""
                    font.family: "monospace"
                    wrapMode: TextEdit.NoWrap
                }
            }
        }
    }
}
