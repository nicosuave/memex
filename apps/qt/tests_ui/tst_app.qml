import QtQuick
import QtTest

TestCase {
    name: "MemexApplication"
    when: windowShown
    property var application
    property var app
    property var store
    function init() { failOnWarning(/.*/); }
    function capture(name) {
        if (!captureDirectory) return;
        waitForRendering(app.contentItem);
        let image = grabImage(app.contentItem.parent);
        image.save(captureDirectory + "/" + name + ".png");
        verify(image.width > 0 && image.height > 0);
    }
    function initTestCase() {
        failOnWarning(/.*/);
        application = Qt.createComponent("qrc:/qml/Main.qml");
        compare(application.status, Component.Ready, application.errorString());
        app = application.createObject(null);
        verify(app !== null);
        store = findChild(app, "store");
        verify(store !== null);
        tryVerify(() => store.sessions.length === 4, 15000);
    }

    function cleanupTestCase() {
        app.close();
        wait(100);
        app.destroy();
        wait(100);
    }
    function test_01_home_and_machine_identity() {
        compare(app.home, true);
        compare(store.origin, "interactive");
        tryCompare(store, "total", 4);
        tryVerify(() => store.projects.length === 1 && store.projects[0].session_count === 8, 10000);
        verify(store.sessions.some(s => s.machine === "fixture-peer"));
        verify(store.sessions.some(s => s.machine === "local"));
        compare(new Set(store.sessions.map(s => store.identity(s))).size, 4);
        tryCompare(store, "activityBusy", false);
        capture("home");
    }
    function test_02_filters_and_search() {
        store.configure({
                            origin: "subagent"
                        });
        tryVerify(() => store.sessions.length === 2 && store.sessions.every(s => s.conversation_kind === "subagent"),
        10000);
        store.resetFilters();
        tryVerify(() => store.sessions.length === 4, 10000);
        let input = findChild(app, "searchField");
        input.text = "needle";
        tryVerify(() => store.sessions.length === 2 && store.sessions[0].search_record_id === "r12", 10000);
        compare(store.sessions[0].label, "Qt parity fixture");
        input.text = "";
        tryVerify(() => store.sessions.length === 4, 10000);
    }
    function test_03_reader_paging_and_find() {
        let session = store.sessions.find(s => s.machine === "local" && s.session_id === "first");
        store.openSession(session);
        tryVerify(() => store.records.length === 60 && !store.readerBusy, 10000);
        compare(app.home, false);
        compare(store.offset, 70);
        compare(store.recordTotal, 130);
        compare(store.records[59].record_id, "r129");
        let transcript = findChild(app, "transcriptList").parent;
        tryCompare(transcript, "targetRecord", "r129");
        wait(100);
        transcript.jumpToRecord("r125");
        wait(150);
        compare(transcript.targetRecord, "r125");
        capture("conversation");
        store.earlier();
        tryVerify(() => store.offset === 10 && store.records.length === 120, 10000);
        store.find("needle");
        tryVerify(() => !store.finding && store.matches.length === 3, 10000);
        compare(store.matchIndex, 0);
        store.navigateFind(1);
        compare(store.matchIndex, 1);
        store.navigateFind(1);
        compare(store.matchIndex, 2);
        store.navigateFind(1);
        compare(store.matchIndex, 0);
        app.rawMode = true;
        wait(100);
        verify(findChild(app, "transcriptList") !== null);
    }
    function test_04_search_anchor_and_empty_state() {
        let input = findChild(app, "searchField");
        input.text = "needle";
        tryVerify(() => store.sessions.length === 2 && store.sessions[0].search_record_id === "r12", 10000);
        store.openSession(store.sessions[0]);
        tryVerify(() => !store.readerBusy && store.records.some(r => r.record_id === "r12"), 10000);
        input.text = "missing";
        tryVerify(() => store.sessions.length === 0, 10000);
    }
    function test_05_source_preview_and_remote_guard() {
        let input = findChild(app, "searchField");
        input.text = "";
        tryVerify(() => store.sessions.length === 4, 10000);
        store.openSession(store.sessions.find(s => s.machine === "local" && s.session_id === "first"));
        tryCompare(store, "readerBusy", false, 10000);
        app.link("example.rs:2");
        let dialog = findChild(app, "sourceDialog");
        tryCompare(dialog, "opened", true, 10000);
        let source = findChild(app, "sourceText");
        compare(source.selectedText, "fn main() {}\n");
        dialog.close();
        store.openSession(store.sessions.find(s => s.machine === "fixture-peer"));
        app.link("example.rs:2");
        tryVerify(() => app.actionError.indexOf("fixture-peer") >= 0, 10000);
        verify(!dialog.opened);
        app.actionError = "";
    }
}
