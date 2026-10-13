import Testing
@testable import Memex

@MainActor @Suite(.serialized) struct ConversationListTests {
    @Test func narrowSearchExcerptKeepsTheMatchNearItsStartWithoutChangingSource() {
        let prefix = String(repeating: "context ", count: 10)
        let snippet = prefix + "MEMEX_UI_TOOL_OK in the original result"
        let excerpt = ConversationExcerpt.text(snippet, query: "\"MEMEX_UI_TOOL_OK\"")
        #expect(excerpt.hasPrefix("…"))
        #expect(excerpt.prefix(40).contains("MEMEX_UI_TOOL_OK"))
        #expect(excerpt.hasSuffix("in the original result"))
        #expect(ConversationExcerpt.text(snippet, query: "absent") == snippet)
        #expect(ConversationExcerpt.text("🧪 Café café context needle", query: "needle").contains("needle"))
    }

    @Test func sidebarShowsSearchExcerptsOnlyForSearches() {
        let store = Store()
        let session = Session(source: "codex", sessionID: "s", sourcePath: "/s", project: "memex", label: "Title",
                              snippet: String(repeating: "context ", count: 10) + "needle passage", searchRecordID: "record")
        #expect(store.sidebarExcerpt(for: session) == nil)
        store.query = "needle"
        let excerpt = store.sidebarExcerpt(for: session)
        #expect(excerpt?.hasPrefix("…") == true)
        #expect(excerpt?.contains("needle passage") == true)
        var withoutSnippet = session
        withoutSnippet.snippet = nil
        #expect(store.sidebarExcerpt(for: withoutSnippet) == nil)
    }

    @Test func emptySidebarExplainsHowToIndexHistory() {
        #expect(BrowserSidebar.emptyHint(query: "", filtered: false).contains("memex index"))
        #expect(BrowserSidebar.emptyTitle(query: "x", filtered: false) == "No matches")
        #expect(BrowserSidebar.emptyTitle(query: "", filtered: true) == "No matching conversations")
    }
}
