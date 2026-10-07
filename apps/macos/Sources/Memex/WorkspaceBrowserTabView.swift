import SwiftUI

struct WorkspaceBrowserTabView: View {
    @ObservedObject var tabs: WorkspaceBrowserTabs
    let automation: WorkspaceBrowserAutomationHost
    var live: LiveConversation?
    var isActive = true
    @State private var grantingAccess = false

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 6) {
                ScrollView(.horizontal) {
                    HStack(spacing: 4) {
                        ForEach(tabs.sessions, id: \.id) { session in
                            WorkspaceBrowserTabLabel(session: session, selected: tabs.selectedID == session.id,
                                                     select: { tabs.selectedID = session.id }, close: { tabs.close(session.id) })
                        }
                    }
                }
                Button { _ = tabs.add() } label: { Image(systemName: "plus") }
                    .disabled(tabs.sessions.count >= 20).help("New browser tab").accessibilityLabel("New browser tab")
                Button {
                    if tabs.automationGrant != nil { automation.revoke(conversationID: tabs.conversationID) }
                    else { grantingAccess = true }
                } label: {
                    Image(systemName: tabs.automationGrant == nil ? "hand.raised" : "checkmark.shield")
                        .foregroundStyle(tabs.automationGrant == nil ? Color.secondary : Color.accentColor)
                }
                .help(tabs.automationGrant == nil ? "Allow agent access to this chat’s browser tabs" : "Revoke agent browser access")
                .accessibilityLabel(tabs.automationGrant == nil ? "Allow agent browser access" : "Revoke agent browser access")
            }.font(.caption).buttonStyle(.plain).padding(8).background(.bar)
            WorkspaceBrowserView(session: tabs.selected, isActive: isActive, live: live)
        }
        .confirmationDialog("Allow agents to control this chat’s browser tabs?", isPresented: $grantingAccess) {
            Button("Allow reading and interaction") {
                _ = try? automation.allow(conversationID: tabs.conversationID, capabilities: Set(WorkspaceBrowserCapability.allCases).subtracting([.evaluate, .record, .stopRecording]))
            }
            Button("Allow interaction and JavaScript evaluation") {
                _ = try? automation.allow(conversationID: tabs.conversationID, capabilities: Set(WorkspaceBrowserCapability.allCases).subtracting([.record, .stopRecording]))
            }
            Button("Allow interaction and viewport recording") {
                _ = try? automation.allow(conversationID: tabs.conversationID, capabilities: Set(WorkspaceBrowserCapability.allCases).subtracting([.evaluate]))
            }
            Button("Cancel", role: .cancel) {}
        } message: {
            Text("Agents can read pages and interact with signed-in sites in these app-owned tabs. Access lasts until revoked or Memex quits. Recording is separately granted and saves short silent MP4 clips on this desktop. Other apps and conversations are excluded.")
        }
    }
}

private struct WorkspaceBrowserTabLabel: View {
    @ObservedObject var session: WorkspaceBrowserSession
    let selected: Bool
    let select: () -> Void
    let close: () -> Void
    var body: some View {
        HStack(spacing: 6) {
            Button(action: select) { Text(session.title).lineLimit(1).frame(maxWidth: 140) }
            Button(action: close) { Image(systemName: "xmark").font(.system(size: 9)) }
                .help("Close tab").accessibilityLabel("Close " + session.title)
        }
        .padding(6).background(selected ? Color.accentColor.opacity(0.15) : .clear, in: RoundedRectangle(cornerRadius: 4))
    }
}
