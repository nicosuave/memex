import SwiftUI

struct ConversationProviderSetupView: View {
    @Environment(\.dismiss) private var dismiss
    @State private var catalog = ConversationProviderCatalog()
    @State private var selection: String?
    @State private var name = ""
    @State private var executable = ""
    @State private var arguments = ""
    @State private var home = FileManager.default.homeDirectoryForCurrentUser.path
    @State private var homeKey = "HOME"
    @State private var error: String?
    @State private var showTools = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Conversation providers").font(.title2.weight(.semibold))
                Spacer()
                Button("Done") { dismiss() }.keyboardShortcut(.cancelAction)
            }
            Text("Codex and Claude Code use their native runtimes. Add another installed agent that speaks ACP over standard input and output.")
                .font(.callout).foregroundStyle(.secondary)
            ForEach(ConversationProviderCatalog.builtins) { provider in
                VStack(alignment: .leading, spacing: 3) {
                    HStack { Text(provider.name); Spacer(); Text(provider.transport).foregroundStyle(.secondary) }
                    Text(provider.capabilitySummary).font(.caption).foregroundStyle(.secondary)
                }
            }
            Button("Manage native plugins & MCP servers…") { showTools = true }
            Divider()
            HStack(alignment: .top, spacing: 18) {
                VStack(alignment: .leading) {
                    ForEach(catalog.configured) { provider in
                        VStack(alignment: .leading, spacing: 3) {
                            Button(provider.name) { select(provider) }.buttonStyle(.plain)
                                .fontWeight(selection == provider.id ? .semibold : .regular)
                            if provider.availabilityIssue != nil {
                                Text("Installation unavailable").font(.caption).foregroundStyle(.orange)
                            }
                        }
                    }
                    Button("Add provider") { reset() }
                }.frame(width: 160, alignment: .leading)
                VStack(alignment: .leading, spacing: 10) {
                    TextField("Provider name", text: $name)
                    TextField("Absolute path to ACP executable", text: $executable)
                    TextField("Arguments, one per line", text: $arguments, axis: .vertical).lineLimit(2...4)
                    TextField("Original provider home directory", text: $home)
                    TextField("Home environment variable", text: $homeKey)
                    Text("Use the agent's documented ACP arguments. The process inherits your environment with the home above. Sign in using the agent's own CLI. Model, permissions and resume support are negotiated; native steering is unavailable on this transport.")
                        .font(.caption).foregroundStyle(.secondary)
                    HStack {
                        if selection != nil { Button("Remove configuration") { remove() } }
                        Spacer()
                        Button("Save provider") { save() }.buttonStyle(.borderedProminent)
                    }
                    Text("Changes apply to new conversations. Existing chats retain their original provider home and executable.")
                        .font(.caption).foregroundStyle(.secondary)
                    Text("Imported Cursor, OpenCode, Pi and other history does not establish an executable integration. Add a documented ACP executable only when that installed agent supports ACP. Manage its tools through that agent's native settings.")
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
            if let error { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
        }
        .padding(24).frame(width: 700)
        .task { do { catalog = try .load() } catch { self.error = error.localizedDescription } }
        .sheet(isPresented: $showTools) { ProviderToolsSettingsView().frame(width: 740, height: 680) }
    }

    private func select(_ provider: ConfiguredConversationProvider) {
        selection = provider.id; name = provider.name; executable = provider.executablePath
        arguments = provider.arguments.joined(separator: "\n"); home = provider.homePath; homeKey = provider.homeEnvironmentKey
        error = nil
    }
    private func reset() {
        selection = nil; name = ""; executable = ""; arguments = ""
        home = FileManager.default.homeDirectoryForCurrentUser.path; homeKey = "HOME"; error = nil
    }
    private func save() {
        do {
            let provider = ConfiguredConversationProvider(id: selection ?? "acp:" + UUID().uuidString.lowercased(),
                name: name.trimmingCharacters(in: .whitespacesAndNewlines), executablePath: executable,
                arguments: arguments.split(separator: "\n", omittingEmptySubsequences: true).map(String.init),
                homePath: home, homeEnvironmentKey: homeKey)
            try provider.validate()
            var updated = catalog
            updated.configured.removeAll { $0.id == provider.id }
            updated.configured.append(provider)
            try updated.save(); catalog = updated; select(provider)
        } catch { self.error = error.localizedDescription }
    }
    private func remove() {
        do {
            var updated = catalog; updated.configured.removeAll { $0.id == selection }
            try updated.save(); catalog = updated; reset()
        } catch { self.error = error.localizedDescription }
    }
}
