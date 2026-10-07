import SwiftUI

/// Embeddable in the Settings window; a chat may supply its exact native installation.
struct ProviderToolsSettingsView: View {
    private let target: InAppResumeTarget?
    @State private var installation: ProviderToolsInstallation
    @State private var inventory: ProviderToolsInventory?
    @State private var pluginInventoryError: String?
    @State private var draft = ProviderMCPDraft()
    @State private var argumentLines = ""
    @State private var pluginName = ""
    @State private var optionKey = ""
    @State private var optionValue = ""
    @State private var localPluginLines = ""
    @State private var busy = false
    @State private var error: String?
    @State private var result: String?
    @State private var showProviders = false

    init(target: InAppResumeTarget? = nil) {
        self.target = target
        if let target, let provider = ProviderToolsInstallation.Provider(rawValue: target.session.source) {
            _installation = State(initialValue: .init(provider: provider, executable: target.executableURL.path,
                home: target.providerHome.path, workingDirectory: target.workingDirectory.path,
                claudeNativeConfiguration: target.claudeNativeConfiguration))
        } else { _installation = State(initialValue: .local(.codex)) }
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                HStack {
                    Text("Providers & tools").font(.title2.weight(.semibold))
                    Spacer()
                    Button("Conversation providers…") { showProviders = true }
                }
                Text("Manage the selected local installation using its native CLI. Changes apply to its future connections, including other apps using this home. Reconnect an idle chat to load changes.")
                    .font(.callout).foregroundStyle(.secondary)
                GroupBox("Installation") {
                    VStack(alignment: .leading, spacing: 10) {
                        Picker("Provider", selection: $installation.provider) {
                            Text("Codex").tag(ProviderToolsInstallation.Provider.codex)
                            Text("Claude Code").tag(ProviderToolsInstallation.Provider.claude)
                        }.disabled(target != nil)
                        TextField("CLI executable (absolute path)", text: $installation.executable)
                        TextField("Native provider home", text: $installation.home)
                        TextField("Workspace", text: $installation.workingDirectory)
                        Text("Host: local · Configuration: \(installation.configurationURL.path)")
                            .font(.caption).textSelection(.enabled)
                        HStack {
                            Button("Refresh inventory") { perform { try await refresh() } }
                            Button("Edit native configuration…") { openConfiguration() }
                        }
                    }.padding(8)
                }
                if let inventory {
                    GroupBox("Custom MCP servers") {
                        VStack(alignment: .leading, spacing: 10) {
                            if inventory.servers.isEmpty { Text("No custom servers configured.").foregroundStyle(.secondary) }
                            ForEach(inventory.servers) { server in
                                HStack {
                                    VStack(alignment: .leading) {
                                        Text(server.name)
                                        Text("\(server.kind) · \(server.scope) · \(server.status)").font(.caption).foregroundStyle(.secondary)
                                    }
                                    Spacer()
                                    Button("Configure…") { openConfiguration(scope: server.scope) }
                                    if ["http", "sse", "streamable_http"].contains(server.kind) {
                                        Button("Authenticate…") { perform {
                                            _ = try await run(ProviderToolsCommands.loginMCP(server))
                                            result = "Authentication completed."
                                            try await refresh()
                                        } }
                                        .disabled(inventory.servers.filter { $0.name == server.name }.count > 1)
                                        .help("For names configured in multiple scopes, authenticate through the native CLI after selecting the intended effective server.")
                                    }
                                    Button("Remove") { perform {
                                        _ = try await run(ProviderToolsCommands.removeMCP(server, provider: installation.provider))
                                        try await refresh()
                                    } }.disabled(!server.canRemove)
                                }
                            }
                        }.frame(maxWidth: .infinity, alignment: .leading).padding(8)
                    }
                    GroupBox("Installed plugins") {
                        VStack(alignment: .leading, spacing: 10) {
                            if let pluginInventoryError {
                                Text(pluginInventoryError).foregroundStyle(.orange)
                            } else if inventory.plugins.isEmpty { Text("No installed plugins.").foregroundStyle(.secondary) }
                            ForEach(inventory.plugins) { plugin in
                                HStack {
                                    VStack(alignment: .leading) {
                                        Text(plugin.name).textSelection(.enabled)
                                        Text("\(plugin.scope) · \(plugin.status)").font(.caption).foregroundStyle(.secondary)
                                    }
                                    Spacer()
                                    if installation.provider == .claude {
                                        Button("Configure") { pluginName = plugin.name }
                                    }
                                    Button("Remove") { perform {
                                        _ = try await run(ProviderToolsCommands.plugin(install: false, name: plugin.name,
                                            provider: installation.provider, scope: plugin.scope))
                                        try await refresh()
                                    } }.disabled(!plugin.canRemove)
                                }
                            }
                        }.frame(maxWidth: .infinity, alignment: .leading).padding(8)
                    }
                }
                GroupBox("Add custom MCP server") {
                    VStack(alignment: .leading, spacing: 10) {
                        TextField("Server name", text: $draft.name)
                        Picker("Transport", selection: $draft.transport) {
                            Text("Streamable HTTP").tag(ProviderMCPDraft.Transport.http)
                            Text("Standard input/output").tag(ProviderMCPDraft.Transport.stdio)
                        }
                        TextField(draft.transport == .http ? "Endpoint URL" : "Executable", text: $draft.endpoint)
                        if draft.transport == .stdio {
                            TextField("Arguments, one per line", text: $argumentLines, axis: .vertical).lineLimit(2...5)
                        } else if installation.provider == .codex {
                            TextField("Bearer token environment variable (optional)", text: $draft.bearerEnvironmentVariable)
                        }
                        Text("New servers are saved at user scope. Use Edit native configuration for existing server settings, headers, environment variables and project policy. Authentication opens the provider's browser flow.")
                            .font(.caption).foregroundStyle(.secondary)
                        Button("Add server") { perform {
                            try await refresh()
                            guard inventory?.servers.contains(where: { $0.name == draft.name }) == false else {
                                throw ConversationRuntimeError(message: "A server with this name already exists. Edit its native configuration to preserve existing options.")
                            }
                            var submitted = draft
                            submitted.arguments = argumentLines.components(separatedBy: "\n").filter { !$0.isEmpty }
                            if submitted.transport == .stdio { submitted.bearerEnvironmentVariable = "" }
                            _ = try await run(ProviderToolsCommands.addMCP(submitted, provider: installation.provider))
                            draft = .init(); argumentLines = ""
                            try await refresh()
                        } }
                    }.padding(8)
                }
                GroupBox("Install a plugin") {
                    VStack(alignment: .leading, spacing: 10) {
                        TextField("Exact plugin@marketplace identifier", text: $pluginName)
                        Button("Install plugin") { perform {
                            _ = try await run(ProviderToolsCommands.plugin(install: true, name: pluginName, provider: installation.provider))
                            try await refresh()
                        } }
                        Text("Uses marketplaces already configured in this installation. Availability and authentication depend on the provider and account. Command-based installs requiring interactive approval stay in the native CLI.")
                            .font(.caption).foregroundStyle(.secondary)
                        if installation.provider == .claude {
                            Divider()
                            Text("Configure the selected plugin option").font(.callout)
                            TextField("Option key from the plugin's documentation", text: $optionKey)
                            SecureField("Value", text: $optionValue)
                            Button("Save option") { perform {
                                let command = try ProviderToolsCommands.configureClaudePlugin(name: pluginName, key: optionKey, value: optionValue)
                                _ = try await run(command)
                                optionValue = ""; result = "Plugin option saved."
                            } }
                        }
                    }.padding(8)
                }
                if installation.provider == .claude {
                    GroupBox("Additional local Claude SDK plugins") {
                        VStack(alignment: .leading, spacing: 10) {
                            TextField("Absolute plugin directories, one per line", text: $localPluginLines, axis: .vertical).lineLimit(2...5)
                            Text("Memex loads these local plugin directories in Claude SDK sessions for this exact home. Installed marketplace plugins continue to use native Claude settings.")
                                .font(.caption).foregroundStyle(.secondary)
                            Button("Save local plugins") {
                                do {
                                    try ProviderLocalPlugins.save(localPluginLines.components(separatedBy: "\n").filter { !$0.isEmpty }, home: installation.home)
                                    error = nil; result = "Local plugins saved for future connections."
                                } catch { self.error = error.localizedDescription }
                            }
                        }.padding(8)
                    }
                }
                if busy { ProgressView().controlSize(.small) }
                if let error { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
                if let result { Text(result).foregroundStyle(.secondary) }
            }.padding(20).disabled(busy)
        }
        .sheet(isPresented: $showProviders) { ConversationProviderSetupView() }
        .onChange(of: installation.provider) { _, provider in
            installation = .local(provider); inventory = nil; error = nil; result = nil
            draft = .init(); localPluginLines = ProviderLocalPlugins.paths(home: installation.home).joined(separator: "\n")
        }
        .onChange(of: installation.home) { _, home in
            inventory = nil; localPluginLines = ProviderLocalPlugins.paths(home: home).joined(separator: "\n")
        }
        .onChange(of: installation.workingDirectory) { _, _ in inventory = nil }
        .onChange(of: installation.executable) { _, _ in inventory = nil }
        .onAppear { localPluginLines = ProviderLocalPlugins.paths(home: installation.home).joined(separator: "\n") }
    }

    private func openConfiguration(scope: String = "user") {
        do {
            try installation.validate()
            let url = installation.configurationURL(scope: scope)
            guard FileManager.default.fileExists(atPath: url.path) else {
                throw ConversationRuntimeError(message: "This installation has no native configuration file yet. Add a server first.")
            }
            NSWorkspace.shared.open(url)
        } catch { self.error = error.localizedDescription }
    }
    private func run(_ command: ProviderToolsCommand) async throws -> Data {
        try await ProviderToolsProcess.run(command, installation: installation)
    }
    private func refresh() async throws {
        try installation.validate()
        let servers: [ProviderToolEntry]
        if installation.provider == .codex {
            servers = try ProviderToolsInventory.codexServers(from: await run(.init(arguments: ["mcp", "list", "--json"])))
        } else { servers = try ProviderToolsInventory.claudeServers(installation: installation) }
        inventory = .init(servers: servers, plugins: [])
        do {
            inventory?.plugins = try ProviderToolsInventory.plugins(from: await run(.init(arguments: ["plugin", "list", "--json"])),
                provider: installation.provider, cwd: installation.workingDirectory)
            pluginInventoryError = nil
        } catch { pluginInventoryError = "Plugin inventory unavailable. " + error.localizedDescription }
    }
    private func perform(_ operation: @escaping @MainActor () async throws -> Void) {
        busy = true; error = nil; result = nil
        Task { @MainActor in
            defer { busy = false }
            do { try await operation() } catch { self.error = error.localizedDescription }
        }
    }
}
