import Darwin
import Foundation
import Testing

@testable import Memex

struct ConversationProviderContextCatalogTests {
  @Test func codexReadsOnlySelectedWorkspaceAndEnabledInstalledEntries() throws {
    var catalog = ConversationProviderContextCatalog()
    try catalog.readCodexSkills(
      [
        "data": [
          [
            "cwd": "/work",
            "skills": [
              [
                "name": "one", "description": "description", "path": "/home/skills/one/SKILL.md",
                "enabled": true, "pluginId": "tools@market",
              ],
              ["name": "disabled", "path": "/disabled", "enabled": false],
            ], "errors": [["message": "unreadable"]],
          ],
          ["cwd": "/other", "skills": [["name": "other", "path": "/other", "enabled": true]]],
        ]
      ], cwd: "/work")
    try catalog.readCodexPlugins([
      "marketplaces": [
        [
          "plugins": [
            ["id": "tools@market", "name": "tools", "installed": true, "enabled": true],
            ["id": "uninstalled", "name": "uninstalled", "installed": false, "enabled": true],
            ["id": "disabled", "name": "disabled", "installed": true, "enabled": false],
          ]
        ]
      ]
    ])
    #expect(catalog.skills.map(\.name) == ["one"])
    #expect(catalog.skills.first?.pluginID == "tools@market")
    #expect(catalog.plugins.map(\.id) == ["tools@market"])
    #expect(catalog.issues.count == 1)
  }

  @Test func claudeFollowsLinksWithoutCyclesAndHonorsPluginScope() throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    let shared = fixture.root.appendingPathComponent("shared")
    try fixture.skill(at: shared.appendingPathComponent("linked"), name: "linked")
    let roots = fixture.root.appendingPathComponent("skills")
    try FileManager.default.createDirectory(at: roots, withIntermediateDirectories: true)
    try FileManager.default.createSymbolicLink(
      at: roots.appendingPathComponent("linked"),
      withDestinationURL: shared.appendingPathComponent("linked"))
    try FileManager.default.createSymbolicLink(
      at: roots.appendingPathComponent("cycle"), withDestinationURL: roots)
    let plugin = fixture.root.appendingPathComponent("plugin")
    try fixture.skill(at: plugin.appendingPathComponent("skills/review"), name: "review")
    var catalog = ConversationProviderContextCatalog()
    try catalog.readClaudeSkills(roots: [roots], pluginID: nil)
    let data = try JSONSerialization.data(withJSONObject: [
      [
        "id": "active@market", "enabled": true, "scope": "project",
        "projectPath": fixture.root.path, "installPath": plugin.path,
      ],
      [
        "id": "other@market", "enabled": true, "scope": "project", "projectPath": "/another",
        "installPath": plugin.path,
      ],
      ["id": "disabled@market", "enabled": false, "installPath": plugin.path],
      [
        "id": "overridden@market", "enabled": true, "projectEnabled": false,
        "installPath": plugin.path,
      ],
      ["id": "shadowed@synced", "enabled": true, "installPath": ""],
    ])
    try catalog.readClaudePlugins(data, cwd: fixture.root.path)
    #expect(catalog.skills.map(\.name) == ["linked", "active:review"])
    #expect(catalog.skills.first?.path == roots.appendingPathComponent("linked/SKILL.md").path)
    #expect(catalog.skills.last?.pluginID == "active@market")
    #expect(catalog.plugins.map(\.id) == ["active@market"])
    #expect(catalog.issues.isEmpty)
  }

  @Test func frontmatterHandlesFoldedDescriptionsAndNonInvocableSkills() throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    let directory = fixture.root.appendingPathComponent("hidden")
    try fixture.skill(at: directory, name: "hidden", extra: "user-invocable: false\n")
    var catalog = ConversationProviderContextCatalog()
    try catalog.readClaudeSkills(roots: [fixture.root], pluginID: nil)
    #expect(catalog.skills.isEmpty)
    let fields = ConversationProviderContextCatalog.skillFields(
      "---\nname: 'review'\ndescription: >-\n  First line\n  second line\n---\n")
    #expect(fields["name"] == "review")
    #expect(fields["description"] == "First line second line")
  }

  @Test func codexSubprocessUsesScopedReadOnlyHandshakeAndUnsupportedFallback() async throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    try fixture.executable(
      """
      test "$1" = app-server || exit 9
      printf '%s\\n%s\\n' "$PWD" "$CODEX_HOME" > scope
      while IFS= read -r line; do
        printf '%s\\n' "$line" >> requests
        case "$line" in
          *'"method":"initialize"'*) echo '{"id":1,"result":{}}' ;;
          *'"method":"skills/list"'*) echo '\(try fixture.skillsResponse())' ;;
          *'"method":"plugin/installed"'*) echo '{"id":3,"error":{"code":-32601}}' ;;
          *'"method":"plugin/list"'*) echo '{"id":4,"result":{"marketplaces":[{"plugins":[{"id":"tools@market","name":"tools","installed":true,"enabled":true}]}]}}' ;;
        esac
      done
      """)
    let catalog = try await ConversationProviderContextCatalog.load(
      installation: fixture.installation(.codex))
    #expect(catalog.skills.map(\.name) == ["fixture"])
    #expect(catalog.plugins.map(\.id) == ["tools@market"])
    #expect(catalog.issues.isEmpty)
    let requests = try String(
      contentsOf: fixture.root.appendingPathComponent("requests"), encoding: .utf8)
    let messages = try requests.split(separator: "\n").map {
      try JSONSerialization.jsonObject(with: Data($0.utf8)) as! [String: Any]
    }
    #expect(
      messages.compactMap { $0["method"] as? String } == [
        "initialize", "initialized", "skills/list", "plugin/installed", "plugin/list",
      ])
    #expect((messages[2]["params"] as? [String: Any])?["cwds"] as? [String] == [fixture.root.path])
    #expect((messages[2]["params"] as? [String: Any])?["forceReload"] as? Bool == true)
    let scope = try String(
      contentsOf: fixture.root.appendingPathComponent("scope"), encoding: .utf8)
    let scopedPaths = scope.split(separator: "\n").map {
      URL(fileURLWithPath: String($0)).resolvingSymlinksInPath().path
    }
    #expect(scopedPaths == [fixture.root.path, fixture.root.path])
  }

  @Test func pluginFailurePreservesSkillsWithoutFallbackForOtherErrors() async throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    try fixture.executable(
      """
      while IFS= read -r line; do
        printf '%s\\n' "$line" >> requests
        case "$line" in
          *'"method":"initialize"'*) echo '{"id":1,"result":{}}' ;;
          *'"method":"skills/list"'*) echo '\(try fixture.skillsResponse())' ;;
          *'"method":"plugin/installed"'*) echo '{"id":3,"error":{"code":-32603}}' ;;
        esac
      done
      """)
    let catalog = try await ConversationProviderContextCatalog.load(
      installation: fixture.installation(.codex))
    #expect(catalog.skills.count == 1)
    #expect(catalog.issues.count == 1)
    let requests = try String(
      contentsOf: fixture.root.appendingPathComponent("requests"), encoding: .utf8)
    #expect(!requests.contains("plugin/list"))
  }

  @Test func claudeNativeListUsesSelectedHomeAndWorkspace() async throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    try fixture.skill(at: fixture.root.appendingPathComponent("skills/personal"), name: "personal")
    try fixture.skill(
      at: fixture.root.appendingPathComponent(".agents/skills/shared"), name: "shared")
    try fixture.executable(
      """
      test "$1 $2 $3" = 'plugin list --json' || exit 9
      test "$(cd "$CLAUDE_CONFIG_DIR" && pwd -P)" = "$(pwd -P)" || exit 10
      test "$HOME" = "$CLAUDE_CONFIG_DIR" || exit 11
      echo '[]'
      """)
    let catalog = try await ConversationProviderContextCatalog.load(
      installation: fixture.installation(.claude))
    // Shared .agents directories are import sources, not native Claude skill roots.
    #expect(catalog.skills.map(\.name) == ["personal"])
    #expect(catalog.issues.isEmpty)
  }

  @Test func claudePersonalSkillWinsAndLocalPluginsRetainNativeNamespace() async throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    try fixture.skill(at: fixture.root.appendingPathComponent("skills/review"), name: "review")
    let workspaceSkill = fixture.root.appendingPathComponent(".claude/skills/review")
    try fixture.skill(at: workspaceSkill, name: "review")
    let plugin = fixture.root.appendingPathComponent("local-plugin")
    try FileManager.default.createDirectory(
      at: plugin.appendingPathComponent(".claude-plugin"), withIntermediateDirectories: true)
    try Data(#"{"name":"native-tools","description":"Local tools"}"#.utf8)
      .write(to: plugin.appendingPathComponent(".claude-plugin/plugin.json"))
    try fixture.skill(at: plugin.appendingPathComponent("skills/audit"), name: "audit")
    try ProviderLocalPlugins.save([plugin.path], home: fixture.root.path)
    defer {
      UserDefaults.standard.removeObject(forKey: ProviderLocalPlugins.key(home: fixture.root.path))
    }
    try fixture.executable("echo '[]'")
    let catalog = try await ConversationProviderContextCatalog.load(
      installation: fixture.installation(.claude))
    #expect(catalog.skills.map(\.name) == ["native-tools:audit", "review"])
    #expect(
      catalog.skills.last?.path
        == fixture.root.appendingPathComponent("skills/review/SKILL.md").path)
    #expect(catalog.skills.first?.pluginID == "native-tools")
    #expect(catalog.plugins.map(\.id) == ["native-tools"])
    #expect(catalog.issues.isEmpty)
  }

  @Test func timeoutAndOutputLimitStopAndReapDiscovery() async throws {
    for oversized in [false, true] {
      let fixture = try Fixture()
      defer { fixture.remove() }
      try fixture.executable(
        oversized
          ? "echo $$ > pid\nwhile :; do printf '0123456789012345678901234567890123456789'; done"
          : "echo $$ > pid\nexec /bin/sleep 30")
      let catalog = try await ConversationProviderContextCatalog.load(
        installation: fixture.installation(.claude),
        timeout: oversized ? 3 : 1, outputLimit: 1_024)
      #expect(catalog.issues.count == 1)
      #expect(catalog.issues[0].contains(oversized ? "inventory limit" : "timed out"))
      let pid = try fixture.pid()
      #expect(kill(pid, 0) == -1)
    }
  }

  @Test func claudePluginManifestAddsCustomDirectoriesAndOwnsNamespace() throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    let plugin = fixture.root.appendingPathComponent("plugin")
    try fixture.manifest(
      at: plugin,
      fields: ["name": "native-name", "skills": ["./extra", "./direct", "./../outside"]])
    try fixture.skill(at: plugin.appendingPathComponent("skills/default"), name: "default")
    try fixture.skill(at: plugin.appendingPathComponent("extra/custom"), name: "custom")
    try fixture.skill(at: plugin.appendingPathComponent("direct"), name: "native-name:direct")
    try fixture.skill(at: fixture.root.appendingPathComponent("outside"), name: "outside")
    let rows: [[String: Any]] = [
      ["id": "installed-name@market", "enabled": true, "installPath": plugin.path]
    ]
    var catalog = ConversationProviderContextCatalog()
    try catalog.readClaudePlugins(
      JSONSerialization.data(withJSONObject: rows), cwd: fixture.root.path)
    #expect(
      Set(catalog.skills.map(\.name)) == [
        "native-name:default", "native-name:custom", "native-name:direct",
      ])
    #expect(catalog.skills.allSatisfy { $0.pluginID == "installed-name@market" })
    #expect(catalog.plugins.first?.id == "installed-name@market")
    #expect(catalog.plugins.first?.name == "native-name")
    #expect(catalog.issues.count == 1)
  }

  @Test func claudePluginRootSkillSupportsExplicitAndImplicitManifestForms() throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    for (index, path) in [nil, ".", "./", "./custom"].enumerated() {
      let plugin = fixture.root.appendingPathComponent("plugin-\(index)")
      var manifest: [String: Any] = ["name": "native-\(index)"]
      if let path { manifest["skills"] = path }
      try fixture.manifest(at: plugin, fields: manifest)
      let skillRoot = path == "./custom" ? plugin.appendingPathComponent("custom") : plugin
      try fixture.skill(at: skillRoot, name: "root-skill")
      var catalog = ConversationProviderContextCatalog()
      try catalog.readClaudeLocalPlugins(paths: [plugin.path])
      #expect(catalog.skills.map(\.name) == ["native-\(index):root-skill"])
      #expect(catalog.skills.first?.path == skillRoot.appendingPathComponent("SKILL.md").path)
      #expect(catalog.issues.isEmpty)
    }
  }

  @Test func repeatedDiscoveryReapsProcessesThatExitBeforeCleanup() async throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    try fixture.executable("echo $$ > pid\necho '[]'")
    for _ in 0..<20 {
      let catalog = try await ConversationProviderContextCatalog.load(
        installation: fixture.installation(.claude))
      #expect(catalog.issues.isEmpty)
      #expect(kill(try fixture.pid(), 0) == -1)
      await Task.yield()
    }
  }

  @Test func cancellationStopsAndReapsDiscovery() async throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    try fixture.executable("echo $$ > pid\nexec /bin/sleep 30")
    let installation = fixture.installation(.claude)
    let task = Task {
      try await ConversationProviderContextCatalog.load(installation: installation)
    }
    for _ in 0..<100 {
      if FileManager.default.fileExists(atPath: fixture.root.appendingPathComponent("pid").path) {
        break
      }
      try await Task.sleep(for: .milliseconds(10))
    }
    task.cancel()
    do {
      _ = try await task.value
      Issue.record("Discovery ignored cancellation")
    } catch is CancellationError {}
    #expect(kill(try fixture.pid(), 0) == -1)
  }

  @Test func closedProviderInputReturnsAnErrorWithoutTerminatingTheApp() async throws {
    let fixture = try Fixture()
    defer { fixture.remove() }
    try fixture.executable(
      """
      echo $$ > pid
      IFS= read -r line
      exec 0<&-
      echo '{"id":1,"result":{}}'
      exec /bin/sleep 30
      """)
    do {
      _ = try await ConversationProviderContextCatalog.load(
        installation: fixture.installation(.codex))
      Issue.record("Expected the closed provider input to fail")
    } catch {
      #expect(kill(try fixture.pid(), 0) == -1)
    }
  }

  private struct Fixture {
    let root: URL
    init() throws {
      let directory = FileManager.default.temporaryDirectory.appendingPathComponent(
        "provider-context-test-" + UUID().uuidString
      )
      try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
      root = URL(fileURLWithPath: directory.path).resolvingSymlinksInPath()
    }
    func remove() { try? FileManager.default.removeItem(at: root) }
    func manifest(at plugin: URL, fields: [String: Any]) throws {
      try FileManager.default.createDirectory(
        at: plugin.appendingPathComponent(".claude-plugin"), withIntermediateDirectories: true)
      try JSONSerialization.data(withJSONObject: fields).write(
        to: plugin.appendingPathComponent(".claude-plugin/plugin.json"))
    }
    func skill(at directory: URL, name: String, extra: String = "") throws {
      try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
      try "---\nname: \(name)\ndescription: Fixture skill\n\(extra)---\nBody".write(
        to: directory.appendingPathComponent("SKILL.md"), atomically: true, encoding: .utf8)
    }
    func executable(_ body: String) throws {
      let url = root.appendingPathComponent("provider")
      try ("#!/bin/sh\n" + body + "\n").write(to: url, atomically: true, encoding: .utf8)
      try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: url.path)
    }
    func installation(_ provider: ProviderToolsInstallation.Provider) -> ProviderToolsInstallation {
      .init(
        provider: provider, executable: root.appendingPathComponent("provider").path,
        home: root.path,
        workingDirectory: root.path,
        claudeNativeConfiguration: .init(
          providerHome: root, userHome: root, explicitConfigDirectory: root.path))
    }
    func skillsResponse() throws -> String {
      let response: [String: Any] = [
        "id": 2,
        "result": [
          "data": [
            [
              "cwd": root.path,
              "skills": [
                [
                  "name": "fixture", "description": "Fixture", "enabled": true,
                  "path": root.appendingPathComponent("SKILL.md").path,
                ]
              ], "errors": [],
            ]
          ]
        ],
      ]
      return String(
        decoding: try JSONSerialization.data(
          withJSONObject: response, options: [.withoutEscapingSlashes]), as: UTF8.self)
    }
    func pid() throws -> Int32 {
      let text = try String(contentsOf: root.appendingPathComponent("pid"), encoding: .utf8)
        .trimmingCharacters(in: .whitespacesAndNewlines)
      return try #require(Int32(text))
    }
  }
}
