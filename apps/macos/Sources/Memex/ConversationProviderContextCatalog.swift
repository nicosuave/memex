import Darwin
import Foundation

/// Discovery belongs to one executable, provider home and workspace. No conversation is created.
struct ConversationProviderContextCatalog: Sendable {
  struct Skill: Identifiable, Equatable, Sendable {
    let id: String
    let name: String
    let description: String
    let path: String
    let pluginID: String?
  }
  struct Plugin: Identifiable, Equatable, Sendable {
    let id: String
    let name: String
    let description: String
  }
  var skills: [Skill] = []
  var plugins: [Plugin] = []
  var issues: [String] = []

  static func load(installation: ProviderToolsInstallation) async throws -> Self {
    try await load(installation: installation, timeout: 15, outputLimit: 2_000_000)
  }

  // Internal limits also make lifecycle behavior testable with real fixture executables.
  static func load(installation: ProviderToolsInstallation, timeout: TimeInterval, outputLimit: Int)
    async throws -> Self
  {
    try installation.validate()
    try Task.checkCancellation()
    switch installation.provider {
    case .codex:
      let process = try DiscoveryProcess(
        installation: installation,
        arguments: ["app-server"], timeout: timeout, outputLimit: outputLimit)
      defer { process.close() }
      _ = try await process.request(
        id: 1, method: "initialize",
        params: [
          "clientInfo": ["name": "memex-context-catalog", "version": "1.0"],
          "capabilities": ["experimentalApi": true],
        ])
      try process.send(["method": "initialized", "params": [:]])
      var catalog = Self()
      do {
        let response = try await process.request(
          id: 2, method: "skills/list",
          params: ["cwds": [installation.workingDirectory], "forceReload": true])
        try catalog.readCodexSkills(response, cwd: installation.workingDirectory)
      } catch is CancellationError { throw CancellationError() } catch {
        catalog.issues.append("Skills: \(error.localizedDescription)")
      }
      do {
        let response: [String: Any]
        do {
          response = try await process.request(
            id: 3, method: "plugin/installed",
            params: ["cwds": [installation.workingDirectory]])
        } catch let error as DiscoveryRPCError where error.code == -32601 {
          response = try await process.request(
            id: 4, method: "plugin/list",
            params: ["cwds": [installation.workingDirectory], "forceRefetch": false])
        }
        try catalog.readCodexPlugins(response)
      } catch is CancellationError { throw CancellationError() } catch {
        catalog.issues.append("Plugins: \(error.localizedDescription)")
      }
      return catalog.sorted()
    case .claude:
      var catalog = Self()
      let cwd = URL(fileURLWithPath: installation.workingDirectory)
      // Claude's personal skills win over project skills with the same command name.
      var roots = [URL(fileURLWithPath: installation.home).appendingPathComponent("skills")]
      // Project discovery stops at the repository/worktree boundary.
      var ancestor = cwd
      while ancestor.path != "/" {
        roots.append(ancestor.appendingPathComponent(".claude/skills"))
        if FileManager.default.fileExists(atPath: ancestor.appendingPathComponent(".git").path) {
          break
        }
        ancestor.deleteLastPathComponent()
      }
      try catalog.readClaudeSkills(roots: roots, pluginID: nil)
      try catalog.readClaudeLocalPlugins(paths: ProviderLocalPlugins.paths(home: installation.home))
      do {
        let process = try DiscoveryProcess(
          installation: installation,
          arguments: ["plugin", "list", "--json"], timeout: timeout, outputLimit: outputLimit)
        defer { process.close() }
        let data = try await process.completedOutput()
        try catalog.readClaudePlugins(data, cwd: installation.workingDirectory)
      } catch is CancellationError { throw CancellationError() } catch {
        catalog.issues.append("Plugins: \(error.localizedDescription)")
      }
      return catalog.sorted(deduplicateSkillNames: true)
    }
  }

  mutating func readCodexSkills(_ response: [String: Any], cwd: String) throws {
    guard let entries = response["data"] as? [[String: Any]] else { throw Self.invalidInventory() }
    for entry in entries where entry["cwd"] as? String == cwd {
      guard let rows = entry["skills"] as? [[String: Any]] else { throw Self.invalidInventory() }
      for row in rows where row["enabled"] as? Bool == true {
        guard let name = row["name"] as? String, let path = row["path"] as? String,
          path.hasPrefix("/"), !path.contains("\0")
        else { continue }
        skills.append(
          .init(
            id: path, name: name, description: row["description"] as? String ?? "",
            path: path, pluginID: row["pluginId"] as? String))
      }
      let errors = entry["errors"] as? [[String: Any]] ?? []
      if !errors.isEmpty {
        issues.append("Codex could not load \(errors.count) skill(s) in this workspace.")
      }
    }
  }

  mutating func readCodexPlugins(_ response: [String: Any]) throws {
    guard let marketplaces = response["marketplaces"] as? [[String: Any]] else {
      throw Self.invalidInventory()
    }
    for marketplace in marketplaces {
      guard let rows = marketplace["plugins"] as? [[String: Any]] else {
        throw Self.invalidInventory()
      }
      for row in rows where row["installed"] as? Bool == true && row["enabled"] as? Bool == true {
        guard let id = row["id"] as? String, let name = row["name"] as? String else { continue }
        let interface = row["interface"] as? [String: Any]
        plugins.append(
          .init(
            id: id, name: interface?["displayName"] as? String ?? name,
            description: interface?["shortDescription"] as? String ?? ""))
      }
    }
    let errors = response["marketplaceLoadErrors"] as? [[String: Any]] ?? []
    if !errors.isEmpty {
      issues.append("Codex could not load \(errors.count) plugin marketplace(s).")
    }
  }

  mutating func readClaudePlugins(_ data: Data, cwd: String) throws {
    guard let rows = try JSONSerialization.jsonObject(with: data) as? [[String: Any]] else {
      throw Self.invalidInventory()
    }
    for row in rows {
      guard row["enabled"] as? Bool == true, row["projectEnabled"] as? Bool != false,
        let id = row["id"] as? String, !id.isEmpty,
        let path = row["installPath"] as? String, path.hasPrefix("/"), !path.contains("\0")
      else { continue }
      let scope = row["scope"] as? String ?? "user"
      if let project = row["projectPath"] as? String, project != cwd { continue }
      if ["project", "local"].contains(scope), row["projectPath"] as? String != cwd { continue }
      try readClaudePlugin(root: URL(fileURLWithPath: path), installedID: id)
    }
  }

  mutating func readClaudeLocalPlugins(paths: [String]) throws {
    for path in paths {
      try Task.checkCancellation()
      guard path.hasPrefix("/"), !path.contains("\0") else { continue }
      try readClaudePlugin(root: URL(fileURLWithPath: path), installedID: nil)
    }
  }

  /// Native plugin IDs identify installation; manifest names independently own command namespaces.
  private mutating func readClaudePlugin(root: URL, installedID: String?) throws {
    try Task.checkCancellation()
    do {
      let manifestURL = root.appendingPathComponent(".claude-plugin/plugin.json")
      var manifest: [String: Any] = [:]
      if FileManager.default.fileExists(atPath: manifestURL.path) {
        let handle = try FileHandle(forReadingFrom: manifestURL)
        defer { try? handle.close() }
        let data = try handle.read(upToCount: 65_537) ?? Data()
        guard data.count <= 65_536,
          let object = try JSONSerialization.jsonObject(with: data) as? [String: Any]
        else {
          throw Self.invalidInventory()
        }
        manifest = object
      }
      let fallback =
        installedID.map { String($0.split(separator: "@", maxSplits: 1).first ?? Substring($0)) }
        ?? root.lastPathComponent
      let namespace = (manifest["name"] as? String)?.nilIfBlank ?? fallback
      let id = installedID ?? namespace
      plugins.append(
        .init(id: id, name: namespace, description: manifest["description"] as? String ?? ""))
      let defaultRoot = root.appendingPathComponent("skills")
      var roots = [defaultRoot]
      if let configured = manifest["skills"] {
        let paths: [String]
        if let path = configured as? String {
          paths = [path]
        } else if let values = configured as? [String] {
          paths = values
        } else {
          throw Self.invalidInventory()
        }
        for path in paths {
          let candidate = root.appendingPathComponent(path).standardizedFileURL
          var isDirectory: ObjCBool = false
          guard path == "." || path.hasPrefix("./"), !path.contains("\0"),
            candidate.resolvingSymlinksInPath().pathComponents.starts(
              with: root.resolvingSymlinksInPath().pathComponents),
            FileManager.default.fileExists(atPath: candidate.path, isDirectory: &isDirectory),
            isDirectory.boolValue
          else {
            issues.append("Plugin \(namespace) has an unavailable skill directory.")
            continue
          }
          roots.append(candidate)
        }
      } else if !FileManager.default.fileExists(atPath: defaultRoot.path),
        FileManager.default.fileExists(atPath: root.appendingPathComponent("SKILL.md").path)
      {
        roots.append(root)
      }
      try readClaudeSkills(roots: roots, pluginID: id, namespace: namespace, containedIn: root)
    } catch is CancellationError { throw CancellationError() } catch {
      issues.append("Could not read plugin at \(root.path).")
    }
  }

  /// Follow linked skill directories, but visit every resolved directory only once per namespace.
  mutating func readClaudeSkills(
    roots: [URL], pluginID: String?, namespace: String? = nil, containedIn: URL? = nil
  ) throws {
    var visited = Set<String>()
    var pending = roots.reversed().map { ($0, 0) }
    var inspected = 0
    while let (directory, depth) = pending.popLast() {
      try Task.checkCancellation()
      let resolved = directory.resolvingSymlinksInPath().standardizedFileURL
      if let containedIn,
        !resolved.pathComponents.starts(with: containedIn.resolvingSymlinksInPath().pathComponents)
      {
        continue
      }
      guard visited.insert(resolved.path).inserted else { continue }
      inspected += 1
      guard inspected <= 5_000, skills.count < 1_000 else {
        issues.append("Skill discovery reached its inventory limit.")
        return
      }
      guard FileManager.default.fileExists(atPath: resolved.path) else { continue }
      do {
        let skillURL = directory.appendingPathComponent("SKILL.md")
        if let containedIn,
          !skillURL.resolvingSymlinksInPath().pathComponents.starts(
            with: containedIn.resolvingSymlinksInPath().pathComponents)
        {
          continue
        }
        if FileManager.default.fileExists(atPath: skillURL.path) {
          let handle = try FileHandle(forReadingFrom: skillURL)
          defer { try? handle.close() }
          let data = try handle.read(upToCount: 65_536) ?? Data()
          let text = String(decoding: data, as: UTF8.self)
          let fields = Self.skillFields(text)
          if !["false", "no", "off", "0"].contains(fields["user-invocable"]?.lowercased() ?? "true")
          {
            let baseName = fields["name"]?.nilIfBlank ?? directory.lastPathComponent
            let prefix =
              namespace
              ?? pluginID.map {
                String($0.split(separator: "@", maxSplits: 1).first ?? Substring($0))
              }
            let name =
              prefix.map { baseName.hasPrefix($0 + ":") ? baseName : $0 + ":" + baseName }
              ?? baseName
            let path = skillURL.standardizedFileURL.path
            skills.append(
              .init(
                id: (pluginID ?? "") + ":" + path, name: name,
                description: fields["description"] ?? "", path: path, pluginID: pluginID))
          }
          continue
        }
        guard depth < 32 else {
          issues.append("Skill directory nesting exceeds the discovery limit.")
          continue
        }
        let children = try FileManager.default.contentsOfDirectory(
          at: directory,
          includingPropertiesForKeys: [.isDirectoryKey], options: [.skipsHiddenFiles])
        for child in children.sorted(by: { $0.path > $1.path }) {
          var isDirectory: ObjCBool = false
          if FileManager.default.fileExists(atPath: child.path, isDirectory: &isDirectory),
            isDirectory.boolValue
          {
            pending.append((child, depth + 1))
          }
        }
      } catch is CancellationError { throw CancellationError() } catch {
        issues.append("Could not read skills at \(directory.path).")
      }
    }
  }

  static func skillFields(_ text: String) -> [String: String] {
    let lines = text.components(separatedBy: .newlines)
    guard lines.first == "---" else { return [:] }
    var fields: [String: String] = [:]
    var multiline: String?
    for line in lines.dropFirst() {
      if line == "---" { break }
      if let key = multiline, line.first?.isWhitespace == true {
        fields[key, default: ""] +=
          (fields[key, default: ""].isEmpty ? "" : " ") + line.trimmingCharacters(in: .whitespaces)
        continue
      }
      multiline = nil
      guard let colon = line.firstIndex(of: ":") else { continue }
      let key = String(line[..<colon])
      guard ["name", "description", "user-invocable"].contains(key) else { continue }
      let value = String(line[line.index(after: colon)...]).trimmingCharacters(in: .whitespaces)
        .trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
      if ["|", ">", "|-", ">-", "|+", ">+"].contains(value) {
        multiline = key
        fields[key] = ""
      } else {
        fields[key] = value
      }
    }
    return fields
  }

  private func sorted(deduplicateSkillNames: Bool = false) -> Self {
    var result = self
    var skillIDs = Set<String>()
    var pluginIDs = Set<String>()
    result.skills = skills.filter {
      skillIDs.insert(deduplicateSkillNames ? $0.name : $0.id).inserted
    }
    .sorted { $0.name < $1.name }
    result.plugins = plugins.filter { pluginIDs.insert($0.id).inserted }.sorted {
      $0.name < $1.name
    }
    return result
  }
  private static func invalidInventory() -> ConversationRuntimeError {
    .init(message: "The selected provider returned an unsupported context inventory.")
  }
}

private struct DiscoveryRPCError: LocalizedError {
  let code: Int
  var errorDescription: String? {
    "The selected provider rejected context discovery (RPC \(code))."
  }
}

/// Task-confined short-lived process. Files avoid blocked pipe writers; polling bounds the file,
/// total deadline and cancellation. Close always kills and reaps before removing its private files.
private final class DiscoveryProcess {
  private let process = Process()
  private let exited = DispatchSemaphore(value: 0)
  private let input = Pipe()
  private let directory: URL
  private let outputURL: URL
  private let output: FileHandle
  private let reader: FileHandle
  private let deadline: Date
  private let outputLimit: Int
  private var received = Data()
  private var totalBytes = 0

  init(
    installation: ProviderToolsInstallation, arguments: [String], timeout: TimeInterval,
    outputLimit: Int
  ) throws {
    directory = FileManager.default.temporaryDirectory.appendingPathComponent(
      "memex-context-" + UUID().uuidString)
    outputURL = directory.appendingPathComponent("output")
    deadline = Date().addingTimeInterval(timeout)
    self.outputLimit = outputLimit
    try FileManager.default.createDirectory(
      at: directory, withIntermediateDirectories: false,
      attributes: [.posixPermissions: 0o700])
    do {
      guard
        FileManager.default.createFile(
          atPath: outputURL.path, contents: nil,
          attributes: [.posixPermissions: 0o600])
      else {
        throw ConversationRuntimeError(message: "Could not prepare provider discovery output.")
      }
      output = try FileHandle(forWritingTo: outputURL)
      reader = try FileHandle(forReadingFrom: outputURL)
      process.executableURL = URL(fileURLWithPath: installation.executable)
      process.arguments = arguments
      process.currentDirectoryURL = URL(fileURLWithPath: installation.workingDirectory)
      process.environment = ClaudeNativeConfiguration.mergedEnvironment(
        installation.environment,
        inherited: ProcessInfo.processInfo.environment)
      process.standardInput = input
      process.standardOutput = output
      process.standardError = FileHandle.nullDevice
      process.terminationHandler = { [exited] _ in exited.signal() }
      // A provider can close stdin while still running. Keep that a write error,
      // rather than allowing SIGPIPE to terminate the entire native app.
      guard fcntl(input.fileHandleForWriting.fileDescriptor, F_SETNOSIGPIPE, 1) != -1 else {
        throw ConversationRuntimeError(message: "Could not prepare provider discovery input.")
      }
      try process.run()
    } catch {
      try? FileManager.default.removeItem(at: directory)
      throw error
    }
  }

  func close() {
    try? input.fileHandleForWriting.close()
    if process.isRunning { kill(process.processIdentifier, SIGKILL) }
    // Discovery can resume on a different cooperative thread after an await.
    // waitUntilExit spins a thread-local run loop and can miss termination there.
    // The handler is installed before launch and signals after Foundation reaps.
    exited.wait()
    try? reader.close()
    try? output.close()
    try? FileManager.default.removeItem(at: directory)
  }

  func send(_ object: [String: Any]) throws {
    guard process.isRunning else {
      throw ConversationRuntimeError(message: "Provider discovery exited before responding.")
    }
    var data = try JSONSerialization.data(
      withJSONObject: object, options: [.withoutEscapingSlashes])
    data.append(10)
    try input.fileHandleForWriting.write(contentsOf: data)
  }

  func request(id: Int, method: String, params: [String: Any]) async throws -> [String: Any] {
    try Task.checkCancellation()
    try send(["id": id, "method": method, "params": params])
    while true {
      try readAvailable()
      while let end = received.firstIndex(of: 10) {
        let line = received[..<end]
        received.removeSubrange(...end)
        guard !line.isEmpty else { continue }
        guard let message = try JSONSerialization.jsonObject(with: line) as? [String: Any] else {
          continue
        }
        guard message["id"] as? Int == id else { continue }
        if let error = message["error"] as? [String: Any] {
          throw DiscoveryRPCError(code: error["code"] as? Int ?? -1)
        }
        guard let result = message["result"] as? [String: Any] else {
          throw ConversationRuntimeError(
            message: "The provider returned an invalid discovery response.")
        }
        return result
      }
      guard process.isRunning else {
        throw ConversationRuntimeError(message: "Provider discovery exited before responding.")
      }
      try await Task.sleep(for: .milliseconds(25))
    }
  }

  func completedOutput() async throws -> Data {
    try input.fileHandleForWriting.close()
    while true {
      try readAvailable()
      if !process.isRunning {
        try readAvailable()
        guard process.terminationStatus == 0 else {
          throw ConversationRuntimeError(
            message: "Provider discovery failed (exit \(process.terminationStatus)).")
        }
        return received
      }
      try await Task.sleep(for: .milliseconds(25))
    }
  }

  private func readAvailable() throws {
    try Task.checkCancellation()
    guard Date() < deadline else {
      throw ConversationRuntimeError(message: "Provider context discovery timed out.")
    }
    let size =
      (try FileManager.default.attributesOfItem(atPath: outputURL.path)[.size] as? NSNumber)?
      .intValue ?? 0
    guard size <= outputLimit else {
      throw ConversationRuntimeError(
        message: "Provider context output exceeds the inventory limit.")
    }
    let bytes = try reader.read(upToCount: outputLimit - totalBytes + 1) ?? Data()
    totalBytes += bytes.count
    guard totalBytes <= outputLimit else {
      throw ConversationRuntimeError(
        message: "Provider context output exceeds the inventory limit.")
    }
    received.append(bytes)
  }
}
