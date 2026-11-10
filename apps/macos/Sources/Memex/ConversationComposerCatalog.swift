import Foundation

struct ConversationComposerCatalog: Sendable {
    struct File: Identifiable, Equatable, Sendable {
        let url: URL
        let relativePath: String
        var id: String { url.path }
    }
    struct Prompt: Identifiable, Equatable, Sendable {
        enum Kind: String, Sendable { case skill, command }
        let url: URL
        let name: String
        let description: String
        let kind: Kind
        var id: String { url.path }
    }
    var files: [File] = []
    var prompts: [Prompt] = []
    var truncated = false

    static func locallyAccessible(machineID: String, isServerOwned: Bool) -> Bool {
        machineID == "local" && !isServerOwned
    }

    static func load(
        workspace: URL?, providerHome: URL?, locallyAccessible: Bool, includePrompts: Bool = true,
                     userHome: URL = FileManager.default.homeDirectoryForCurrentUser) throws -> Self {
        guard locallyAccessible else { return Self() }
        var result = Self()
        let manager = FileManager.default
        if let workspace {
            let root = workspace.standardizedFileURL.resolvingSymlinksInPath()
            guard let enumerator = manager.enumerator(at: root,
                includingPropertiesForKeys: [.isRegularFileKey, .isDirectoryKey, .isSymbolicLinkKey],
                options: [.skipsHiddenFiles]) else {
                throw ConversationRuntimeError(message: "The workspace could not be searched for file mentions.")
            }
            let excluded: Set<String> = ["node_modules", "target", "build", "dist", "vendor", "DerivedData"]
            for case let enumeratedURL as URL in enumerator {
                if Task.isCancelled { throw CancellationError() }
                let values = try enumeratedURL.resourceValues(forKeys: [.isRegularFileKey, .isDirectoryKey, .isSymbolicLinkKey])
                if values.isSymbolicLink == true || (values.isDirectory == true && excluded.contains(enumeratedURL.lastPathComponent)) {
                    enumerator.skipDescendants()
                    continue
                }
                guard values.isRegularFile == true else { continue }
                if result.files.count == 5_000 { result.truncated = true; break }
                // Foundation enumeration may return /var URLs for a /private/var
                // root. Normalize both before deriving the relative mention path.
                let url = enumeratedURL.standardizedFileURL.resolvingSymlinksInPath()
                guard url.pathComponents.starts(with: root.pathComponents) else { continue }
                let relativePath = url.pathComponents.dropFirst(root.pathComponents.count).joined(separator: "/")
                result.files.append(.init(url: url, relativePath: relativePath))
            }
            result.files.sort { $0.relativePath.localizedStandardCompare($1.relativePath) == .orderedAscending }
        }
        guard includePrompts else { return result }
        let skills = [
            workspace?.appendingPathComponent(".agents/skills"),
            workspace?.appendingPathComponent(".claude/skills"),
            userHome.appendingPathComponent(".agents/skills"),
            providerHome?.appendingPathComponent("skills"),
        ].compactMap { $0 }
        let commands = [workspace?.appendingPathComponent(".claude/commands"), providerHome?.appendingPathComponent("commands")].compactMap { $0 }
        var seen: Set<String> = []
        for (roots, kind) in [(skills, Prompt.Kind.skill), (commands, .command)] {
            for root in roots.map({ $0.standardizedFileURL.resolvingSymlinksInPath() }) where seen.insert(root.path).inserted {
                guard let enumerator = manager.enumerator(at: root, includingPropertiesForKeys: [.isRegularFileKey], options: [.skipsHiddenFiles]) else { continue }
                for case let enumeratedURL as URL in enumerator {
                    if Task.isCancelled { throw CancellationError() }
                    guard result.prompts.count < 1_000 else { result.truncated = true; break }
                    let url = enumeratedURL.standardizedFileURL.resolvingSymlinksInPath()
                    guard (kind == .skill ? url.lastPathComponent == "SKILL.md" : url.pathExtension == "md") else { continue }
                    guard (try? url.resourceValues(forKeys: [.isRegularFileKey]).isRegularFile) == true,
                          let handle = try? FileHandle(forReadingFrom: url) else { continue }
                    let data = try handle.read(upToCount: 16_384) ?? Data()
                    try handle.close()
                    let preview = String(decoding: data, as: UTF8.self)
                    let fallback = kind == .skill ? url.deletingLastPathComponent().lastPathComponent : url.deletingPathExtension().lastPathComponent
                    let fields = frontMatter(preview)
                    result.prompts.append(.init(url: url, name: fields["name"]?.nilIfBlank ?? fallback,
                        description: fields["description"] ?? "\(kind.rawValue.capitalized) from \(url.path)", kind: kind))
                }
            }
        }
        result.prompts.sort { $0.name.localizedStandardCompare($1.name) == .orderedAscending }
        return result
    }

    /// Only the simple front-matter display fields are needed; prompt bytes stay
    /// intact and are captured from the file only after explicit selection.
    static func frontMatter(_ text: String) -> [String: String] {
        let lines = text.components(separatedBy: .newlines)
        guard lines.first == "---" else { return [:] }
        var fields: [String: String] = [:]
        for line in lines.dropFirst() {
            if line == "---" { break }
            guard let colon = line.firstIndex(of: ":") else { continue }
            let key = String(line[..<colon]).trimmingCharacters(in: .whitespaces)
            guard key == "name" || key == "description" else { continue }
            let value = String(line[line.index(after: colon)...]).trimmingCharacters(in: .whitespaces)
                .trimmingCharacters(in: CharacterSet(charactersIn: "\"'"))
            if !["|", ">", "|-", ">-"].contains(value) { fields[key] = value }
        }
        return fields
    }

    func matchingFiles(_ query: String) -> [File] {
        Array(files.filter { query.isEmpty || $0.relativePath.localizedCaseInsensitiveContains(query) }.prefix(20))
    }

    func matchingPrompts(_ query: String) -> [Prompt] {
        Array(prompts.filter { query.isEmpty || $0.name.localizedCaseInsensitiveContains(query) || $0.description.localizedCaseInsensitiveContains(query) }.prefix(12))
    }
}

enum ConversationMentionContext {
    static func capture(_ session: Session, client: MemexClient = MemexClient()) async throws -> ConversationAttachment {
        let limit = 60
        let offset = max(0, (session.messageCount ?? limit) - limit)
        var records = try await client.records(for: session, offset: offset, limit: limit)
        if records.isEmpty, session.machineID == "local", ["codex", "claude"].contains(session.source) {
            records = Array(try await NewConversationRuntime.records(for: session).suffix(limit))
        }
        guard !records.isEmpty else { throw ConversationRuntimeError(message: "This conversation has no readable context yet.") }
        let text = "Conversation: \(session.title)\nProvider: \(session.source)\nMachine: \(session.machineID)\nExcerpt: \(records.count) records starting at indexed offset \(offset).\n\n"
            + records.map { "[\($0.record.role) · \($0.sourceID)]\n\($0.record.text)" }.joined(separator: "\n\n")
        return try .text(title: session.title, text: text, source: "memex://conversation/\(session.sessionID)")
    }
}
