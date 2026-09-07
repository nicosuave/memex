import Foundation
import CryptoKit

struct ProjectSummary: Codable, Identifiable, Equatable, Sendable {
    let project: String
    let sessionCount: Int
    let lastAt: String?
    var id: String { project }
    enum CodingKeys: String, CodingKey {
        case project
        case sessionCount = "session_count", lastAt = "last_at"
    }
}

enum ProjectSort: String, Codable, CaseIterable, Sendable {
    case recent, conversations, name
    var title: String {
        switch self {
        case .recent: "Recent activity"
        case .conversations: "Most conversations"
        case .name: "Name"
        }
    }
}

struct ProjectCatalogSnapshot: Sendable {
    let recent: [ProjectSummary]
    let conversations: [ProjectSummary]
    let names: [ProjectSummary]
    let sort: ProjectSort
    let updatedAt: Date
    let cacheWarning: String?
    subscript(sort: ProjectSort) -> [ProjectSummary] {
        switch sort {
        case .recent: recent
        case .conversations: conversations
        case .name: names
        }
    }
}

/// All cache IO, decoding, aggregation and sorting stay on this actor. Each
/// machine keeps its last successful snapshot when another machine is offline.
actor ProjectCatalog {
    private struct Cache: Codable {
        let version: Int
        let root: String
        let updatedAt: Date
        var sort: ProjectSort
        let projects: [ProjectSummary]
        var machine: String?
    }
    private struct MachineCache: Codable {
        let root: String
        let machines: [MachineChoice]
    }
    private let directory: URL
    private let root: String
    private let rootDigest: String
    private let client: MemexClient
    private let fetch: @Sendable (String) async throws -> [ProjectSummary]
    private var caches: [String: Cache] = [:]
    private var sort = ProjectSort.recent
    private var sortWasSelected = false
    private var sortWasLoaded = false

    init(client: MemexClient, cacheDirectory: URL? = nil,
         fetch: (@Sendable () async throws -> [ProjectSummary])? = nil,
         machineFetch: (@Sendable (String) async throws -> [ProjectSummary])? = nil) {
        self.client = client
        root = URL(fileURLWithPath: client.root ?? NSHomeDirectory() + "/.memex").standardizedFileURL.path
        rootDigest = Self.digest(root)
        directory = cacheDirectory ?? FileManager.default.urls(for: .cachesDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("dev.memex.app", isDirectory: true)
        self.fetch = machineFetch ?? { machine in
            if let fetch { return try await fetch() }
            return try await client.projects(machine: machine)
        }
    }

    private static func digest(_ value: String) -> String {
        SHA256.hash(data: Data(value.utf8)).map { String(format: "%02x", $0) }.joined()
    }
    private func cacheURL(_ machine: String) -> URL {
        let suffix = machine == "local" ? "" : "-" + Self.digest(machine)
        return directory.appendingPathComponent("projects-\(rootDigest)\(suffix).json")
    }

    func loadCache(machine: String = "local") -> ProjectCatalogSnapshot? {
        guard readCache(machine) else { return nil }
        return combinedSnapshot(machines: [machine])
    }
    func loadCaches(machines: [String]) -> ProjectCatalogSnapshot? {
        for machine in machines where caches[machine] == nil { _ = readCache(machine) }
        return combinedSnapshot(machines: machines)
    }
    private func readCache(_ machine: String) -> Bool {
        guard let data = try? Data(contentsOf: cacheURL(machine)),
              var value = try? JSONDecoder().decode(Cache.self, from: data),
              value.version == 1, value.root == root, (value.machine ?? "local") == machine,
              Set(value.projects.map(\.project)).count == value.projects.count,
              value.projects.allSatisfy({ !$0.project.isEmpty && $0.sessionCount >= 0 }) else { return false }
        if sortWasSelected || sortWasLoaded { value.sort = sort }
        else { sort = value.sort; sortWasLoaded = true }
        caches[machine] = value
        if sortWasSelected { _ = save(value, machine: machine) }
        return true
    }

    func refresh(machine: String = "local") async throws -> ProjectCatalogSnapshot {
        let projects = try await fetch(machine)
        try Task.checkCancellation()
        let value = Cache(version: 1, root: root, updatedAt: Date(), sort: sort, projects: projects, machine: machine)
        caches[machine] = value
        let warning = save(value, machine: machine)
        return combinedSnapshot(machines: [machine], warning: warning)!
    }

    func setSort(_ value: ProjectSort) {
        guard !Task.isCancelled else { return }
        sortWasSelected = true
        sort = value
        for (machine, var cache) in caches {
            cache.sort = value
            caches[machine] = cache
            _ = save(cache, machine: machine)
        }
    }
    private func save(_ value: Cache, machine: String) -> String? {
        do {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
            try JSONEncoder().encode(value).write(to: cacheURL(machine), options: .atomic)
            return nil
        } catch { return "Projects updated, but the startup cache could not be saved: \(error.localizedDescription)" }
    }

    func combinedSnapshot(machines: [String], warning: String? = nil) -> ProjectCatalogSnapshot? {
        let values = machines.compactMap { caches[$0] }
        guard let updatedAt = values.map(\.updatedAt).min() else { return nil }
        let plain = ISO8601DateFormatter()
        let fractional = ISO8601DateFormatter()
        fractional.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        var totals: [String: ProjectSummary] = [:]
        var timestamps: [String: TimeInterval] = [:]
        for value in values {
            for item in value.projects {
                let time = item.lastAt.flatMap { fractional.date(from: $0) ?? plain.date(from: $0) }?.timeIntervalSince1970 ?? -.infinity
                let previous = totals[item.project]
                let newer = time > (timestamps[item.project] ?? -.infinity)
                totals[item.project] = ProjectSummary(project: item.project,
                    sessionCount: (previous?.sessionCount ?? 0) + item.sessionCount,
                    lastAt: newer || previous == nil ? item.lastAt : previous?.lastAt)
                timestamps[item.project] = max(time, timestamps[item.project] ?? -.infinity)
            }
        }
        let rows = Array(totals.values)
        func byName(_ a: ProjectSummary, _ b: ProjectSummary) -> Bool {
            let order = a.project.localizedStandardCompare(b.project)
            return order == .orderedSame ? a.project < b.project : order == .orderedAscending
        }
        let recent = rows.sorted {
            let a = timestamps[$0.project] ?? -.infinity
            let b = timestamps[$1.project] ?? -.infinity
            return a == b ? byName($0, $1) : a > b
        }
        let conversations = rows.sorted {
            if $0.sessionCount != $1.sessionCount { return $0.sessionCount > $1.sessionCount }
            let a = timestamps[$0.project] ?? -.infinity
            let b = timestamps[$1.project] ?? -.infinity
            return a == b ? byName($0, $1) : a > b
        }
        return ProjectCatalogSnapshot(recent: recent, conversations: conversations, names: rows.sorted(by: byName),
            sort: sort, updatedAt: updatedAt, cacheWarning: warning)
    }

    private var machinesURL: URL { directory.appendingPathComponent("machines-\(rootDigest).json") }
    func loadMachineCache() -> [MachineChoice]? {
        guard let data = try? Data(contentsOf: machinesURL),
              let cache = try? JSONDecoder().decode(MachineCache.self, from: data), cache.root == root,
              cache.machines.contains(where: { $0.id == "local" }),
              Set(cache.machines.map(\.id)).count == cache.machines.count else { return nil }
        return cache.machines
    }
    func refreshMachines() async throws -> [MachineChoice] {
        let machines = try await client.machines()
        try Task.checkCancellation()
        try? FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        if let data = try? JSONEncoder().encode(MachineCache(root: root, machines: machines)) {
            try? data.write(to: machinesURL, options: .atomic)
        }
        return machines
    }
}
