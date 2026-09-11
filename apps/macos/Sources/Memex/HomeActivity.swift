import Charts
import Foundation
import SwiftUI

struct HomeActivityPayload: Decodable, Sendable {
    let metric: String
    let bucketKeys: [String]
    let tokenUsageEnabled: Bool
    let partial: Bool
    let points: [HomeActivityPoint]
    var warnings: [String]? = nil

    var total: Double { points.reduce(0) { $0 + $1.value } }
    enum CodingKeys: String, CodingKey {
        case metric, partial, points, warnings
        case bucketKeys = "bucket_keys", tokenUsageEnabled = "token_usage_enabled"
    }
}

struct HomeActivityPoint: Decodable, Sendable, Identifiable {
    let date: String
    let source: String
    let value: Double
    var id: String { "\(date)|\(source)" }
}

enum HomeActivityMetric: String, CaseIterable {
    case sessions, tokens
    var title: String { self == .sessions ? "Conversations" : "Tokens" }
}

extension ConversationTimeframe {
    var activityRange: String {
        switch self {
        case .all: "all"
        case .day: "24h"
        case .week: "7d"
        case .month: "30d"
        }
    }
}

struct RawHomeActivityPayload: Decodable, Sendable {
    let tokenUsageEnabled: Bool
    let partial: Bool
    let points: [RawHomeActivityPoint]
    var warnings: [String]? = nil
    enum CodingKeys: String, CodingKey {
        case partial, points, warnings
        case tokenUsageEnabled = "token_usage_enabled"
    }
}

struct RawHomeActivityPoint: Decodable, Sendable {
    let timestampMS: UInt64
    let source: String
    let value: Double
    enum CodingKeys: String, CodingKey {
        case source, value
        case timestampMS = "timestamp_ms"
    }
}

struct HomeActivitySelection: Sendable {
    let metric: String
    let timeframe: ConversationTimeframe
    let query: String?
    let project: String?
    let source: String?
    let origin: ConversationOrigin
    let nowMS: UInt64
}

struct HomeActivityBatch: Sendable {
    let machine: String
    let payload: RawHomeActivityPayload?
    let error: String?
}

extension MemexClient {
    func activity(_ selection: HomeActivitySelection, machine: String, progress: ActivityProgressHandler? = nil) async throws -> RawHomeActivityPayload {
        var args = ["activity", "--format", "json", "--metric", selection.metric, "--range", selection.timeframe.activityRange,
                    "--machine", machine, "--origin", selection.origin.argument, "--progress", "--raw", "--now-ms", String(selection.nowMS)]
        if let query = selection.query?.nilIfBlank { args += ["--query=\(query)"] }
        if let project = selection.project { args += ["--project", project] }
        if let source = selection.source { args += ["--source", source] }
        var request = DaemonRequest(op: "activity")
        request.machine = machine
        request.metric = selection.metric
        request.range = selection.timeframe.activityRange
        request.query = selection.query
        request.project = selection.project
        request.source = selection.source
        request.origin = selection.origin.argument
        request.nowMS = selection.nowMS
        return try JSONDecoder().decode(RawHomeActivityPayload.self, from: await run(args, daemonRequest: selection.metric == "tokens" ? nil : request, progress: progress))
    }

    @MainActor
    func activityBatches(_ selection: HomeActivitySelection, machines: [String],
                         progress: @escaping @MainActor @Sendable (String, ActivityScanProgress) -> Void = { _, _ in },
                         receive: (HomeActivityBatch) -> Void) async throws {
        try await withThrowingTaskGroup(of: HomeActivityBatch.self) { group in
            for machine in machines {
                group.addTask {
                    do {
                        let result = try await activity(selection, machine: machine) { update in
                            Task { @MainActor in progress(machine, update) }
                        }
                        return HomeActivityBatch(machine: machine, payload: result, error: nil)
                    } catch is CancellationError { throw CancellationError() } catch {
                        return HomeActivityBatch(machine: machine, payload: nil, error: error.localizedDescription)
                    }
                }
            }
            for try await batch in group {
                try Task.checkCancellation()
                receive(batch)
            }
        }
    }
}

extension HomeActivityPayload {
    /// Merge raw buckets, then compress once: per-machine all-time charts can have different grids.
    static func merge(_ batches: [RawHomeActivityPayload], selection: HomeActivitySelection, failed: Bool) -> Self {
        let unit: UInt64 = selection.timeframe == .day ? 3_600_000 : 86_400_000
        let end = selection.nowMS / unit * unit
        let days: UInt64?
        switch selection.timeframe {
        case .all: days = nil
        case .day: days = 1
        case .week: days = 7
        case .month: days = 30
        }
        let raw = batches.flatMap(\.points)
        let since = days.map { selection.nowMS > $0 * 86_400_000 ? selection.nowMS - $0 * 86_400_000 : 0 }
        let start = min(end, since.map { $0 / unit * unit } ?? raw.map(\.timestampMS).min() ?? end)
        let units = (end - start) / unit + 1
        let step = ((units + 59) / 60) * unit
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = unit < 86_400_000 ? "yyyy-MM-dd'T'HH:mm'Z'" : "yyyy-MM-dd"
        func label(_ timestamp: UInt64) -> String {
            formatter.string(from: Date(timeIntervalSince1970: Double(timestamp) / 1000))
        }
        let keys = stride(from: start, through: end, by: Int(step)).map(label)
        struct Bucket: Hashable { let timestamp: UInt64; let source: String }
        var totals: [Bucket: Double] = [:]
        for point in raw where point.timestampMS >= start && point.timestampMS <= end {
            let timestamp = start + (point.timestampMS - start) / step * step
            totals[Bucket(timestamp: timestamp, source: point.source), default: 0] += point.value
        }
        let points = totals.sorted {
            $0.key.timestamp == $1.key.timestamp ? $0.key.source < $1.key.source : $0.key.timestamp < $1.key.timestamp
        }.map { HomeActivityPoint(date: label($0.key.timestamp), source: $0.key.source, value: $0.value) }
        return HomeActivityPayload(metric: selection.metric, bucketKeys: keys,
            tokenUsageEnabled: batches.contains { $0.tokenUsageEnabled },
            partial: failed || batches.contains { $0.partial || (selection.metric == "tokens" && !$0.tokenUsageEnabled) }, points: points,
            warnings: batches.flatMap { $0.warnings ?? [] })
    }
}

struct HomeActivityCache {
    let criteria: String
    let request: String
    let payload: HomeActivityPayload
    let machines: [String: RawHomeActivityPayload]
    let failures: [String: String]
    let complete: Bool
    let updatedAt: Date

    func isFresh(for request: String, now: Date = Date()) -> Bool {
        complete && self.request == request && now.timeIntervalSince(updatedAt) < 60
    }
}

struct HomeActivityView: View {
    @Bindable var store: Store
    @State private var error: String?
    @State private var errorRequest: String?
    @State private var retry = 0
    @State private var remainingMachines = 0
    @State private var failedMachines: [String: String] = [:]
    @State private var pendingMachines = Set<String>()
    @State private var scanProgress: [String: ActivityScanProgress] = [:]

    private var metric: HomeActivityMetric { store.homeActivityMetric }
    private var requestID: String { "\(store.homeActivityRequestID)|\(metric)|\(retry)" }
    private var criteriaID: String { "\(store.homeActivityCriteriaID)|\(metric)" }
    private var currentPayload: HomeActivityPayload? {
        store.homeActivityCache?.criteria == criteriaID ? store.homeActivityCache?.payload : nil
    }
    private var currentError: String? { errorRequest == requestID ? error : nil }
    private var loadingDetail: String {
        let details = scanProgress.keys.sorted().compactMap { machine -> String? in
            guard let update = scanProgress[machine] else { return nil }
            let provider = ConversationProvider(rawValue: update.source)?.title ?? update.source
            return "\(machine): preparing \(provider) usage, \(update.done.formatted()) of \(update.total.formatted()) logs"
        }
        return details.isEmpty ? "Loading activity" : details.joined(separator: "\n")
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Activity").font(.title2.weight(.semibold))
                if currentError == nil && (currentPayload == nil || remainingMachines > 0) {
                    ProgressView().controlSize(.small)
                        .accessibilityLabel("Loading activity")
                        .accessibilityValue(loadingDetail)
                        .help(loadingDetail)
                }
                Spacer()
                Picker("Activity metric", selection: $store.homeActivityMetric) {
                    ForEach(HomeActivityMetric.allCases, id: \.self) { Text($0.title).tag($0) }
                }
                .labelsHidden()
                .frame(width: 150)
                Picker("Activity timeframe", selection: $store.filters.timeframe) {
                    ForEach(ConversationTimeframe.allCases, id: \.self) { Text($0.title).tag($0) }
                }
                .labelsHidden()
                .frame(width: 160)
            }
            VStack(alignment: .leading, spacing: 16) {
                if let payload = currentPayload {
                    activityChart(payload)
                } else if let error = currentError {
                    VStack(spacing: 10) {
                        Text("Activity unavailable").font(.headline)
                        Text(error).font(.callout).foregroundStyle(.secondary).multilineTextAlignment(.center)
                        Button("Try Again") { retry += 1 }
                    }
                    .frame(maxWidth: .infinity)
                    .frame(height: 200)
                } else {
                    Color.clear.frame(height: 200)
                }
                // Keep the footer's space while a new search is loading.
                HStack(alignment: .firstTextBaseline) {
                    Text(currentPayload.map { "\($0.total.formatted(.number.notation(.compactName))) \(metric.title.lowercased())" } ?? " ")
                        .font(.headline)
                    if remainingMachines == 0 && currentPayload?.partial == true {
                        Text("Partial results").foregroundStyle(.secondary)
                    }
                    Spacer()
                    Text("UTC").font(.caption).foregroundStyle(.secondary)
                }
                if let payload = currentPayload {
                    if metric == .tokens && !payload.tokenUsageEnabled {
                        Text("Token usage is disabled on the available machines.")
                            .font(.callout).foregroundStyle(.secondary)
                    }
                    if !failedMachines.isEmpty {
                        Text("Unavailable: \(failedMachines.keys.sorted().joined(separator: ", "))")
                            .font(.callout).foregroundStyle(.secondary)
                            .help(failedMachines.keys.sorted().compactMap { id in failedMachines[id].map { "\(id): \($0)" } }.joined(separator: "\n"))
                    }
                    if let warning = payload.warnings?.first {
                        Text(warning)
                            .font(.callout).foregroundStyle(.secondary)
                            .help((payload.warnings ?? []).joined(separator: "\n"))
                    } else if payload.partial && failedMachines.isEmpty && payload.tokenUsageEnabled {
                        Text("Some usage sources could not contribute activity.")
                            .font(.callout).foregroundStyle(.secondary)
                    }
                }
            }
            .padding(20)
            .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 14))
        }
        .task(id: requestID) { await load() }
    }

    @ViewBuilder
    private func activityChart(_ payload: HomeActivityPayload) -> some View {
        if payload.points.isEmpty {
            Text(metric == .tokens ? "No token activity in this timeframe" : "No conversations in this timeframe")
                .foregroundStyle(.secondary).frame(maxWidth: .infinity).frame(height: 200)
        } else {
            Chart(payload.points) { point in
                BarMark(x: .value("Date", point.date), y: .value(metric.title, point.value))
                    .foregroundStyle(by: .value("Provider", ConversationProvider(rawValue: point.source)?.title ?? point.source))
                    .accessibilityLabel("\(point.source), \(point.date)")
                    .accessibilityValue("\(point.value.formatted()) \(metric.title.lowercased())")
            }
            .chartXScale(domain: payload.bucketKeys)
            .chartXAxis {
                AxisMarks(values: axisKeys(payload.bucketKeys)) { value in
                    AxisValueLabel(anchor: value.index == 0 ? .topLeading : value.index == value.count - 1 ? .topTrailing : .top) {
                        if let key = value.as(String.self) {
                            Text(axisLabel(key)).fixedSize()
                        }
                    }
                }
            }
            .chartYAxis {
                AxisMarks(position: .leading) {
                    AxisGridLine()
                    AxisValueLabel(format: FloatingPointFormatStyle<Double>.number.notation(.compactName))
                }
            }
            .chartLegend(position: .bottom, alignment: .leading)
            .frame(height: 200)
        }
    }

    private func axisKeys(_ keys: [String]) -> [String] {
        guard keys.count > 5 else { return keys }
        return (0..<5).map { keys[$0 * (keys.count - 1) / 4] }
    }

    private func axisLabel(_ key: String) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US_POSIX")
        formatter.timeZone = TimeZone(secondsFromGMT: 0)
        formatter.dateFormat = key.contains("T") ? "yyyy-MM-dd'T'HH:mm'Z'" : "yyyy-MM-dd"
        guard let date = formatter.date(from: key) else { return key }
        formatter.locale = .current
        formatter.dateFormat = key.contains("T") ? "HH:mm" : "MMM d"
        return formatter.string(from: date)
    }

    private func load() async {
        let request = requestID
        let criteria = criteriaID
        if let cache = store.homeActivityCache, cache.isFresh(for: request) {
            failedMachines = cache.failures
            return
        }
        let generation = UUID()
        store.homeActivityGeneration = generation
        store.loadingHomeActivity = true
        defer { if store.homeActivityGeneration == generation { store.loadingHomeActivity = false } }
        // Refresh the same selection in place, retaining each peer until its replacement arrives.
        if store.homeActivityCache?.criteria != criteria { store.homeActivityCache = nil }
        error = nil
        errorRequest = nil
        failedMachines = [:]
        let machines = store.selectedMachineIDs
        remainingMachines = machines.count
        pendingMachines = Set(machines)
        scanProgress = [:]
        let selection = HomeActivitySelection(metric: metric.rawValue, timeframe: store.filters.timeframe,
            query: store.query, project: store.selectedProject, source: store.filters.provider.argument,
            origin: store.filters.origin, nowMS: UInt64(max(0, Date().timeIntervalSince1970 * 1000)))
        var batches = store.homeActivityCache?.machines ?? [:]
        do {
            try await Task.sleep(for: .milliseconds(180))
            try await store.client.activityBatches(selection, machines: machines, progress: { machine, update in
                guard store.homeActivityGeneration == generation, requestID == request, pendingMachines.contains(machine) else { return }
                scanProgress[machine] = update
            }) { batch in
                guard store.homeActivityGeneration == generation, requestID == request else { return }
                pendingMachines.remove(batch.machine)
                scanProgress.removeValue(forKey: batch.machine)
                remainingMachines -= 1
                if let result = batch.payload { batches[batch.machine] = result }
                if let failure = batch.error { failedMachines[batch.machine] = failure }
                if !batches.isEmpty {
                    store.homeActivityCache = HomeActivityCache(criteria: criteria, request: request,
                        payload: .merge(machines.compactMap { batches[$0] }, selection: selection, failed: !failedMachines.isEmpty),
                        machines: batches, failures: failedMachines,
                        complete: remainingMachines == 0, updatedAt: Date())
                } else if remainingMachines == 0 {
                    error = failedMachines.keys.sorted().compactMap { id in failedMachines[id].map { "\(id): \($0)" } }.joined(separator: "\n")
                    errorRequest = request
                }
            }
        } catch is CancellationError {} catch {
            guard !Task.isCancelled, store.homeActivityGeneration == generation, requestID == request else { return }
            self.error = error.localizedDescription
            errorRequest = request
        }
    }
}
