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
    var title: String { self == .sessions ? "Sessions" : "Tokens" }
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
    static func skeleton(metric: HomeActivityMetric, timeframe: ConversationTimeframe, nowMS: UInt64) -> Self {
        let day: UInt64 = 86_400_000
        let selection = HomeActivitySelection(metric: metric.rawValue, timeframe: timeframe,
            query: nil, project: nil, source: nil, origin: .all, nowMS: nowMS)
        // An all-time chart has no known start until data arrives. Reserve its
        // maximum 60 weekly buckets; rolling ranges use the exact request grid.
        let span = (60 * 7 - 1) * day
        let end = nowMS / day * day
        let seed = RawHomeActivityPayload(tokenUsageEnabled: true, partial: false,
            points: timeframe == .all ? [RawHomeActivityPoint(
                timestampMS: end > span ? end - span : 0, source: "codex", value: 0)] : [])
        let keys = merge([seed], selection: selection, failed: false).bucketKeys
        return Self(metric: metric.rawValue, bucketKeys: keys, tokenUsageEnabled: true,
            partial: false, points: keys.map { HomeActivityPoint(date: $0, source: "codex", value: 100) })
    }

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
        store.cachedHomeActivity(for: criteriaID)?.payload
    }
    private var currentError: String? { errorRequest == requestID ? error : nil }
    private var pendingMachineLabels: [String] {
        pendingMachines.sorted().map { $0 == "local" ? "This Mac" : $0 }
    }
    private var skeletonPayload: HomeActivityPayload {
        .skeleton(metric: metric, timeframe: store.filters.timeframe,
            nowMS: UInt64(Date().timeIntervalSince1970 * 1000))
    }
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
                if let payload = currentPayload {
                    Text("\(payload.total.formatted(.number.notation(.compactName).precision(.fractionLength(0...1)))) \(metric.title.lowercased())")
                        .font(.callout).foregroundStyle(.secondary)
                        .help(remainingMachines > 0 ? loadingDetail : "Total activity in the selected timeframe")
                }
                Spacer()
                HStack(spacing: 8) {
                    ActivitySegments("Activity metric", selection: $store.homeActivityMetric,
                        options: HomeActivityMetric.allCases.map { ($0, $0.title) })
                    ActivitySegments("Activity timeframe", selection: $store.filters.timeframe,
                        options: [(.day, "24H"), (.week, "7D"), (.month, "30D"), (.all, "All")])
                }
            }
            VStack(alignment: .leading, spacing: 16) {
                if let payload = currentPayload {
                    if payload.points.isEmpty && !pendingMachines.isEmpty {
                        activityChart(skeletonPayload, isLoading: true)
                            .accessibilityElement(children: .ignore)
                            .accessibilityLabel("Loading activity")
                            .accessibilityValue(loadingDetail)
                            .help(loadingDetail)
                    } else {
                        activityChart(payload)
                    }
                } else if let error = currentError {
                    VStack(spacing: 10) {
                        Text("Activity unavailable").font(.headline)
                        Text(error).font(.callout).foregroundStyle(.secondary).multilineTextAlignment(.center)
                        Button("Try Again") { retry += 1 }
                    }
                    .frame(maxWidth: .infinity)
                    .frame(height: 200)
                } else {
                    activityChart(skeletonPayload, isLoading: true)
                        .accessibilityElement(children: .ignore)
                        .accessibilityLabel("Loading activity")
                        .accessibilityValue(loadingDetail)
                        .help(loadingDetail)
                }
                if let payload = currentPayload {
                    if let status = homeActivityStatus(payload: payload, remainingMachines: remainingMachines,
                        failedMachines: failedMachines, refreshError: currentError) {
                        Text(status).font(.callout).foregroundStyle(.secondary).lineLimit(1)
                    }
                }
            }
            .padding([.top, .horizontal], 20)
            .padding(.bottom, 10)
            .background(.quaternary.opacity(0.35), in: RoundedRectangle(cornerRadius: 14))
        }
        .task(id: requestID) { await load() }
    }

    @ViewBuilder
    private func activityChart(_ payload: HomeActivityPayload, isLoading: Bool = false) -> some View {
        if payload.points.isEmpty {
            Text(metric == .tokens ? "No token activity in this timeframe" : "No conversations in this timeframe")
                .foregroundStyle(.secondary).frame(maxWidth: .infinity).frame(height: 200)
        } else {
            Chart(payload.points) { point in
                BarMark(x: .value("Date", point.date), y: .value(metric.title, point.value), width: .ratio(0.8))
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
                AxisMarks(position: .leading) { value in
                    AxisGridLine()
                    AxisValueLabel {
                        if let amount = value.as(Double.self) {
                            Text(amount.formatted(.number.notation(.compactName).precision(.fractionLength(0...1))))
                                .fixedSize(horizontal: true, vertical: false)
                        }
                    }
                }
            }
            .chartLegend(position: .bottom, alignment: .leading)
            .frame(height: 200)
            .opacity(isLoading ? 0 : 1)
            .chartOverlay { proxy in
                GeometryReader { geometry in
                    if let plotFrame = proxy.plotFrame {
                        let totals = Dictionary(grouping: payload.points, by: \.date)
                            .mapValues { $0.reduce(0) { $0 + $1.value } }
                        let bars = payload.bucketKeys.compactMap { key -> ActivityPendingBar? in
                            guard let x = proxy.position(forX: key),
                                  let y = proxy.position(forY: totals[key, default: 0]) else { return nil }
                            return ActivityPendingBar(id: key, x: x, baseline: y)
                        }
                        // Match the mark's explicit ratio to the categorical band,
                        // which excludes the chart's outer padding and band spacing.
                        let band = payload.bucketKeys.first.flatMap { proxy.positionRange(forX: $0) }
                        let barWidth = band.map { ($0.upperBound - $0.lowerBound) * 0.8 } ?? 0
                        Color.clear.preference(key: ActivityPlotLayoutKey.self,
                            value: ActivityPlotLayout(frame: geometry[plotFrame], bars: bars, barWidth: barWidth))
                    }
                }
            }
            .overlayPreferenceValue(ActivityPlotLayoutKey.self) { layout in
                let frame = layout.frame
                if isLoading {
                    ZStack(alignment: .topLeading) {
                        HomeActivityLoadingBars(barCount: payload.bucketKeys.count, height: frame.height,
                            barWidth: layout.barWidth)
                            .frame(width: frame.width, height: frame.height)
                            .position(x: frame.midX, y: frame.midY)
                        ForEach(0..<5) { index in
                            RoundedRectangle(cornerRadius: 2).fill(.primary.opacity(0.08))
                                .frame(width: 28, height: 7)
                                .position(x: frame.minX + 14 + (frame.width - 28) * CGFloat(index) / 4,
                                    y: frame.maxY + 10)
                        }
                        HStack(spacing: 6) {
                            ForEach(0..<3) { _ in
                                Circle().frame(width: 6, height: 6)
                                RoundedRectangle(cornerRadius: 2).frame(width: 30, height: 7)
                            }
                        }
                        .foregroundStyle(.primary.opacity(0.08))
                        .frame(height: 12)
                        .offset(y: 188)
                    }
                    .transition(.identity)
                    .transaction { $0.animation = nil }
                } else if !pendingMachines.isEmpty {
                    HomeActivityPendingBars(bars: layout.bars,
                        barWidth: layout.barWidth)
                        .frame(width: frame.width, height: frame.height)
                        .clipped()
                        .position(x: frame.midX, y: frame.midY)
                        .accessibilityElement(children: .ignore)
                        .accessibilityLabel("Additional activity is loading")
                        .accessibilityValue("Waiting for \(pendingMachineLabels.joined(separator: ", "))")
                        .help("Waiting for \(pendingMachineLabels.joined(separator: ", "))\n\(loadingDetail)")
                        .transition(.identity)
                        .transaction { $0.animation = nil }
                }
            }
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
        remainingMachines = 0
        pendingMachines = []
        scanProgress = [:]
        error = nil
        errorRequest = nil
        store.homeActivityCache = store.cachedHomeActivity(for: criteria)
        if let cache = store.homeActivityCache, cache.isFresh(for: request) {
            failedMachines = cache.failures
            return
        }
        let generation = UUID()
        store.homeActivityGeneration = generation
        store.loadingHomeActivity = true
        defer {
            if store.homeActivityGeneration == generation {
                store.loadingHomeActivity = false
                remainingMachines = 0
                pendingMachines = []
                scanProgress = [:]
            }
        }
        // Retain this selection's previous results while refreshing each peer.
        failedMachines = [:]
        let machines = store.selectedMachineIDs
        remainingMachines = machines.count
        pendingMachines = Set(machines)
        scanProgress = [:]
        let selection = HomeActivitySelection(metric: metric.rawValue, timeframe: store.filters.timeframe,
            query: store.query, project: store.selectedProject, source: store.filters.provider.argument,
            origin: store.filters.origin, nowMS: UInt64(max(0, Date().timeIntervalSince1970 * 1000)))
        guard !machines.isEmpty else {
            store.homeActivityCache = HomeActivityCache(criteria: criteria, request: request,
                payload: .merge([], selection: selection, failed: false), machines: [:], failures: [:],
                complete: true, updatedAt: Date())
            return
        }
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

func homeActivityStatus(payload: HomeActivityPayload, remainingMachines: Int,
                        failedMachines: [String: String], refreshError: String?) -> String? {
    let failed = refreshError != nil || !failedMachines.isEmpty
    let incomplete = remainingMachines == 0 && (payload.partial || !(payload.warnings ?? []).isEmpty
        || (payload.metric == "tokens" && !payload.tokenUsageEnabled))
    return failed || incomplete ? "Some activity is unavailable." : nil
}

private struct ActivityPendingBar: Identifiable, Equatable {
    let id: String
    let x: CGFloat
    let baseline: CGFloat
}

private struct ActivityPlotLayout: Equatable {
    var frame = CGRect.zero
    var bars: [ActivityPendingBar] = []
    var barWidth: CGFloat = 0
}

private struct ActivityPlotLayoutKey: PreferenceKey {
    static let defaultValue = ActivityPlotLayout()
    static func reduce(value: inout ActivityPlotLayout, nextValue: () -> ActivityPlotLayout) { value = nextValue() }
}

private struct HomeActivityPendingBars: View {
    let bars: [ActivityPendingBar]
    let barWidth: CGFloat
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    var body: some View {
        TimelineView(.periodic(from: .now, by: 0.8)) { context in
            let time = reduceMotion ? 0 : context.date.timeIntervalSinceReferenceDate
            ZStack(alignment: .topLeading) {
                ForEach(Array(bars.enumerated()), id: \.element.id) { index, bar in
                    // Overlay pending activity in plot coordinates, so it never
                    // changes the numeric scale or the already-loaded marks.
                    let height = min(28, max(0, bar.baseline))
                    let fraction = 0.225 + 0.775 * (sin(time * 1.7 + Double(index) * 2.399) + 1) / 2
                    ActivityLoadingBar(fraction: fraction)
                        .fill(.primary.opacity(0.1))
                        .animation(reduceMotion ? nil : .easeInOut(duration: 0.8), value: time)
                        .frame(width: barWidth, height: height)
                        .position(x: bar.x, y: bar.baseline - height / 2)
                }
            }
        }
    }
}

private struct HomeActivityLoadingBars: View {
    let barCount: Int
    let height: CGFloat
    let barWidth: CGFloat
    @Environment(\.accessibilityReduceMotion) private var reduceMotion

    var body: some View {
        TimelineView(.periodic(from: .now, by: 0.8)) { context in
            let time = reduceMotion ? 0 : context.date.timeIntervalSinceReferenceDate
            HStack(alignment: .bottom, spacing: 0) {
                ForEach(0..<barCount, id: \.self) { index in
                    let phase = Double(index) * 2.399
                    let fraction = 0.225 + 0.775 * (sin(time * 1.7 + phase) + 1) / 2
                    ActivityLoadingBar(fraction: fraction)
                        .fill(.primary.opacity(0.08))
                        .animation(reduceMotion ? nil : .easeInOut(duration: 0.8), value: time)
                        .frame(width: barWidth, height: height)
                        .frame(maxWidth: .infinity)
                }
            }
            .frame(height: height, alignment: .bottom)
        }
        .accessibilityElement(children: .ignore)
    }
}

// Animate the fill inside a fixed bar, not the plot coordinates or layout.
private struct ActivityLoadingBar: Shape {
    var fraction: CGFloat
    var animatableData: CGFloat {
        get { fraction }
        set { fraction = newValue }
    }
    func path(in rect: CGRect) -> Path {
        let height = rect.height * fraction
        return RoundedRectangle(cornerRadius: 2).path(in: CGRect(
            x: rect.minX, y: rect.maxY - height, width: rect.width, height: height))
    }
}

private struct ActivitySegments<Value: Hashable>: View {
    let label: String
    @Binding var selection: Value
    let options: [(Value, String)]
    @Environment(\.colorScheme) private var colorScheme

    init(_ label: String, selection: Binding<Value>, options: [(Value, String)]) {
        self.label = label
        self._selection = selection
        self.options = options
    }

    var body: some View {
        HStack(spacing: 0) {
            ForEach(options, id: \.0) { value, title in
                Button { selection = value } label: {
                    Text(title)
                        .font(.system(size: 11, weight: selection == value ? .medium : .regular))
                        .foregroundStyle(.primary)
                        .frame(minWidth: 20)
                        .padding(.horizontal, 11)
                        .frame(height: 22)
                        .background(selection == value ? selectedColor : .clear, in: Capsule())
                        .contentShape(Capsule())
                }
                .buttonStyle(.plain)
                .accessibilityAddTraits(selection == value ? .isSelected : [])
            }
        }
        .padding(2)
        .background(trackColor, in: Capsule())
        .accessibilityElement(children: .contain)
        .accessibilityLabel(label)
        .fixedSize()
    }

    private var trackColor: Color {
        colorScheme == .dark ? Color(red: 53 / 255, green: 53 / 255, blue: 56 / 255)
            : Color(red: 237 / 255, green: 237 / 255, blue: 240 / 255)
    }
    private var selectedColor: Color {
        colorScheme == .dark ? Color(red: 66 / 255, green: 66 / 255, blue: 70 / 255)
            : Color(red: 225 / 255, green: 225 / 255, blue: 228 / 255)
    }
}
