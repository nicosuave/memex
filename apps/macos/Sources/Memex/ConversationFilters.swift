import Foundation

struct ConversationFilters: Codable, Equatable, Sendable {
    var timeframe = ConversationTimeframe.all
    var provider = ConversationProvider.all
    var origin = ConversationOrigin.all

    static let defaults = ConversationFilters()
    var conversationType: ConversationOrigin {
        get { origin == .includingReviews ? .all : origin }
        set { origin = newValue }
    }
    var showsPermissionReviews: Bool {
        get { origin == .includingReviews }
        set {
            guard conversationType == .all else { return }
            origin = newValue ? .includingReviews : .all
        }
    }
    var isActive: Bool { self != .defaults }
    var summary: String {
        [timeframe == .all ? nil : timeframe.title,
         provider == .all ? nil : provider.title,
         origin == .all ? nil : origin.title].compactMap { $0 }.joined(separator: " · ")
    }
}

enum ConversationTimeframe: String, CaseIterable, Codable, Sendable {
    case all, day, week, month
    var title: String {
        switch self {
        case .all: "All time"
        case .day: "Last 24 hours"
        case .week: "Last 7 days"
        case .month: "Last 30 days"
        }
    }
    func since(relativeTo date: Date) -> String? {
        let days: Double
        switch self {
        case .all: return nil
        case .day: days = 1
        case .week: days = 7
        case .month: days = 30
        }
        return date.addingTimeInterval(-days * 86_400).formatted(.iso8601)
    }
}

enum ConversationOrigin: String, CaseIterable, Codable, Sendable {
    case all, interactive, subagent, includingReviews
    static let conversationTypes: [Self] = [.all, .interactive, .subagent]
    var argument: String {
        switch self {
        case .all: "regular"
        case .includingReviews: "all"
        case .interactive, .subagent: rawValue
        }
    }
    var title: String {
        switch self {
        case .all: "Chats and subagents"
        case .interactive: "Chats only"
        case .subagent: "Subagents only"
        case .includingReviews: "Permission reviews shown"
        }
    }
}

/// The provider filters supported by the bundled CLI, independent of which
/// providers happen to appear in the first loaded page of conversations.
enum ConversationProvider: String, CaseIterable, Codable, Sendable {
    case all, claude, codex, cursor, opencode, pi, omp, openclaw, copilot, grok, hermes, jcode, muse
    var argument: String? { self == .all ? nil : rawValue }
    var title: String {
        switch self {
        case .all: "All providers"
        case .claude: "Claude"
        case .codex: "Codex"
        case .cursor: "Cursor"
        case .opencode: "OpenCode"
        case .pi: "Pi"
        case .omp: "Oh My Pi"
        case .openclaw: "OpenClaw"
        case .copilot: "Copilot"
        case .grok: "Grok"
        case .hermes: "Hermes"
        case .jcode: "Jcode"
        case .muse: "Muse"
        }
    }
}
