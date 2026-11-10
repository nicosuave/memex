import SwiftUI

enum ConversationActivity: String, Equatable, Sendable {
    case starting, working, stopping, approval, question, failed, completed, stopped, openElsewhere

    var label: String {
        switch self {
        case .starting: "Starting"
        case .working: "Working"
        case .stopping: "Stopping"
        case .approval: "Approval needed"
        case .question: "Answer needed"
        case .failed: "Needs attention"
        case .completed: "Completed"
        case .stopped: "Stopped"
        case .openElsewhere: "Open elsewhere"
        }
    }

    var symbol: String {
        switch self {
        case .starting, .working: "circle.dotted"
        case .stopping, .stopped: "stop.circle"
        case .approval: "hand.raised"
        case .question: "questionmark.bubble"
        case .failed: "exclamationmark.circle"
        case .completed: "checkmark.circle"
        case .openElsewhere: "lock"
        }
    }

    var color: Color {
        switch self {
        case .approval, .question: .orange
        case .failed: .red
        case .starting, .working, .stopping: .accentColor
        case .completed, .stopped, .openElsewhere: .secondary
        }
    }

    /// Only an agent that is busy moves. States waiting on the user stay still so they read as
    /// badges, not activity.
    func pulsesSymbol(reduceMotion: Bool) -> Bool {
        switch self {
        case .starting, .working, .stopping: !reduceMotion
        case .approval, .question, .failed, .completed, .stopped, .openElsewhere: false
        }
    }
}

struct ConversationListState: Equatable, Sendable {
    var activity: ConversationActivity?
    var hasDraft = false

    var label: String? { activity?.label }
}

struct ConversationStateLabel: View {
    let state: ConversationListState
    @Environment(\.accessibilityReduceMotion) private var systemReduceMotion
    @Environment(\.memexReduceMotion) private var appReduceMotion

    var body: some View {
        if let activity = state.activity {
            // The pulse is a system symbol effect: it changes opacity only, so the row keeps its
            // layout, and this view's body is not re-evaluated per frame.
            Image(systemName: activity.symbol)
                .font(.caption).foregroundStyle(activity.color)
                .symbolEffect(.pulse, isActive: activity.pulsesSymbol(reduceMotion: systemReduceMotion || appReduceMotion))
                .help(activity.label).accessibilityLabel(activity.label)
        }
    }
}
