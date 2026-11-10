import SwiftUI

/// Indeterminate progress for a wait that usually ends within a second. It draws nothing for the
/// first 200 ms, so quick work never flashes a spinner, yet takes its final size from the start, so
/// the spinner fading in does not move neighbouring content.
struct DelayedProgress: View {
    private static let showDelay: Duration = .milliseconds(200)
    private let label: String?
    private let controlSize: ControlSize
    @State private var visible = false

    init(_ label: String? = nil, controlSize: ControlSize = .small) {
        self.label = label
        self.controlSize = controlSize
    }

    var body: some View {
        Group {
            if let label { ProgressView(label) } else { ProgressView() }
        }
        .controlSize(controlSize)
        .opacity(visible ? 1 : 0)
        .accessibilityHidden(!visible)
        .task {
            // A cancelled sleep means the wait ended before the delay; stay hidden.
            guard (try? await Task.sleep(for: Self.showDelay)) != nil else { return }
            visible = true
        }
    }
}
