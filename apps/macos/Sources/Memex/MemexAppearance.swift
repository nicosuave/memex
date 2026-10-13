import AppKit
import SwiftUI

private struct MemexReduceMotionKey: EnvironmentKey {
    static let defaultValue = false
}

extension EnvironmentValues {
    /// App-owned override. System accessibility values are read-only and remain
    /// authoritative when either the app override or macOS setting is enabled.
    var memexReduceMotion: Bool {
        get { self[MemexReduceMotionKey.self] }
        set { self[MemexReduceMotionKey.self] = newValue }
    }
}

enum MemexAppearancePolicy {
    static func appearanceName(mode: AppPreferences.Appearance, systemDark: Bool,
                               systemContrast: Bool, increasedContrast: Bool) -> NSAppearance.Name? {
        let dark = mode == .dark || (mode == .system && systemDark)
        if systemContrast || increasedContrast {
            return dark ? .accessibilityHighContrastDarkAqua : .accessibilityHighContrastAqua
        }
        switch mode {
        case .system: return nil
        case .light: return .aqua
        case .dark: return .darkAqua
        }
    }
}

private struct MemexAppearanceModifier: ViewModifier {
    @Bindable var preferences: AppPreferences
    @Environment(\.accessibilityReduceMotion) private var systemReduceMotion
    @State private var systemContrast = NSWorkspace.shared.accessibilityDisplayShouldIncreaseContrast

    func body(content: Content) -> some View {
        content
            .preferredColorScheme(preferences.appearance == .system ? nil : preferences.appearance == .dark ? .dark : .light)
            .font(Font(preferences.bodyNSFont))
            .environment(\.memexReduceMotion, preferences.reduceMotion)
            .transaction {
                if systemReduceMotion || preferences.reduceMotion { $0.animation = nil; $0.disablesAnimations = true }
            }
            .background {
                NativeWindowAppearance(mode: preferences.appearance, increasedContrast: preferences.increasedContrast,
                                       systemContrast: systemContrast)
                    .frame(width: 0, height: 0).accessibilityHidden(true)
            }
            .onReceive(NSWorkspace.shared.notificationCenter.publisher(for: NSWorkspace.accessibilityDisplayOptionsDidChangeNotification)) { _ in
                systemContrast = NSWorkspace.shared.accessibilityDisplayShouldIncreaseContrast
            }
    }
}

/// Apply the native high-contrast appearance to both AppKit transcript surfaces
/// and SwiftUI controls. Reading NSWorkspace directly avoids feeding our own
/// appearance override back into the system-preference decision.
private struct NativeWindowAppearance: NSViewRepresentable {
    let mode: AppPreferences.Appearance
    let increasedContrast: Bool
    let systemContrast: Bool

    func makeCoordinator() -> Coordinator { Coordinator() }
    func makeNSView(context: Context) -> WindowProbe {
        let view = WindowProbe()
        let coordinator = context.coordinator
        coordinator.view = view
        view.didMove = { [weak coordinator] in coordinator?.apply() }
        // A high-contrast window has an explicit appearance, so it must still
        // follow system light/dark changes when the user chose System.
        coordinator.observation = NSApplication.shared.observe(\.effectiveAppearance, options: [.new]) { [weak coordinator] _, _ in
            Task { @MainActor in coordinator?.apply() }
        }
        return view
    }
    func updateNSView(_ view: WindowProbe, context: Context) {
        context.coordinator.mode = mode
        context.coordinator.increasedContrast = increasedContrast
        context.coordinator.systemContrast = systemContrast
        context.coordinator.apply()
    }

    @MainActor final class Coordinator {
        weak var view: WindowProbe?
        var observation: NSKeyValueObservation?
        var mode = AppPreferences.Appearance.system
        var increasedContrast = false
        var systemContrast = false
        func apply() {
            guard let window = view?.window else { return }
            let systemDark = NSApplication.shared.effectiveAppearance.bestMatch(from: [.aqua, .darkAqua]) == .darkAqua
            let name = MemexAppearancePolicy.appearanceName(mode: mode, systemDark: systemDark,
                systemContrast: systemContrast, increasedContrast: increasedContrast)
            guard window.appearance?.name != name else { return }
            window.appearance = name.flatMap(NSAppearance.init(named:))
        }
    }
    final class WindowProbe: NSView {
        var didMove: (() -> Void)?
        override func viewDidMoveToWindow() { super.viewDidMoveToWindow(); didMove?() }
    }
}

extension View {
    @MainActor func memexAppearance(preferences: AppPreferences = .shared) -> some View {
        modifier(MemexAppearanceModifier(preferences: preferences))
    }
}
