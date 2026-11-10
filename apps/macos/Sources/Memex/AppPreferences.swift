import AppKit
import Observation
import SwiftUI

/// Shared by Settings and the actual menu commands; no separate display-only keymap.
@MainActor @Observable final class AppPreferences {
    static let shared = AppPreferences(defaults: .standard)

    enum Command: String, CaseIterable, Identifiable, Codable {
        case newConversation, refresh, find, workspaceChanges, browser, terminal
        var id: Self { self }
        var title: String {
            switch self {
            case .newConversation: "New Conversation"
            case .refresh: "Refresh Conversations"
            case .find: "Find in Conversation"
            case .workspaceChanges: "Workspace Changes"
            case .browser: "Browser"
            case .terminal: "Toggle Terminal Drawer"
            }
        }
        var defaultBinding: Binding {
            switch self {
            case .newConversation: Binding(key: "n")
            case .refresh: Binding(key: "r")
            case .find: Binding(key: "f")
            case .workspaceChanges: Binding(key: "d", shift: true)
            case .browser: Binding(key: "b", shift: true)
            case .terminal: Binding(key: "j")
            }
        }
    }

    struct Binding: Codable, Equatable {
        var key: String
        var command = true
        var shift = false
        var option = false
        var control = false
        var modifiers: EventModifiers {
            var value: EventModifiers = []
            if command { value.insert(.command) }
            if shift { value.insert(.shift) }
            if option { value.insert(.option) }
            if control { value.insert(.control) }
            return value
        }
        var label: String {
            (control ? "⌃" : "") + (option ? "⌥" : "") + (shift ? "⇧" : "") + (command ? "⌘" : "") + key.uppercased()
        }
    }
    enum Appearance: String, CaseIterable { case system, light, dark }
    enum FontDesign: String, CaseIterable { case system, serif, rounded, monospaced }

    @ObservationIgnored private let defaults: UserDefaults
    private(set) var bindings: [String: Binding] = [:]
    private(set) var disabled: Set<String> = []
    var appearance: Appearance { didSet { defaults.set(appearance.rawValue, forKey: "appearance.mode") } }
    var interfaceSize: Double { didSet { defaults.set(interfaceSize, forKey: "appearance.interfaceSize") } }
    var codeSize: Double { didSet { defaults.set(codeSize, forKey: "appearance.codeSize") } }
    var fontDesign: FontDesign { didSet { defaults.set(fontDesign.rawValue, forKey: "appearance.fontDesign") } }
    var reduceMotion: Bool { didSet { defaults.set(reduceMotion, forKey: "appearance.reduceMotion") } }
    var increasedContrast: Bool { didSet { defaults.set(increasedContrast, forKey: "appearance.increasedContrast") } }

    init(defaults: UserDefaults) {
        self.defaults = defaults
        appearance = Appearance(rawValue: defaults.string(forKey: "appearance.mode") ?? "") ?? .system
        fontDesign = FontDesign(rawValue: defaults.string(forKey: "appearance.fontDesign") ?? "") ?? .system
        interfaceSize = Self.size(defaults.object(forKey: "appearance.interfaceSize") as? Double, fallback: 14)
        codeSize = Self.size(defaults.object(forKey: "appearance.codeSize") as? Double, fallback: 12)
        reduceMotion = defaults.bool(forKey: "appearance.reduceMotion")
        increasedContrast = defaults.bool(forKey: "appearance.increasedContrast")
        if let data = defaults.data(forKey: "keyboard.bindings"),
           let saved = try? JSONDecoder().decode(SavedBindings.self, from: data) {
            disabled = Set(saved.disabled.filter { Command(rawValue: $0) != nil })
            // Load the complete map before checking conflicts so valid swaps survive relaunch.
            bindings = saved.bindings.filter { Command(rawValue: $0.key) != nil }
            // Drop only invalid saved bindings, returning each to its default.
            // Repeat because a restored default can conflict with another binding.
            var dropped = true
            while dropped {
                dropped = false
                for command in Command.allCases where bindings[command.rawValue] != nil {
                    if let binding = binding(for: command), validationError(binding, for: command) != nil {
                        bindings.removeValue(forKey: command.rawValue)
                        dropped = true
                    }
                }
            }
        }
    }
    private static func size(_ value: Double?, fallback: Double) -> Double {
        guard let value, value.isFinite else { return fallback }
        return min(24, max(10, value))
    }
    func binding(for command: Command) -> Binding? {
        disabled.contains(command.rawValue) ? nil : bindings[command.rawValue] ?? command.defaultBinding
    }
    func shortcut(for command: Command) -> KeyboardShortcut? {
        guard let binding = binding(for: command), let key = binding.key.first else { return nil }
        return KeyboardShortcut(KeyEquivalent(key), modifiers: binding.modifiers)
    }
    func validationError(_ binding: Binding, for command: Command) -> String? {
        guard binding.key.count == 1, binding.key == binding.key.lowercased(),
              binding.key.unicodeScalars.allSatisfy({ CharacterSet.alphanumerics.contains($0) }) else {
            return "Choose a single letter or number."
        }
        guard binding.command || binding.control else { return "Include Command or Control so typing remains available." }
        // Arrow keys and other non-alphanumeric keys are rejected above, so
        // ⌘↑/⌘↓ text navigation can never be rebound to a command.
        if Self.reserved.contains(binding) { return "This shortcut is reserved by an editing, window or conversation command." }
        if let conflict = Command.allCases.first(where: { $0 != command && self.binding(for: $0) == binding }) {
            return "Already assigned to \(conflict.title). Remove or change that binding first."
        }
        return nil
    }
    /// Native editing, window and Find commands, plus fixed transcript and
    /// composer shortcuts, still own these keys.
    static let reserved: [Binding] = [
        Binding(key: "q"), Binding(key: "w"), Binding(key: "w", option: true), Binding(key: "h"),
        Binding(key: "h", option: true), Binding(key: "m"), Binding(key: "m", option: true),
        Binding(key: "f", control: true), Binding(key: "e"),
        Binding(key: "a"), Binding(key: "c"), Binding(key: "v"), Binding(key: "x"),
        Binding(key: "z"), Binding(key: "z", shift: true), Binding(key: "s"),
        Binding(key: "s", shift: true), Binding(key: "g"), Binding(key: "g", shift: true)]
    @discardableResult func setBinding(_ binding: Binding?, for command: Command) -> String? {
        if let binding, let error = validationError(binding, for: command) { return error }
        if let binding { bindings[command.rawValue] = binding; disabled.remove(command.rawValue) }
        else { bindings.removeValue(forKey: command.rawValue); disabled.insert(command.rawValue) }
        saveBindings()
        return nil
    }
    func resetShortcuts() { bindings = [:]; disabled = []; saveBindings() }
    private struct SavedBindings: Codable { var bindings: [String: Binding]; var disabled: Set<String> }
    private func saveBindings() {
        if let data = try? JSONEncoder().encode(SavedBindings(bindings: bindings, disabled: disabled)) {
            defaults.set(data, forKey: "keyboard.bindings")
        }
    }
    func resetAppearance() {
        appearance = .system; interfaceSize = 14; codeSize = 12; fontDesign = .system
        reduceMotion = false; increasedContrast = false
    }
    var bodyNSFont: NSFont {
        let base = NSFont.systemFont(ofSize: interfaceSize)
        let design: NSFontDescriptor.SystemDesign
        switch fontDesign { case .system: return base; case .serif: design = .serif; case .rounded: design = .rounded; case .monospaced: design = .monospaced }
        return base.fontDescriptor.withDesign(design).flatMap { NSFont(descriptor: $0, size: interfaceSize) } ?? base
    }
    var codeNSFont: NSFont { .monospacedSystemFont(ofSize: codeSize, weight: .regular) }
    /// Sidebar titles track the text size, one point below the reading size.
    var sidebarNSFont: NSFont {
        let body = bodyNSFont
        return NSFont(descriptor: body.fontDescriptor, size: max(10, interfaceSize - 1)) ?? body
    }
}
