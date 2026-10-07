import AppKit
import SwiftUI

struct MemexSettingsView<ProviderContent: View>: View {
    @Bindable var preferences: AppPreferences
    @ViewBuilder var providerContent: () -> ProviderContent

    var body: some View {
        TabView {
            AppearanceSettingsView(preferences: preferences)
                .tabItem { Label("Appearance", systemImage: "paintbrush") }
            KeyboardSettingsView(preferences: preferences)
                .tabItem { Label("Keyboard", systemImage: "keyboard") }
            providerContent()
                .tabItem { Label("Providers & Tools", systemImage: "wrench.and.screwdriver") }
        }
        .frame(minWidth: 620, idealWidth: 720, minHeight: 460, idealHeight: 560)
        .memexAppearance(preferences: preferences)
    }
}

private struct AppearanceSettingsView: View {
    @Bindable var preferences: AppPreferences
    var body: some View {
        Form {
            Picker("Appearance", selection: $preferences.appearance) {
                ForEach(AppPreferences.Appearance.allCases, id: \.self) { Text($0.rawValue.capitalized).tag($0) }
            }
            Picker("Text font", selection: $preferences.fontDesign) {
                ForEach(AppPreferences.FontDesign.allCases, id: \.self) { Text($0.rawValue.capitalized).tag($0) }
            }
            Stepper("Conversation text: \(Int(preferences.interfaceSize)) pt", value: $preferences.interfaceSize, in: 10...24)
            Stepper("Code and tool output: \(Int(preferences.codeSize)) pt", value: $preferences.codeSize, in: 10...24)
            Toggle("Increase contrast", isOn: $preferences.increasedContrast)
            Toggle("Reduce motion", isOn: $preferences.reduceMotion)
            Text("System accessibility preferences remain enabled when these overrides are off.")
                .font(.caption).foregroundStyle(.secondary)
            Button("Restore appearance defaults") { preferences.resetAppearance() }
        }.formStyle(.grouped)
    }
}

private struct KeyboardSettingsView: View {
    @Bindable var preferences: AppPreferences
    @State private var editing: AppPreferences.Command?
    @State private var draft = AppPreferences.Binding(key: "")
    @State private var recording = false
    @State private var error: String?

    var body: some View {
        Form {
            ForEach(AppPreferences.Command.allCases) { command in
                HStack {
                    Text(command.title)
                    Spacer()
                    Button(preferences.binding(for: command)?.label ?? "Unassigned") {
                        editing = command
                        draft = preferences.binding(for: command) ?? .init(key: "")
                        error = nil
                    }.frame(minWidth: 80)
                    Button("Remove") { _ = preferences.setBinding(nil, for: command) }
                        .disabled(preferences.binding(for: command) == nil)
                        .accessibilityLabel("Remove shortcut for \(command.title)")
                }
            }
            Button("Restore all default shortcuts") { preferences.resetShortcuts() }
            Text("Editing, window, find-next and composer shortcuts remain reserved. Choose a letter or number with Command or Control.")
                .font(.caption).foregroundStyle(.secondary)
        }
        .formStyle(.grouped)
        .sheet(item: $editing) { command in
            VStack(alignment: .leading, spacing: 16) {
                Text(command.title).font(.headline)
                HStack {
                    TextField("Key", text: $draft.key).frame(width: 80)
                        .onChange(of: draft.key) { _, value in draft.key = value.lowercased(); error = nil }
                    Toggle("⌘", isOn: $draft.command).accessibilityLabel("Command")
                    Toggle("⇧", isOn: $draft.shift).accessibilityLabel("Shift")
                    Toggle("⌥", isOn: $draft.option).accessibilityLabel("Option")
                    Toggle("⌃", isOn: $draft.control).accessibilityLabel("Control")
                }
                Button(recording ? "Press shortcut… (Escape cancels)" : "Record shortcut") { recording.toggle() }
                ShortcutCapture(recording: $recording) { binding in draft = binding; error = nil }
                    .frame(width: 1, height: 1).accessibilityHidden(true)
                if let message = error ?? preferences.validationError(draft, for: command) {
                    Text(message).font(.callout).foregroundStyle(.red)
                }
                HStack {
                    Spacer()
                    Button("Cancel") { recording = false; editing = nil }
                    Button("Save") {
                        error = preferences.setBinding(draft, for: command)
                        if error == nil { recording = false; editing = nil }
                    }.disabled(recording || preferences.validationError(draft, for: command) != nil)
                }
            }.padding(24).frame(width: 500)
        }
    }
}

private struct ShortcutCapture: NSViewRepresentable {
    @Binding var recording: Bool
    let captured: (AppPreferences.Binding) -> Void
    func makeNSView(context: Context) -> CaptureView { CaptureView() }
    func updateNSView(_ view: CaptureView, context: Context) {
        view.recording = recording
        view.receive = { event in
            recording = false
            guard event.keyCode != 53 else { return }
            let flags = event.modifierFlags
            captured(.init(key: (event.charactersIgnoringModifiers ?? "").lowercased(),
                           command: flags.contains(.command), shift: flags.contains(.shift),
                           option: flags.contains(.option), control: flags.contains(.control)))
        }
        if recording {
            DispatchQueue.main.async { if view.recording { view.window?.makeFirstResponder(view) } }
        } else if view.window?.firstResponder === view { view.window?.makeFirstResponder(nil) }
    }
    final class CaptureView: NSView {
        var recording = false
        var receive: ((NSEvent) -> Void)?
        override var acceptsFirstResponder: Bool { true }
        override func keyDown(with event: NSEvent) {
            if recording { receive?(event) } else { super.keyDown(with: event) }
        }
        override func performKeyEquivalent(with event: NSEvent) -> Bool {
            guard recording else { return super.performKeyEquivalent(with: event) }
            receive?(event)
            return true
        }
    }
}
