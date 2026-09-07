import AppKit
import SwiftUI

// Launch adapters follow the applications' shipped scripting dictionaries:
// Ghostty/Ghostree: new window with a surface configuration; Terminal: do script.
enum ResumeDestination: String, CaseIterable, Sendable {
    case ghostree, ghostty, terminal

    var title: String {
        switch self { case .ghostree: "Ghostree"; case .ghostty: "Ghostty"; case .terminal: "Terminal" }
    }
    var bundleID: String {
        switch self {
        case .ghostree: "dev.sidequery.Ghostree"
        case .ghostty: "com.mitchellh.ghostty"
        case .terminal: "com.apple.Terminal"
        }
    }
    @MainActor var appURL: URL? { NSWorkspace.shared.urlForApplication(withBundleIdentifier: bundleID) }
    @MainActor var icon: NSImage? {
        guard let appURL else { return NSImage(systemSymbolName: "terminal", accessibilityDescription: title) }
        let icon = NSWorkspace.shared.icon(forFile: appURL.path)
        icon.size = NSSize(width: 16, height: 16)
        return icon
    }
    @MainActor static var installed: [Self] {
        allCases.filter { destination in
            guard let url = destination.appURL else { return false }
            if destination == .terminal { return true }
            // Older Ghostty versions do not expose the surface-configuration API.
            let dictionary = url.appendingPathComponent("Contents/Resources/Ghostty.sdef")
            guard let source = try? String(contentsOf: dictionary, encoding: .utf8) else { return false }
            return source.contains("name=\"new window\"") && source.contains("name=\"with configuration\"")
                && source.contains("name=\"command\"")
        }
    }
    static func preferred(in installed: [Self], saved: String?) -> Self? {
        if let saved, let destination = Self(rawValue: saved), installed.contains(destination) { return destination }
        return installed.first
    }
}

struct ResumeLaunchPlan: Equatable, Sendable {
    let destination: ResumeDestination
    let script: String

    static func unavailableReason(for session: Session) -> String? {
        if session.machineID != "local" { return "Resume is available on the session’s machine (\(session.machineID))." }
        guard let command = session.resumeCommand?.nilIfBlank else { return "No resume command is available for this conversation." }
        if command.contains("\0") || session.cwd?.contains("\0") == true { return "The resume command or working directory is invalid." }
        return nil
    }

    init(session: Session, destination: ResumeDestination) throws {
        if let reason = Self.unavailableReason(for: session) { throw ResumeError(message: reason) }
        // The CLI owns the configured shell command. Treat it as one shell argument;
        // never reconstruct a command from IDs, source paths, labels or snippets.
        let cwd = session.cwd?.nilIfBlank
        let payload = (cwd.map { "cd -- \(Self.shellQuote($0)) || exit\n" } ?? "") + session.resumeCommand!
        let launch = "/bin/zsh -lic " + Self.shellQuote(payload)
        self.destination = destination
        switch destination {
        case .terminal:
            script = """
            tell application id "\(destination.bundleID)"
                do script \(Self.appleScriptString(launch))
                activate
            end tell
            """
        case .ghostree, .ghostty:
            // Shell cd fails closed if the directory disappears, rather than resuming
            // in an unrelated default directory. A new surface never types into a busy shell.
            script = """
            tell application id "\(destination.bundleID)"
                new window with configuration {command:\(Self.appleScriptString(launch)), wait after command:true}
                activate
            end tell
            """
        }
    }

    static func shellQuote(_ value: String) -> String {
        "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'"
    }
    static func appleScriptString(_ value: String) -> String {
        "\"" + value.replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
            .replacingOccurrences(of: "\r", with: "\\r")
            .replacingOccurrences(of: "\n", with: "\\n") + "\""
    }

    func run() async throws {
        let source = script
        try await Task.detached {
            let process = Process()
            process.executableURL = URL(fileURLWithPath: "/usr/bin/osascript")
            process.arguments = ["-"]
            let input = Pipe(), errors = Pipe()
            process.standardInput = input
            process.standardOutput = FileHandle.nullDevice
            process.standardError = errors
            try process.run()
            try input.fileHandleForWriting.write(contentsOf: Data(source.utf8))
            try input.fileHandleForWriting.close()
            let data = errors.fileHandleForReading.readDataToEndOfFile()
            process.waitUntilExit()
            guard process.terminationStatus == 0 else {
                throw ResumeError(message: String(decoding: data, as: UTF8.self).nilIfBlank ?? "The terminal could not open this conversation.")
            }
        }.value
    }
}

struct ResumeError: LocalizedError {
    let message: String
    var errorDescription: String? { message }
}

struct ResumeToolbarButton: View {
    @Bindable var store: Store

    var body: some View {
        if let session = store.selected {
            ResumeButton(session: session, metadataHelp: store.loadingSessionMetadata
                         ? "Loading resume details…" : store.sessionMetadataError)
        }
    }
}

struct ResumeButton: View {
    let session: Session
    var metadataHelp: String? = nil
    @AppStorage("resume-terminal") private var savedDestination = ""
    @State private var isLaunching = false
    @State private var error: String?

    var body: some View {
        let destinations = ResumeDestination.installed
        let preferred = ResumeDestination.preferred(in: destinations, saved: savedDestination)
        let reason = metadataHelp ?? ResumeLaunchPlan.unavailableReason(for: session)
            ?? (preferred == nil ? "Install a supported terminal to resume this conversation." : nil)
        ResumeSplitControl(destinations: destinations, preferred: preferred,
                           enabled: reason == nil && !isLaunching,
                           help: reason ?? "Resume in \(preferred?.title ?? "terminal")") { destination in
            savedDestination = destination.rawValue
            isLaunching = true
            Task {
                defer { isLaunching = false }
                do { try await ResumeLaunchPlan(session: session, destination: destination).run() }
                catch { self.error = error.localizedDescription }
            }
        }
        .fixedSize()
        .alert("Could not resume conversation", isPresented: Binding(get: { error != nil }, set: { if !$0 { error = nil } })) {
            Button("OK") { error = nil }
        } message: { Text(error ?? "") }
    }
}

struct ResumeSplitControl: NSViewRepresentable {
    let destinations: [ResumeDestination]
    let preferred: ResumeDestination?
    let enabled: Bool
    let help: String
    let onOpen: (ResumeDestination) -> Void

    func makeCoordinator() -> Coordinator { Coordinator() }
    func makeNSView(context: Context) -> NSSegmentedControl {
        let control = NSSegmentedControl()
        control.segmentCount = 2
        control.trackingMode = .momentary
        control.segmentStyle = .separated
        control.setLabel("Resume", forSegment: 0)
        control.setWidth(0, forSegment: 0)
        control.setWidth(22, forSegment: 1)
        control.setShowsMenuIndicator(true, forSegment: 1)
        control.setImageScaling(.scaleProportionallyDown, forSegment: 0)
        control.target = context.coordinator
        control.action = #selector(Coordinator.openPreferred(_:))
        control.setAccessibilityLabel("Resume conversation")
        return control
    }
    func updateNSView(_ control: NSSegmentedControl, context: Context) {
        context.coordinator.preferred = preferred
        context.coordinator.onOpen = onOpen
        context.coordinator.enabled = enabled
        control.isEnabled = enabled
        control.setImage(preferred?.icon ?? NSImage(systemSymbolName: "terminal", accessibilityDescription: nil), forSegment: 0)
        control.setToolTip(help, forSegment: 0)
        control.setToolTip(enabled ? "Choose terminal" : help, forSegment: 1)
        control.toolTip = help
        let menu = NSMenu()
        for destination in destinations {
            let item = NSMenuItem(title: destination.title, action: #selector(Coordinator.openSpecific(_:)), keyEquivalent: "")
            item.target = context.coordinator
            item.representedObject = destination.rawValue
            item.image = destination.icon
            item.state = destination == preferred ? .on : .off
            menu.addItem(item)
        }
        control.setMenu(menu, forSegment: 1)
    }

    @MainActor final class Coordinator: NSObject {
        var preferred: ResumeDestination?
        var enabled = false
        var onOpen: ((ResumeDestination) -> Void)?
        @objc func openPreferred(_ sender: NSSegmentedControl) {
            guard enabled else { return }
            if sender.selectedSegment == 1 {
                sender.menu(forSegment: 1)?.popUp(positioning: nil, at: NSPoint(x: 0, y: sender.bounds.minY), in: sender)
            } else if let preferred { onOpen?(preferred) }
        }
        @objc func openSpecific(_ sender: NSMenuItem) {
            guard enabled, let raw = sender.representedObject as? String,
                  let destination = ResumeDestination(rawValue: raw) else { return }
            onOpen?(destination)
        }
    }
}
