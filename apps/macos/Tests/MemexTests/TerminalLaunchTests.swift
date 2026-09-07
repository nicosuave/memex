import Foundation
import Testing
@testable import Memex

@Test func terminalCLIAdaptersPreserveProgramArgumentsAndCreateWindows() {
    let payload = "cd -- '/tmp/a b' || exit\nCUSTOM=value codex resume 'a b'; printf '%s' \"$CUSTOM\""
    let app = URL(fileURLWithPath: "/Applications/Terminal With Spaces.app")
    let shell = ["/bin/zsh", "-lic", payload]
    let expected: [(TerminalLaunchAdapter, [String])] = [
        (.alacritty, ["--hold", "--command"] + shell),
        (.kitty, ["--hold"] + shell),
        (.wezterm, ["start", "--no-auto-connect", "--always-new-process", "--domain", "local", "--"] + shell),
    ]
    for (adapter, arguments) in expected {
        #expect(adapter.launch(payload: payload, appURL: app) == .process(
            executable: URL(fileURLWithPath: "/usr/bin/open"),
            arguments: ["-n", "-a", app.path, "--args"] + arguments))
    }
}

@Test func cmuxLaunchTargetsTheNewWindowAndQuotesItsCommand() throws {
    let payload = "cd -- '/tmp/a b' || exit\nprintf '%s' \"$HOME\"; codex resume 'a b'"
    let app = URL(fileURLWithPath: "/Applications/cmux 'test'.app")
    let plan = TerminalLaunchAdapter.cmux.launch(payload: payload, appURL: app)
    guard case let .appleScript(script) = plan else {
        Issue.record("cmux should use its new-window scripting command")
        return
    }
    #expect(script.contains("set resumeWindow to new window"))
    #expect(script.contains("set resumeWindowID to id of resumeWindow"))
    #expect(script.contains("quoted form of resumeWindowID"))
    #expect(!script.contains("input text"))
    let prefix = ResumeLaunchPlan.shellQuote(app.appendingPathComponent("Contents/Resources/bin/cmux").path)
        + " new-workspace --window "
    let suffix = " --command " + ResumeLaunchPlan.shellQuote("/bin/zsh -lic " + ResumeLaunchPlan.shellQuote(payload))
        + " --focus true"
    #expect(script.contains(ResumeLaunchPlan.appleScriptString(prefix)))
    #expect(script.contains(ResumeLaunchPlan.appleScriptString(suffix)))
    #expect(script.contains("cmux socket access is enabled"))
}

@Test func terminalAdapterAvailabilityRequiresItsExecutable() throws {
    let app = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: app, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: app) }
    for adapter in TerminalLaunchAdapter.allCases {
        #expect(!adapter.isAvailable(at: app))
    }
    let executable = app.appendingPathComponent("Contents/MacOS/alacritty")
    try FileManager.default.createDirectory(at: executable.deletingLastPathComponent(), withIntermediateDirectories: true)
    try Data().write(to: executable)
    try FileManager.default.setAttributes([.posixPermissions: 0o755], ofItemAtPath: executable.path)
    #expect(TerminalLaunchAdapter.alacritty.isAvailable(at: app))
    #expect(!TerminalLaunchAdapter.kitty.isAvailable(at: app))
}
