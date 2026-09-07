import AppKit
import Foundation
import Testing
@testable import Memex

private func resumableSession(machine: String = "local", command: String? = "codex resume abc", cwd: String? = "/tmp/project") -> Session {
    Session(source: "codex", sessionID: "abc", sourcePath: "/tmp/transcript", project: "project",
            resumeCommand: command, cwd: cwd, machine: machine)
}

@Test func resumeAdaptersUseFreshWindowsAndPreserveCanonicalCommands() throws {
    let command = "CUSTOM=value codex resume 'a b' --flag; printf '%s' \"$CUSTOM\""
    let cwd = "/tmp/a'b $(touch nope)\nline"
    let session = resumableSession(command: command, cwd: cwd)
    let launch = "/bin/zsh -lic " + ResumeLaunchPlan.shellQuote("cd -- " + ResumeLaunchPlan.shellQuote(cwd) + " || exit\n" + command)
    for destination in ResumeDestination.allCases {
        let plan = try ResumeLaunchPlan(session: session, destination: destination)
        #expect(plan.destination == destination)
        #expect(plan.script.contains(ResumeLaunchPlan.appleScriptString(launch)))
        #expect(plan.script.contains("tell application id \"\(destination.bundleID)\""))
        if destination == .terminal {
            #expect(plan.script.contains("do script "))
            #expect(!plan.script.contains("in selected tab"))
        } else {
            #expect(plan.script.contains("new window with configuration {command:"))
            #expect(plan.script.contains("wait after command:true"))
        }
    }
}

@Test func resumeRejectsRemoteMissingAndInvalidCommands() {
    for session in [resumableSession(machine: "nicbook-atm"), resumableSession(command: nil),
                    resumableSession(command: " \n"), resumableSession(command: "abc\0def"),
                    resumableSession(cwd: "/tmp/\0")] {
        #expect(ResumeLaunchPlan.unavailableReason(for: session) != nil)
        #expect(throws: ResumeError.self) { try ResumeLaunchPlan(session: session, destination: .terminal) }
    }
    #expect(ResumeLaunchPlan.unavailableReason(for: resumableSession(cwd: nil)) == nil)
}

@Test func resumeQuotingKeepsShellAndAppleScriptDataLiteral() {
    #expect(ResumeLaunchPlan.shellQuote("") == "''")
    #expect(ResumeLaunchPlan.shellQuote("a'b $c `d`") == "'a'\\''b $c `d`'")
    #expect(ResumeLaunchPlan.appleScriptString("a\"b\\c\n\r") == "\"a\\\"b\\\\c\\n\\r\"")
}

@Test func resumePreferenceUsesOnlyInstalledDestinations() {
    #expect(ResumeDestination.preferred(in: [.ghostree, .terminal], saved: "terminal") == .terminal)
    #expect(ResumeDestination.preferred(in: [.ghostree, .terminal], saved: "ghostty") == .ghostree)
    #expect(ResumeDestination.preferred(in: [.terminal], saved: "unknown") == .terminal)
    #expect(ResumeDestination.preferred(in: [], saved: "terminal") == nil)
}

@MainActor @Test func resumeSplitButtonDispatchesPrimaryAndMenuChoicesOnlyWhenEnabled() {
    let coordinator = ResumeSplitControl.Coordinator()
    var opened: [ResumeDestination] = []
    coordinator.onOpen = { opened.append($0) }
    coordinator.preferred = .ghostree
    coordinator.enabled = true
    let control = NSSegmentedControl()
    control.segmentCount = 2
    control.selectedSegment = 0
    coordinator.openPreferred(control)
    let menu = NSMenuItem()
    menu.representedObject = ResumeDestination.terminal.rawValue
    coordinator.openSpecific(menu)
    #expect(opened == [.ghostree, .terminal])
    coordinator.enabled = false
    coordinator.openPreferred(control)
    coordinator.openSpecific(menu)
    #expect(opened == [.ghostree, .terminal])
}

@Test func resumeShellArgumentRoundTripsWithoutExpansion() throws {
    let value = "a'b $(printf expanded) `printf expanded` $HOME\nsecond line"
    let process = Process()
    process.executableURL = URL(fileURLWithPath: "/bin/zsh")
    process.arguments = ["-fc", "printf '%s' " + ResumeLaunchPlan.shellQuote(value)]
    let output = Pipe()
    process.standardOutput = output
    try process.run()
    let data = output.fileHandleForReading.readDataToEndOfFile()
    process.waitUntilExit()
    #expect(process.terminationStatus == 0)
    #expect(String(decoding: data, as: UTF8.self) == value)
}
