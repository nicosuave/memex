import Foundation

// These interfaces are supplied by the installed applications: Alacritty/WezTerm
// CLI help, kitty's bundled manual, and cmux's CLI help and scripting dictionary.
enum TerminalLaunchAdapter: String, CaseIterable, Sendable {
    case alacritty, kitty, wezterm, cmux

    var title: String {
        switch self {
        case .alacritty: "Alacritty"
        case .kitty: "kitty"
        case .wezterm: "WezTerm"
        case .cmux: "cmux"
        }
    }

    var bundleID: String {
        switch self {
        case .alacritty: "org.alacritty"
        case .kitty: "net.kovidgoyal.kitty"
        case .wezterm: "com.github.wez.wezterm"
        case .cmux: "com.cmuxterm.app"
        }
    }

    private var executablePath: String {
        switch self {
        case .alacritty: "Contents/MacOS/alacritty"
        case .kitty: "Contents/MacOS/kitty"
        case .wezterm: "Contents/MacOS/wezterm-gui"
        case .cmux: "Contents/Resources/bin/cmux"
        }
    }

    func isAvailable(at appURL: URL) -> Bool {
        guard FileManager.default.isExecutableFile(atPath: appURL.appendingPathComponent(executablePath).path) else { return false }
        if self == .cmux {
            let dictionary = appURL.appendingPathComponent("Contents/Resources/cmux.sdef")
            guard let source = try? String(contentsOf: dictionary, encoding: .utf8) else { return false }
            return source.contains("name=\"new window\"") && source.contains("name=\"id\"")
        }
        return true
    }

    // The caller validates metadata and supplies the fail-closed cwd preamble plus
    // canonical command. CLI argv preserves the entire payload as one argument.
    func launch(payload: String, appURL: URL) -> TerminalLaunch {
        let shell = ["/bin/zsh", "-lic", payload]
        let arguments: [String]
        switch self {
        case .alacritty:
            arguments = ["--hold", "--command"] + shell
        case .kitty:
            // Explicit program arguments override kitty's startup_session setting.
            arguments = ["--hold"] + shell
        case .wezterm:
            // The default start operation creates a new window, unless --new-tab
            // is explicitly supplied. Keep local sessions out of remote domains.
            arguments = ["start", "--no-auto-connect", "--always-new-process", "--domain", "local", "--"] + shell
        case .cmux:
            let cli = appURL.appendingPathComponent(executablePath).path
            let launch = "/bin/zsh -lic " + ResumeLaunchPlan.shellQuote(payload)
            let prefix = ResumeLaunchPlan.shellQuote(cli) + " new-workspace --window "
            let suffix = " --command " + ResumeLaunchPlan.shellQuote(launch) + " --focus true"
            return .appleScript("""
            tell application id "\(bundleID)"
                set resumeWindow to new window
                set resumeWindowID to id of resumeWindow
                activate
            end tell
            try
                do shell script \(ResumeLaunchPlan.appleScriptString(prefix)) & quoted form of resumeWindowID & \(ResumeLaunchPlan.appleScriptString(suffix))
            on error errorMessage
                error "cmux could not create the resume workspace. Check that cmux socket access is enabled. " & errorMessage
            end try
            """)
        }
        // Launch Services owns the GUI process, so completing Resume does not
        // wait for the new terminal window to close. -n forwards arguments even
        // when an instance of the terminal is already running.
        return .process(executable: URL(fileURLWithPath: "/usr/bin/open"),
                        arguments: ["-n", "-a", appURL.path, "--args"] + arguments)
    }
}

enum TerminalLaunch: Equatable, Sendable {
    case process(executable: URL, arguments: [String])
    case appleScript(String)

    func run() async throws {
        let launch = self
        try await Task.detached {
            let process = Process()
            let input: Pipe?
            switch launch {
            case let .process(executable, arguments):
                process.executableURL = executable
                process.arguments = arguments
                input = nil
                process.standardInput = FileHandle.nullDevice
            case .appleScript:
                process.executableURL = URL(fileURLWithPath: "/usr/bin/osascript")
                process.arguments = ["-"]
                input = Pipe()
                process.standardInput = input
            }
            let errors = Pipe()
            process.standardOutput = FileHandle.nullDevice
            process.standardError = errors
            try process.run()
            if case let .appleScript(source) = launch, let input {
                try input.fileHandleForWriting.write(contentsOf: Data(source.utf8))
                try input.fileHandleForWriting.close()
            }
            let data = errors.fileHandleForReading.readDataToEndOfFile()
            process.waitUntilExit()
            guard process.terminationStatus == 0 else {
                throw ResumeError(message: String(decoding: data, as: UTF8.self).nilIfBlank
                                  ?? "The terminal could not open this conversation.")
            }
        }.value
    }
}
