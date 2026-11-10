import Foundation

/// Claude's default user config is $HOME/.claude.json, whereas an explicit
/// CLAUDE_CONFIG_DIR (even $HOME/.claude) moves it inside that directory.
/// A transcript home alone cannot distinguish those two native installations.
struct ClaudeNativeConfiguration: Equatable, Sendable {
    let providerHome: URL
    let userHome: URL
    let explicitConfigDirectory: String?

    var configurationURL: URL {
        if let explicitConfigDirectory {
            return URL(fileURLWithPath: explicitConfigDirectory).appendingPathComponent(".claude.json")
        }
        return userHome.appendingPathComponent(".claude.json")
    }

    /// Empty means explicitly use Claude's default. Process launchers remove
    /// this key *after* merging inherited environment, not merely omit it.
    var environment: [String: String] {
        ["HOME": userHome.path, "CLAUDE_CONFIG_DIR": explicitConfigDirectory ?? ""]
    }

    static func resolve(providerHome: URL, environment: [String: String] = ProcessInfo.processInfo.environment,
                        fallbackUserHome: URL = FileManager.default.homeDirectoryForCurrentUser) -> Self {
        let userHome = environment["HOME"].flatMap { value in
            value.hasPrefix("/") && !value.contains("\0") ? URL(fileURLWithPath: value) : nil
        } ?? fallbackUserHome
        let nativeHome = providerHome.resolvingSymlinksInPath()
        let configured = environment["CLAUDE_CONFIG_DIR"].flatMap { $0.isEmpty ? nil : $0 }
        let explicit: String?
        if let configured,
           URL(fileURLWithPath: configured).resolvingSymlinksInPath() == nativeHome {
            // An intentional override of ~/.claude must stay explicit.
            explicit = configured.hasPrefix("/") ? configured : nativeHome.path
        } else if nativeHome == userHome.appendingPathComponent(".claude").resolvingSymlinksInPath() {
            // Ignore an inherited override belonging to another installation.
            explicit = nil
        } else {
            explicit = nativeHome.path
        }
        return Self(providerHome: nativeHome, userHome: userHome, explicitConfigDirectory: explicit)
    }

    static func mergedEnvironment(_ overrides: [String: String], inherited: [String: String]) -> [String: String] {
        var merged = inherited.merging(overrides) { _, selected in selected }
        if merged["CLAUDE_CONFIG_DIR"] == "" { merged.removeValue(forKey: "CLAUDE_CONFIG_DIR") }
        return merged
    }
}
