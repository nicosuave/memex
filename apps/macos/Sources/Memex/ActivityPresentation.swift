import CoreFoundation
import Foundation

/// Display-only metadata. The original records remain the source for details and Find.
struct ActivityPresentation: Equatable, Sendable {
    let title: String
    let symbolName: String
    let hasFailure: Bool
    let category: String?
    var isInterrupted = false
    var needsAttention: Bool { hasFailure || isInterrupted }
}

extension TranscriptActivity {
    var presentation: ActivityPresentation {
        let message = records.first(where: { $0.record.role == "tool_use" })?.record ?? records[0].record
        let interruption = records.lazy.compactMap { entry -> String? in
            let message = entry.record
            let output = message.toolOutput ?? (["tool_result", "tool"].contains(message.role) ? message.text : "")
            guard let status = Self.object(output)?["status"] as? String else { return nil }
            switch status.lowercased() {
            case "cancelled", "canceled": return "Cancelled"
            case "interrupted": return "Interrupted"
            case "incomplete": return "Incomplete"
            default: return nil
            }
        }.first
        let failed = records.contains { entry in
            let message = entry.record
            if message.role == "tool_result", message.toolResultIsError == true { return true }
            // A call's arguments may themselves contain error/status fields.
            if let output = message.toolOutput, Self.explicitFailure(output) { return true }
            return ["tool_result", "tool"].contains(message.role) && Self.explicitFailure(message.text)
        }
        func value(_ title: String, _ symbol: String, _ category: String? = nil) -> ActivityPresentation {
            ActivityPresentation(title: interruption.map { $0 + " · " + title } ?? (failed ? "Failed · " + title : title),
                                 symbolName: interruption == nil ? symbol : "pause.circle", hasFailure: failed, category: category,
                                 isInterrupted: interruption != nil)
        }
        if let label = message.contextLabel { return value(label, "doc.text") }
        if message.isEnvironmentContext { return value("Environment context", "info.circle") }
        if message.isInstruction { return value(message.role.capitalized + " instructions", "doc.text") }
        if message.role == "reasoning" { return value("Reasoning", "ellipsis.bubble") }
        if message.role == "assistant" { return value(Self.compact(message.text), "text.bubble") }

        // Match a complete tool name, including the final component of provider namespaces.
        // Arbitrary executable source (such as functions.exec JavaScript) is never parsed.
        let name = (message.toolName ?? "").components(separatedBy: "__").last?
            .components(separatedBy: ".").last?.lowercased() ?? ""
        let input = message.toolInput.flatMap(Self.object)
        func detail(_ keys: [String]) -> String? {
            keys.lazy.compactMap { input?[$0] as? String }.first { $0.nilIfBlank != nil }
        }
        func title(_ verb: String, _ detail: String?) -> String {
            guard let detail else { return verb }
            return verb + " " + Self.compact(detail)
        }
        switch name {
        case "read", "read_file", "readfile":
            return value(title("Read", detail(["file_path", "path", "filename"])), "doc.text", "Read")
        case "grep", "search", "search_code", "search_files", "search_query", "ripgrep":
            return value(title("Search", detail(["pattern", "query", "q", "search_term"])), "magnifyingglass", "Search")
        case "glob", "find_files", "list_files", "list_directory", "ls":
            return value(title("List files", detail(["pattern", "glob", "path", "directory"])), "folder", "Search")
        case "bash", "shell", "shell_command", "exec_command", "run_command", "terminal":
            return value(title("Run", detail(["command", "cmd"])), "terminal", "Command")
        case "exec" where detail(["command", "cmd"]) != nil:
            return value(title("Run", detail(["command", "cmd"])), "terminal", "Command")
        case "exec" where message.toolName == "functions.exec":
            return value("Run tool script", "terminal", "Command")
        case "wait", "write_stdin":
            return value("Wait for command", "terminal", "Command")
        case "view_image", "open_image":
            return value(title("View image", detail(["path", "image_path"])), "photo", "Image")
        case "write", "write_file", "writefile", "create_file":
            return value(title("Write", detail(["file_path", "path", "filename"])), "doc.badge.plus", "Write")
        case "edit", "edit_file", "editfile", "multiedit", "multi_edit", "replace_in_file":
            return value(title("Edit", detail(["file_path", "path", "filename"])), "pencil", "Edit")
        case "apply_patch":
            return value("Apply patch", "pencil", "Edit")
        case "web_fetch", "webfetch", "fetch_url":
            return value(title("Fetch", detail(["url"])), "globe", "Search")
        default:
            return value(message.activityTitle, "wrench.and.screwdriver")
        }
    }

    private static func compact(_ text: String) -> String {
        let line = text.split(whereSeparator: \.isWhitespace).joined(separator: " ")
        return line.count > 88 ? String(line.prefix(87)) + "…" : line
    }

    private static func object(_ text: String) -> [String: Any]? {
        guard let data = text.data(using: .utf8) else { return nil }
        return (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
    }

    private static func explicitFailure(_ text: String) -> Bool {
        guard let output = object(text) else { return false }
        // JSON booleans and numbers both bridge to NSNumber; distinguish them so
        // malformed numeric error flags or boolean exit codes do not imply failure.
        for key in ["isError", "is_error"] {
            if let flag = output[key] as? NSNumber,
               CFGetTypeID(flag) == CFBooleanGetTypeID(), flag.boolValue { return true }
        }
        if let code = output["exit_code"] as? NSNumber,
           CFGetTypeID(code) != CFBooleanGetTypeID(), code.doubleValue != 0 { return true }
        return false
    }
}
