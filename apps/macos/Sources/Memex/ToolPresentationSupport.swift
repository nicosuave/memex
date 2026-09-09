import AppKit

/// Decorations never change the underlying text or interpret executable tool input.
@MainActor enum ToolPresentationSupport {
    static let codeLanguageAttribute = NSAttributedString.Key("MemexToolCodeLanguage")

    static func decorate(_ text: String, path: String, toolName: String, code: Bool) -> NSAttributedString? {
        let name = toolName.components(separatedBy: "__").last?.components(separatedBy: ".").last?.lowercased() ?? ""
        let key = path.components(separatedBy: ".").last ?? ""
        let fileTool = ["read", "read_file", "readfile", "write", "write_file", "edit", "edit_file"].contains(name)
        let searchTool = ["grep", "search", "search_code", "search_files", "ripgrep"].contains(name)
        let commandTool = ["bash", "shell", "shell_command", "exec_command", "run_command", "terminal", "exec", "write_stdin"].contains(name)
        let commandField = ["cmd", "command", "code", "script", "output"].contains(key) || path.isEmpty
        let patch = ["patch", "diff"].contains(key) || name == "apply_patch"
        let pathValue = ["path", "file_path", "filename"].contains(key) && text.hasPrefix("/")
        guard patch || fileTool || searchTool || pathValue || (commandTool && commandField) else { return nil }
        let language = patch ? "diff" : (commandTool && code && path.isEmpty && toolName == "functions.exec" ? "javascript" : (["cmd", "command"].contains(key) ? "sh" : "text"))
        let result = NSMutableAttributedString(attributedString: CodeSyntax.render(text, language: language, font: .systemFont(ofSize: 13)))
        let style = NSMutableParagraphStyle()
        style.lineSpacing = 2
        style.paragraphSpacing = 0
        result.addAttribute(.paragraphStyle, value: style, range: NSRange(location: 0, length: result.length))
        let fileContent = fileTool && (path.isEmpty || ["content", "text", "output"].contains(key))
        if result.length > 0, patch || fileContent || (commandTool && commandField) {
            result.addAttribute(codeLanguageAttribute, value: language, range: NSRange(location: 0, length: result.length))
        }
        if pathValue { result.addAttributes([RichTextRenderer.sourceLocationAttribute: text, .foregroundColor: NSColor.linkColor], range: NSRange(location: 0, length: result.length)) }
        if searchTool, let expression = try? NSRegularExpression(pattern: #"(?m)^(/[^\n:]+):(\d+)(?::(\d+))?:"#) {
            let source = text as NSString
            for match in expression.matches(in: text, range: NSRange(location: 0, length: source.length)) {
                let file = source.substring(with: match.range(at: 1))
                let destination = file + ":" + source.substring(with: match.range(at: 2))
                result.addAttributes([RichTextRenderer.sourceLocationAttribute: destination, .foregroundColor: NSColor.linkColor], range: match.range)
            }
        }
        return result
    }
}
