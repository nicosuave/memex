import AppKit

/// Small, conservative lexical highlighter. Unknown languages remain literal.
@MainActor enum CodeSyntax {
    static func render(_ source: String, language: String, font: NSFont) -> NSAttributedString {
        let result = NSMutableAttributedString(string: source, attributes: [
            .font: NSFont.monospacedSystemFont(ofSize: font.pointSize - 1, weight: .regular),
            .foregroundColor: NSColor.labelColor,
        ])
        let language = language.lowercased().split(separator: " ").first.map(String.init) ?? "text"
        let full = NSRange(location: 0, length: result.length)
        if ["diff", "patch"].contains(language) {
            for (pattern, color) in [("(?m)^\\+.*$", NSColor.systemGreen), ("(?m)^-.*$", NSColor.systemRed), ("(?m)^@@.*$", NSColor.systemPurple)] {
                apply(pattern, color: color, to: result, range: full)
            }
            return result
        }
        guard ["swift", "rust", "rs", "javascript", "js", "typescript", "ts", "tsx", "jsx", "python", "py", "sql", "json", "bash", "sh", "zsh", "c", "cpp", "go", "java", "ruby", "rb", "yaml", "yml", "toml"].contains(language) else { return result }
        // Consume quoted strings and comments as complete tokens so keywords
        // inside them are never colored as executable code.
        let hashComments = ["python", "py", "bash", "sh", "zsh", "ruby", "rb", "yaml", "yml", "toml"].contains(language)
        let comment = language == "sql" ? "--[^\\n]*" : (hashComments ? "#[^\\n]*" : "//[^\\n]*|/\\*[\\s\\S]*?\\*/")
        let pattern = "\"(?:\\\\.|[^\"\\\\])*\"|'(?:\\\\.|[^'\\\\])*'|`(?:\\\\.|[^`\\\\])*`|" + comment + "|\\b[0-9]+(?:\\.[0-9]+)?\\b|\\b[A-Za-z_][A-Za-z_0-9]*\\b"
        let keywords = Set("let var const func fn return if else for while switch case break continue class struct enum protocol extension import from as async await try catch throw throws public private static mut impl use pub def in is not and or None True False null true false select with recursive distinct from where group by order asc desc having limit offset join left right inner outer on union all insert into values update set delete create table alter drop begin end function export default new interface type guard defer do self super package match some nil void int string boolean bool final override actor nonisolated".lowercased().split(separator: " ").map(String.init))
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return result }
        let ns = source as NSString
        for match in regex.matches(in: source, range: full) {
            let token = ns.substring(with: match.range)
            let color: NSColor?
            if token.hasPrefix("\"") || token.hasPrefix("'") || token.hasPrefix("`") { color = .systemRed }
            else if token.hasPrefix("//") || token.hasPrefix("/*") || token.hasPrefix("#") || token.hasPrefix("--") { color = .secondaryLabelColor }
            else if token.first?.isNumber == true { color = .systemOrange }
            else if keywords.contains(token.lowercased()) { color = .systemPurple }
            else { color = nil }
            if let color { result.addAttribute(.foregroundColor, value: color, range: match.range) }
        }
        return result
    }

    private static func apply(_ pattern: String, color: NSColor, to result: NSMutableAttributedString, range: NSRange) {
        guard let regex = try? NSRegularExpression(pattern: pattern) else { return }
        for match in regex.matches(in: result.string, range: range) { result.addAttribute(.foregroundColor, value: color, range: match.range) }
    }
}
