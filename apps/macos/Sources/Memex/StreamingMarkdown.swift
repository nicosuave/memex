import AppKit
import Markdown

/// Display-only repairs for the unfinished end of a streaming Markdown message.
/// Every rule withholds a trailing partial construct or appends closers to the
/// last, unterminated line, so text before that line is never changed and a
/// complete message is never rewritten. A wrong guess shows up as text that
/// snaps into another shape, so each rule leans towards changing nothing.
enum StreamingMarkdownHealer {
    static func heal(_ text: String) -> String { healTrailingLine(holdBackIncompleteLine(text)) }

    struct FenceRun {
        let character: Character
        let count: Int
        let rest: Substring
    }

    /// Withholds an unterminated last line whose block meaning is still unknown:
    /// a fence opener whose language is half typed, a partial closing fence, or
    /// a bare `-`, `#`, `>`, `1.` that would otherwise flash as a list item,
    /// heading or setext underline.
    static func holdBackIncompleteLine(_ text: String) -> String {
        guard let last = text.last, last != "\n" else { return text }
        let lineStart = text.lastIndex(of: "\n").map(text.index(after:)) ?? text.startIndex
        let line = text[lineStart...]
        let withhold: Bool
        if let fence = openFence(in: text[..<lineStart]) {
            if let run = fenceRun(line), run.character == fence.character, run.count < fence.count,
               run.rest.allSatisfy(isSpaceOrTab) { withhold = true } else { withhold = false }
        } else if let run = fenceRun(line), run.character == "~" || !run.rest.contains("`"),
                  run.count >= 3 || run.rest.allSatisfy(isSpaceOrTab) {
            withhold = true
        } else {
            withhold = isBareBlockMarker(line)
        }
        return withhold ? String(text[..<lineStart]) : text
    }

    /// Closes inline constructs opened on the unterminated last line. Lines
    /// that end in a newline are left alone: their paragraph may continue.
    static func healTrailingLine(_ text: String) -> String {
        guard let last = text.last, last != "\n" else { return text }
        let lineStart = text.lastIndex(of: "\n").map(text.index(after:)) ?? text.startIndex
        let line = text[lineStart...]
        // Code and raw HTML are literal; nothing in them can be repaired.
        guard openFence(in: text[..<lineStart]) == nil, (fenceRun(line)?.count ?? 0) < 3 else { return text }
        let content = line.drop(while: isSpaceOrTab)
        let indent = line.prefix(while: isSpaceOrTab).reduce(0) { $0 + ($1 == "\t" ? 4 : 1) }
        if indent >= 4 && !startsListItem(content) { return text }
        if content.first == "<" { return text }
        var scopeStart = lineStart
        if content.first == "|" {
            // Table cells are separate inline contexts: heal only the last one.
            var index = content.startIndex
            var previous: Character?
            while index < content.endIndex {
                if content[index] == "|" && previous != "\\" { scopeStart = content.index(after: index) }
                previous = content[index]
                index = content.index(after: index)
            }
        }
        let scope = String(text[scopeStart...])
        let healed = healInline(scope)
        return healed == scope ? text : String(text[..<scopeStart]) + healed
    }

    /// Inline healing of one line: hides a half-written link or image target,
    /// closes a code span, then closes emphasis and strikethrough.
    static func healInline(_ scope: String) -> String {
        var characters = Array(scope)
        healPartialLink(&characters)
        let code = scanCode(characters)
        var removals: [Range<Int>] = []
        var suffix = ""
        let emphasisEnd = code.open?.start ?? characters.count
        var stack: [(character: Character, count: Int)] = []
        var partialCloser: (character: Character, count: Int)?
        var index = 0
        while index < emphasisEnd {
            let character = characters[index]
            guard !code.literal[index], character == "*" || character == "_" || character == "~" else { index += 1; continue }
            var end = index
            while end < emphasisEnd, characters[end] == character, !code.literal[end] { end += 1 }
            let count = end - index
            let before: Character? = index > 0 ? characters[index - 1] : nil
            let after: Character? = end < characters.count ? characters[end] : nil
            let flanks = flanking(character, before: before, after: after)
            let closesAtEnd = end == characters.count
            if flanks.close, let opener = stack.lastIndex(where: { $0.character == character }),
               character != "~" || count <= 2 {
                if closesAtEnd && count < stack[opener].count {
                    partialCloser = (character, stack[opener].count - count)
                }
                stack.removeSubrange(opener...)
            } else if flanks.open, isConfidentOpener(character, count: count, before: before, after: after) {
                stack.append((character, count))
            } else if closesAtEnd, before.map(\.isWhitespace) ?? true {
                // A marker typed before its content, as in `Hello **`.
                removals.append(index..<end)
            }
            index = end
        }
        if let open = code.open {
            let contentStart = open.start + open.length
            if contentStart == characters.count {
                removals.append(open.start..<characters.count)
            } else {
                var trailing = 0
                while trailing < characters.count - contentStart, characters[characters.count - 1 - trailing] == "`" { trailing += 1 }
                if trailing < open.length { suffix += String(repeating: "`", count: open.length - trailing) }
            }
        }
        if let partialCloser { suffix += String(repeating: partialCloser.character, count: partialCloser.count) }
        let closers = stack.reversed().map { String(repeating: $0.character, count: $0.count) }.joined()
        for range in removals.sorted(by: { $0.lowerBound > $1.lowerBound }) { characters.removeSubrange(range) }
        if !closers.isEmpty || partialCloser != nil {
            // A closer after whitespace cannot close; Markdown drops trailing spaces anyway.
            while let last = characters.last, last == " " || last == "\t" { characters.removeLast() }
        }
        return String(characters) + suffix + closers
    }

    // MARK: Inline scanning

    /// `[text](http://par` shows as `text`; a half-written image is hidden.
    private static func healPartialLink(_ characters: inout [Character]) {
        let code = scanCode(characters)
        guard code.open == nil else { return }
        // A trailing `![alt` whose alt text is still arriving.
        if let bang = characters.indices.last(where: { $0 + 1 < characters.count && characters[$0] == "!" && characters[$0 + 1] == "[" && !code.literal[$0] }),
           !characters[(bang + 2)...].contains("]") {
            characters.removeSubrange(bang...)
            return
        }
        guard let close = characters.indices.last(where: {
            $0 + 1 < characters.count && characters[$0] == "]" && characters[$0 + 1] == "(" && !code.literal[$0]
        }) else { return }
        let destination = characters[(close + 2)...]
        guard !destination.contains(")"), !destination.contains(where: \.isWhitespace) else { return }
        var depth = 0
        var open: Int?
        for position in stride(from: close - 1, through: 0, by: -1) where !code.literal[position] {
            if characters[position] == "]" { depth += 1 }
            if characters[position] == "[" {
                if depth == 0 { open = position; break }
                depth -= 1
            }
        }
        guard let open else { return }
        if open > 0, characters[open - 1] == "!", !code.literal[open - 1] {
            characters.removeSubrange((open - 1)...)
        } else {
            characters = Array(characters[..<open]) + Array(characters[(open + 1)..<close])
        }
    }

    private struct CodeScan {
        /// Characters inside closed code spans or escapes; emphasis ignores them.
        var literal: [Bool]
        /// An unclosed code span that is being written.
        var open: (start: Int, length: Int)?
    }

    private static func scanCode(_ characters: [Character]) -> CodeScan {
        var literal = [Bool](repeating: false, count: characters.count)
        var index = 0
        while index < characters.count {
            let character = characters[index]
            if character == "\\", index + 1 < characters.count, isASCIIPunctuation(characters[index + 1]) {
                literal[index] = true; literal[index + 1] = true
                index += 2
                continue
            }
            guard character == "`" else { index += 1; continue }
            var length = 0
            while index + length < characters.count, characters[index + length] == "`" { length += 1 }
            var search = index + length
            var closing: Int?
            while search < characters.count {
                guard characters[search] == "`" else { search += 1; continue }
                var run = 0
                while search + run < characters.count, characters[search + run] == "`" { run += 1 }
                if run == length { closing = search; break }
                search += run
            }
            if let closing {
                for position in index..<(closing + length) { literal[position] = true }
                index = closing + length
                continue
            }
            // Unclosed: a span only when content follows directly, as in `name`.
            // A lone backtick before a space (`the ` key`) stays literal text.
            let next = index + length < characters.count ? characters[index + length] : nil
            if next.map({ !$0.isWhitespace }) ?? true { return CodeScan(literal: literal, open: (index, length)) }
            for position in index..<(index + length) { literal[position] = true }
            index += length
        }
        return CodeScan(literal: literal, open: nil)
    }

    /// CommonMark left- and right-flanking rules for a delimiter run.
    private static func flanking(_ character: Character, before: Character?, after: Character?) -> (open: Bool, close: Bool) {
        let beforeSpace = before?.isWhitespace ?? true
        let afterSpace = after?.isWhitespace ?? true
        let beforePunctuation = before.map(isPunctuation) ?? false
        let afterPunctuation = after.map(isPunctuation) ?? false
        let left = !afterSpace && (!afterPunctuation || beforeSpace || beforePunctuation)
        let right = !beforeSpace && (!beforePunctuation || afterSpace || afterPunctuation)
        if character == "_" { return (left && (!right || beforePunctuation), right && (!left || afterPunctuation)) }
        return (left, right)
    }

    /// Only an opener at the start of a word followed by content is closed
    /// speculatively; `2*x`, `*.swift`, `**/*.ts` and `snake_case` are not.
    private static func isConfidentOpener(_ character: Character, count: Int, before: Character?, after: Character?) -> Bool {
        if character == "~" ? count != 2 : count > 3 { return false }
        if let before, before.isLetter || before.isNumber || before == character { return false }
        guard let after else { return false }
        return after.isLetter || after.isNumber || "`[\"'“‘".contains(after) || (!after.isASCII && after.isSymbol)
    }

    // MARK: Line helpers

    static func fenceRun(_ line: Substring) -> FenceRun? {
        let content = line.drop(while: { $0 == " " })
        guard let character = content.first, character == "`" || character == "~" else { return nil }
        let run = content.prefix(while: { $0 == character })
        return FenceRun(character: character, count: run.count, rest: content[run.endIndex...])
    }

    /// The fence still open at the end of `text`, if any.
    static func openFence(in text: Substring) -> FenceRun? {
        var open: FenceRun?
        for line in text.split(separator: "\n", omittingEmptySubsequences: false) {
            guard let run = fenceRun(line) else { continue }
            if let current = open {
                if run.character == current.character, run.count >= current.count, run.rest.allSatisfy(isSpaceOrTab) { open = nil }
            } else if run.count >= 3, run.character == "~" || !run.rest.contains("`") {
                open = run
            }
        }
        return open
    }

    private static func isBareBlockMarker(_ line: Substring) -> Bool {
        let compact = line.filter { !isSpaceOrTab($0) }
        guard !compact.isEmpty else { return false }
        if compact.allSatisfy({ "-*+=_#>".contains($0) }) { return true }
        guard let last = compact.last, last == "." || last == ")", compact.count <= 10 else { return false }
        let digits = compact.dropLast()
        return !digits.isEmpty && digits.allSatisfy(\.isASCIIDigit)
    }

    private static func startsListItem(_ content: Substring) -> Bool {
        if let first = content.first, "-*+".contains(first) { return content.dropFirst().first.map(isSpaceOrTab) ?? true }
        let digits = content.prefix(while: \.isASCIIDigit)
        guard !digits.isEmpty, let marker = content.dropFirst(digits.count).first else { return false }
        return marker == "." || marker == ")"
    }

    private static func isSpaceOrTab(_ character: Character) -> Bool { character == " " || character == "\t" }
    private static func isPunctuation(_ character: Character) -> Bool { character.isPunctuation || character.isSymbol }
    private static func isASCIIPunctuation(_ character: Character) -> Bool { character.isASCII && isPunctuation(character) }
}

private extension Character {
    var isASCIIDigit: Bool { isASCII && isNumber }
}

/// Renders a growing Markdown message so that only its unfinished tail is
/// parsed and rendered again. A top-level block settles once the first line of
/// the block after it is complete: CommonMark decides block structure line by
/// line, so that line closes every earlier block and nothing later can reopen
/// it. Settled blocks keep their rendered text, and parsing restarts at the
/// first unsettled line, which produces the same blocks as a full parse.
///
/// The concatenated output equals `RichTextRenderer.renderMarkdown` of the
/// same source when `heals` is false. With `heals`, only the last block is
/// rendered from repaired source; settled blocks are never healed.
@MainActor final class StreamingMarkdownRenderer {
    struct Output {
        /// The rendered message, untrimmed.
        let text: NSAttributedString
        /// The UTF-16 prefix of `text` that every later output of the same
        /// generation repeats unchanged.
        let settledLength: Int
        /// Changes whenever settled blocks are discarded.
        let generation: Int
        /// The message needs the ordinary renderer (prompt sections or CR line
        /// endings, which this renderer does not split safely).
        let requiresFullRender: Bool
        /// The message renders as rich content blocks rather than plain text.
        let isRich: Bool
    }

    @MainActor private final class SettledBlock {
        let node: any Markup
        let mayBeRich: Bool
        private var cachedRichBlocks: [RichContentBlock]?
        var items: [RichContentLayout.Item]?
        init(node: any Markup, mayBeRich: Bool) { self.node = node; self.mayBeRich = mayBeRich }
        var richBlocks: [RichContentBlock] {
            if let cachedRichBlocks { return cachedRichBlocks }
            let blocks = RichContentDocument.blocks(for: node)
            cachedRichBlocks = blocks
            return blocks
        }
        var hasRichBlocks: Bool { mayBeRich && richBlocks.contains(where: \.isRich) }
    }

    let font: NSFont
    let heals: Bool
    /// Number of top-level blocks rendered so far; tests use it to prove that
    /// settled blocks are not rendered again.
    private(set) var renderedBlockCount = 0
    private(set) var generation: Int
    private var settledSource = ""
    private let settledText = NSMutableAttributedString()
    private var settledBlocks: [SettledBlock] = []
    private var settledMayHaveRich = false
    private var hasReferenceDefinitions = false
    private var requiresFullRender = false
    private var tailNodes: [any Markup] = []
    private var tailRichBlocks: [[RichContentBlock]]?
    private var regionMayHaveRich = false
    private var itemContext: RichContentContext?
    private static var nextGeneration = 0

    init(font: NSFont, heals: Bool = true) {
        self.font = font
        self.heals = heals
        generation = Self.makeGeneration()
    }

    private static func makeGeneration() -> Int {
        nextGeneration += 1
        return nextGeneration
    }

    var settledBlockCount: Int { settledBlocks.count }

    func render(_ source: String) -> Output {
        var source = source
        source.makeContiguousUTF8()
        if !settledSource.isEmpty, !Self.utf8(source, hasPrefix: settledSource) { reset() }
        let region = String(decoding: source.utf8.dropFirst(settledSource.utf8.count), as: UTF8.self)
        // Line splitting below is by LF only; CR line endings keep the ordinary renderer.
        if !requiresFullRender, region.utf8.contains(13) || Self.mayContainPromptSection(region) { requiresFullRender = true }
        guard !requiresFullRender else {
            return Output(text: NSAttributedString(), settledLength: 0, generation: generation,
                          requiresFullRender: true, isRich: false)
        }
        if !hasReferenceDefinitions, Self.mayContainReferenceDefinition(region) {
            // A definition can resolve links anywhere in the message, so the
            // message is parsed as one document from here on.
            hasReferenceDefinitions = true
            if !settledSource.isEmpty {
                reset()
                return render(source)
            }
        }
        let displayed = heals ? StreamingMarkdownHealer.holdBackIncompleteLine(region) : region
        let lines = displayed.split(separator: "\n", omittingEmptySubsequences: false)
        var nodes: [any Markup] = Array(Document(parsing: displayed).children)
        let startLines = nodes.map { $0.range?.lowerBound.line ?? 1 }
        var promoted = 0
        if !hasReferenceDefinitions {
            while promoted + 1 < nodes.count, startLines[promoted + 1] < lines.count { promoted += 1 }
        }
        if promoted > 0 {
            var lineOffsets = [0]
            lineOffsets.reserveCapacity(lines.count)
            for line in lines { lineOffsets.append(lineOffsets[lineOffsets.count - 1] + line.utf8.count + 1) }
            for index in 0..<promoted {
                let first = index == 0 ? 0 : startLines[index] - 1
                let chunk = displayed.utf8.dropFirst(lineOffsets[first]).prefix(lineOffsets[startLines[index + 1] - 1] - lineOffsets[first])
                let chunkText = String(decoding: chunk, as: UTF8.self)
                let mayBeRich = Self.mayHaveRichPattern(chunkText)
                settledMayHaveRich = settledMayHaveRich || mayBeRich
                settledText.append(renderBlock(nodes[index]))
                settledBlocks.append(SettledBlock(node: nodes[index], mayBeRich: mayBeRich || nodes[index] is CodeBlock))
                settledSource += chunkText
            }
            nodes.removeFirst(promoted)
        }
        let tailStart = promoted < startLines.count ? startLines[promoted] - 1 : lines.count
        if heals, let last = nodes.last, !(last is CodeBlock), !(last is HTMLBlock), let lastStart = startLines.last {
            let lastSource = lines[(lastStart - 1)...].joined(separator: "\n")
            let healed = StreamingMarkdownHealer.healTrailingLine(lastSource)
            if healed != lastSource {
                nodes.removeLast()
                nodes += Document(parsing: healed).children.map { $0 }
            }
        }
        let tailText = tailStart < lines.count ? lines[tailStart...].joined(separator: "\n") : ""
        regionMayHaveRich = Self.mayHaveRichPattern(tailText)
        tailNodes = nodes
        tailRichBlocks = nil
        let text = NSMutableAttributedString(attributedString: settledText)
        for node in nodes { text.append(renderBlock(node)) }
        return Output(text: text, settledLength: settledText.length, generation: generation,
                      requiresFullRender: false, isRich: isRich)
    }

    /// The rich content blocks and layout items for the last rendered source.
    /// Settled blocks reuse their items, including code cards, for as long as
    /// the context is unchanged.
    func richContent(context: RichContentContext) -> (blocks: [RichContentBlock], items: [RichContentLayout.Item]) {
        if itemContext != context {
            itemContext = context
            for block in settledBlocks { block.items = nil }
        }
        var blocks: [RichContentBlock] = []
        var items: [RichContentLayout.Item] = []
        for block in settledBlocks {
            let richBlocks = block.richBlocks
            blocks += richBlocks
            if let cached = block.items { items += cached; continue }
            let created = richBlocks.map { RichContentLayout.item(for: $0, font: font, context: context) }
            block.items = created
            items += created
        }
        for tail in currentTailRichBlocks {
            blocks += tail
            items += tail.map { RichContentLayout.item(for: $0, font: font, context: context) }
        }
        return (blocks, items)
    }

    private var isRich: Bool {
        guard settledMayHaveRich || regionMayHaveRich else { return false }
        return settledBlocks.contains(where: \.hasRichBlocks) || currentTailRichBlocks.contains { $0.contains(where: \.isRich) }
    }

    private var currentTailRichBlocks: [[RichContentBlock]] {
        if let tailRichBlocks { return tailRichBlocks }
        let blocks = tailNodes.map { RichContentDocument.blocks(for: $0) }
        tailRichBlocks = blocks
        return blocks
    }

    private func renderBlock(_ node: any Markup) -> NSAttributedString {
        renderedBlockCount += 1
        return RichTextRenderer.renderBlock(node, font: font)
    }

    private func reset() {
        settledSource = ""
        settledText.setAttributedString(NSAttributedString())
        settledBlocks = []
        settledMayHaveRich = false
        generation = Self.makeGeneration()
    }

    /// Byte-wise prefix test; `String.hasPrefix` compares by grapheme and is slow on long text.
    nonisolated static func utf8(_ text: String, hasPrefix prefix: String) -> Bool {
        let count = prefix.utf8.count
        guard text.utf8.count >= count else { return false }
        guard count > 0 else { return true }
        let compared = text.utf8.withContiguousStorageIfAvailable { textBytes in
            prefix.utf8.withContiguousStorageIfAvailable { prefixBytes in
                memcmp(textBytes.baseAddress!, prefixBytes.baseAddress!, count) == 0
            }
        }
        return compared.flatMap { $0 } ?? text.utf8.starts(with: prefix.utf8)
    }

    /// Mirrors the substrings `TranscriptController` checks before building rich content.
    static func mayHaveRichPattern(_ text: String) -> Bool {
        text.contains("```") || text.contains("~~~") || text.contains("![") || text.contains("](/") || text.contains("](file:")
    }

    /// `PromptSections` opens a section only on a line that starts with `<`
    /// and holds an opening tag, outside its fences. Such messages keep the
    /// ordinary renderer; inline HTML inside prose does not.
    private static func mayContainPromptSection(_ text: String) -> Bool {
        var fence: Character?
        for line in text.split(separator: "\n") {
            let trimmed = line.trimmingCharacters(in: .whitespaces)
            if trimmed.hasPrefix("```") || trimmed.hasPrefix("~~~") {
                if fence == nil { fence = trimmed.first } else if fence == trimmed.first { fence = nil }
                continue
            }
            guard fence == nil, trimmed.hasPrefix("<") else { continue }
            let characters = Array(trimmed)
            if characters.indices.dropLast().contains(where: {
                characters[$0] == "<" && (characters[$0 + 1].isLetter || characters[$0 + 1] == "_")
            }) { return true }
        }
        return false
    }

    /// A line that may define a link reference or footnote, in any container.
    private static func mayContainReferenceDefinition(_ text: String) -> Bool {
        text.split(separator: "\n").contains { line in
            guard line.contains("]:") else { return false }
            let content = line.drop { $0.isWhitespace || $0 == ">" || $0 == "-" || $0 == "*" || $0 == "+" || $0.isNumber || $0 == "." || $0 == ")" }
            return content.first == "["
        }
    }
}
