import Foundation

/// Lossless display projection. Synthetic context IDs always point back to the source record.
/// Only leading, complete harness envelopes are recognized; quoted/fenced examples and
/// wrappers appearing after user prose remain normal message content.
enum TranscriptPresentation {
    static func project(_ records: [TranscriptRecord]) -> [TranscriptRecord] {
        records.flatMap { sourceEntry in
            if let reply = questionReply(sourceEntry) { return reply }
            var displayMessage = sourceEntry.record
            displayMessage.text = SourceContent.displayText(displayMessage)
            if displayMessage.role == "assistant" {
                displayMessage.text = assistantDisplayText(displayMessage.text)
            }
            let entry = TranscriptRecord(recordID: sourceEntry.id, record: displayMessage,
                                         sourceRecordID: sourceEntry.sourceRecordID,
                                         rawJSON: sourceEntry.rawJSON ?? (displayMessage.text != sourceEntry.record.text ? sourceEntry.rawTranscriptBody : nil))
            guard entry.record.role == "user", entry.record.contextLabel == nil else { return [entry] }
            var remaining = entry.record.text[...]
            var pieces: [(String, String)] = []
            while let envelope = leadingEnvelope(remaining) {
                pieces.append((String(remaining[..<envelope.end]), envelope.label))
                remaining = remaining[envelope.end...]
            }
            guard !pieces.isEmpty else { return [entry] }
            let hasRequest = !remaining.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
            if !hasRequest, !remaining.isEmpty {
                pieces[pieces.count - 1].0 += remaining
            }
            return pieces.enumerated().map { index, piece in
                var message = entry.record
                message.text = piece.0
                message.contextLabel = piece.1
                return TranscriptRecord(recordID: !hasRequest && pieces.count == 1 ? entry.id : "\(entry.id):context:\(index)",
                                        record: message, sourceRecordID: entry.sourceID, rawJSON: entry.rawJSON ?? sourceEntry.rawTranscriptBody)
            } + (hasRequest ? [requestRecord(entry, text: String(remaining), original: sourceEntry.rawTranscriptBody)] : [])
        }
    }

    /// These are transport markers, not arbitrary XML or Markdown directives. Keep
    /// examples in Markdown code/quotes intact and retain the complete source in rawJSON.
    private static func assistantDisplayText(_ text: String) -> String {
        let source = text as NSString
        let protected = protectedMarkdownRanges(text)
        func isProtected(_ range: NSRange) -> Bool {
            protected.contains { NSIntersectionRange($0, range).length > 0 }
        }
        var removals: [NSRange] = []
        let citationPattern = #"(?m)^[ \t]*<oai-mem-citation>\s*<citation_entries>[^<>]*</citation_entries>\s*<rollout_ids>[^<>]*</rollout_ids>\s*</oai-mem-citation>\s*\z"#
        if let regex = try? NSRegularExpression(pattern: citationPattern),
           let match = regex.firstMatch(in: text, range: NSRange(location: 0, length: source.length)),
           !isProtected(match.range) {
            removals.append(match.range)
        }
        let annotationPattern = #"(?<!:)::?codex-annotation\{index="[0-9]+"\}(?!\})"#
        if let regex = try? NSRegularExpression(pattern: annotationPattern) {
            for match in regex.matches(in: text, range: NSRange(location: 0, length: source.length)) {
                let range = match.range
                guard !isProtected(range), !removals.contains(where: { NSIntersectionRange($0, range).length > 0 }) else { continue }
                // Also retain explicitly quoted literal examples outside Markdown code.
                let before = range.location > 0 ? source.substring(with: NSRange(location: range.location - 1, length: 1)) : ""
                let after = NSMaxRange(range) < source.length ? source.substring(with: NSRange(location: NSMaxRange(range), length: 1)) : ""
                guard !["\"", "'", "“", "‘"].contains(before), !["\"", "'", "”", "’"].contains(after) else { continue }
                removals.append(range)
            }
        }
        guard !removals.isEmpty else { return text }
        let result = NSMutableString(string: text)
        for range in removals.sorted(by: { $0.location > $1.location }) { result.deleteCharacters(in: range) }
        return (result as String).trimmingCharacters(in: .whitespacesAndNewlines)
    }

    private static func protectedMarkdownRanges(_ text: String) -> [NSRange] {
        let source = text as NSString
        var protected: [NSRange] = []
        var offset = 0
        var fence: (character: Character, count: Int)?
        var inlineTicks: Int? = nil
        for line in text.components(separatedBy: "\n") {
            let length = (line as NSString).length
            let range = NSRange(location: offset, length: length)
            let trimmed = line.drop(while: { $0 == " " || $0 == "\t" })
            let first = trimmed.first
            let run = first.map { character in trimmed.prefix(while: { $0 == character }).count } ?? 0
            if let active = fence {
                protected.append(range)
                if first == active.character && run >= active.count,
                   trimmed.dropFirst(run).allSatisfy({ $0.isWhitespace }) { fence = nil }
            } else if (first == "`" || first == "~") && run >= 3 {
                fence = (first!, run)
                protected.append(range)
            } else if trimmed.hasPrefix(">") || line.hasPrefix("    ") || line.hasPrefix("\t") {
                protected.append(range)
            } else {
                var cursor = offset
                let end = offset + length
                var start = inlineTicks == nil ? nil : offset
                while cursor < end {
                    if source.character(at: cursor) == 96 {
                        var next = cursor + 1
                        while next < end && source.character(at: next) == 96 { next += 1 }
                        let count = next - cursor
                        if inlineTicks == count {
                            protected.append(NSRange(location: start ?? offset, length: next - (start ?? offset)))
                            inlineTicks = nil
                            start = nil
                        } else if inlineTicks == nil {
                            inlineTicks = count
                            start = cursor
                        }
                        cursor = next
                    } else { cursor += 1 }
                }
                if let start { protected.append(NSRange(location: start, length: end - start)) }
            }
            offset += length + 1
        }
        return protected
    }

    private static func questionReply(_ entry: TranscriptRecord) -> [TranscriptRecord]? {
        guard entry.record.role == "user", entry.record.contextLabel == nil else { return nil }
        let text = entry.record.text.trimmingCharacters(in: .whitespacesAndNewlines)
        let opening = "<send_user_message_question_reply>"
        let closing = "</send_user_message_question_reply>"
        guard text.hasPrefix(opening), text.hasSuffix(closing) else { return nil }
        let payload = String(text.dropFirst(opening.count).dropLast(closing.count))
        guard let data = payload.data(using: .utf8),
              let replies = try? JSONSerialization.jsonObject(with: data) as? [[String: Any]], !replies.isEmpty,
              replies.allSatisfy({ $0["question"] is String && $0["answer"] is String && $0["questionItemId"] is String }) else { return nil }
        let raw = entry.rawJSON ?? entry.rawTranscriptBody
        return replies.enumerated().flatMap { index, reply -> [TranscriptRecord] in
            var context = entry.record
            context.text = reply["question"] as? String ?? ""
            context.contextLabel = "Question"
            var answer = entry.record
            answer.text = reply["answer"] as? String ?? ""
            return [TranscriptRecord(recordID: "\(entry.id):question:\(index)", record: context, sourceRecordID: entry.sourceID, rawJSON: raw),
                    TranscriptRecord(recordID: index == 0 ? entry.id : "\(entry.id):answer:\(index)", record: answer, sourceRecordID: entry.sourceID, rawJSON: raw)]
        }
    }

    private static func requestRecord(_ entry: TranscriptRecord, text: String, original: String) -> TranscriptRecord {
        var message = entry.record
        message.text = text
        return TranscriptRecord(recordID: entry.id, record: message, sourceRecordID: entry.sourceID, rawJSON: entry.rawJSON ?? original)
    }

    private static func leadingEnvelope(_ input: Substring) -> (end: String.Index, label: String)? {
        let value = input.drop(while: { $0.isWhitespace })
        // Match precise harness opening tags, never arbitrary HTML or XML names.
        let tags: [(String, String)] = [
            ("recommended_plugins", "Available plugins"),
            ("environment_context", "Environment context"),
            ("permissions instructions", "Permissions"),
            ("skills_instructions", "Skills"),
            ("app-context", "Application context"),
            ("collaboration_mode", "Collaboration mode"),
            ("context_window", "Context window")
        ]
        for (tag, label) in tags where value.hasPrefix("<\(tag)>") {
            guard let close = value.range(of: "</\(tag)>") else { return nil }
            return (close.upperBound, label)
        }
        if (value.hasPrefix("# AGENTS.md instructions for ") || value.hasPrefix("# AGENTS.md instructions\n")),
           let newline = value.firstIndex(of: "\n") {
            let afterHeading = value[value.index(after: newline)...].drop(while: { $0.isWhitespace })
            guard afterHeading.hasPrefix("<INSTRUCTIONS>"),
                  let close = afterHeading.range(of: "</INSTRUCTIONS>") else { return nil }
            return (close.upperBound, "Project instructions")
        }
        return nil
    }

    static func group(_ source: [TranscriptRecord]) -> [TranscriptItem] {
        let records = project(source)
        // Require both a recorded final answer and task completion for this exact source
        // turn. Partial pages, aborted turns, and old records keep their original hierarchy.
        let byTurn = Dictionary(grouping: records.filter { $0.record.sourceTurnID?.nilIfBlank != nil }) {
            $0.record.sourceTurnID!
        }
        let completed = Set(byTurn.compactMap { turn, entries -> String? in
            guard entries.contains(where: { $0.record.lifecycleEvent == "task_complete" }),
                  entries.contains(where: { $0.record.role == "assistant" && $0.record.assistantPhase == "final_answer" }),
                  !entries.contains(where: { $0.record.lifecycleEvent == "turn_aborted" }) else { return nil }
            return turn
        })
        func isWork(_ entry: TranscriptRecord) -> Bool {
            guard let turn = entry.record.sourceTurnID, completed.contains(turn), !entry.record.isInstruction else { return false }
            return entry.record.isActivity || (entry.record.role == "assistant" && entry.record.assistantPhase == "commentary")
        }
        var output: [TranscriptItem] = []
        var ordinary: [TranscriptRecord] = []
        var work: [TranscriptRecord] = []
        func flushOrdinary() {
            output += TranscriptItem.groupConsecutive(ordinary)
            ordinary = []
        }
        func flushWork() {
            guard !work.isEmpty else { return }
            // Failures stay individually visible even in an otherwise completed turn.
            let groups = TranscriptItem.groupConsecutive(work)
            var routine: [TranscriptRecord] = []
            func flushRoutine() {
                if !routine.isEmpty { output.append(TranscriptItem(records: routine, isCompletedWork: true)); routine = [] }
            }
            for group in groups {
                if group.activities.contains(where: { $0.presentation.needsAttention }) {
                    flushRoutine()
                    output.append(group)
                } else { routine += group.records }
            }
            flushRoutine()
            work = []
        }
        for entry in records {
            if isWork(entry) {
                flushOrdinary()
                if work.first?.record.sourceTurnID != entry.record.sourceTurnID { flushWork() }
                work.append(entry)
            } else {
                flushWork()
                ordinary.append(entry)
            }
        }
        flushWork()
        flushOrdinary()
        return output
    }
}
