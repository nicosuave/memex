import AppKit
import SwiftUI

#if canImport(SQACPUI)
import SQACP
import SQACPUI

struct ConversationQuestionsView: View {
    @Bindable var conversation: LiveConversation
    @State private var navigation = ConversationQuestionNavigation()
    @State private var drafts = AcpStructuredInputDrafts()
    @State private var contexts: [String: [ConversationQuestionContext]] = [:]
    @State private var loading: Set<String> = []
    @State private var errors: [String: String] = [:]

    private var questions: [ConversationQuestion] { conversation.snapshot.questions }
    private var selected: ConversationQuestion? {
        questions.first { $0.id == navigation.selectedID } ?? questions.first
    }

    var body: some View {
        Group {
            if let question = selected {
                VStack(alignment: .leading, spacing: 8) {
                    if questions.count > 1 {
                        HStack {
                            Text("Question \(navigation.index + 1) of \(questions.count) pending")
                            Spacer()
                            Button("Back") { navigation.move(-1) }.disabled(!navigation.canGoBack)
                            Button("Next") { navigation.move(1) }.disabled(!navigation.canGoNext)
                        }.font(.caption)
                    }
                    AcpStructuredUserInputView(input: question.item,
                        isResponding: conversation.submitting || !conversation.snapshot.connected || loading.contains(question.id),
                        draft: Binding(get: { drafts.draft(for: question.item) },
                                       set: { drafts.set($0, for: question.id) }),
                        additionalContent: AnyView(attachmentControls(question)),
                        hasAdditionalAnswer: !(contexts[question.id] ?? []).isEmpty,
                        onSubmit: { values, text in submit(question, values: values, text: text) },
                        onCancel: { Task { await conversation.stop() } })
                        .id(question.id)
                }
            }
        }
        .onChange(of: questions.map(\.id), initial: true) { _, ids in
            navigation.reconcile(ids)
            // Discard only questions acknowledged/removed by the provider;
            // changing the visible step leaves every pending draft intact.
            drafts.retainPending(ids)
            contexts = contexts.filter { ids.contains($0.key) }
            errors = errors.filter { ids.contains($0.key) }
        }
    }

    private func attachmentControls(_ question: ConversationQuestion) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            ForEach(contexts[question.id] ?? []) { context in
                HStack {
                    Label(context.name, systemImage: "doc.text")
                    Spacer()
                    Button("Remove") { contexts[question.id]?.removeAll { $0.id == context.id } }
                }
            }
            Button("Attach text files to answer…") { Task { await attach(to: question) } }
            Text("Captured UTF-8 contents are included in this answer. Native question replies do not accept media attachments.")
                .foregroundStyle(.secondary)
            if let error = errors[question.id] { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
        }.font(.caption)
    }

    private func submit(_ question: ConversationQuestion, values: [String], text: String) {
        guard !loading.contains(question.id) else { return }
        let answer = ConversationQuestionContext.answerText(text, contexts: contexts[question.id] ?? [])
        Task {
            do {
                // Text context is a separate answer value, leaving choice values
                // exact, including commas; adapters retain each native request ID.
                let response = values.isEmpty ? answer : try AcpUserInputAnswer(selectedValues: values, text: answer).encoded()
                await conversation.answer(question, text: response)
            } catch { errors[question.id] = error.localizedDescription }
        }
    }

    @MainActor private func attach(to question: ConversationQuestion) async {
        guard !loading.contains(question.id) else { return }
        loading.insert(question.id)
        defer { loading.remove(question.id) }
        let panel = NSOpenPanel()
        panel.canChooseDirectories = false
        panel.allowsMultipleSelection = true
        panel.prompt = "Attach to answer"
        // No conversation cwd is read here: remote paths belong to the host.
        guard await panel.begin() == .OK else { return }
        do {
            let urls = panel.urls
            let current = contexts[question.id] ?? []
            let captured = try await Task.detached { try ConversationQuestionContext.capture(urls, existing: current) }.value
            guard questions.contains(where: { $0.id == question.id }) else { return }
            contexts[question.id] = current + captured
            errors[question.id] = nil
        } catch { errors[question.id] = error.localizedDescription }
    }
}

private extension ConversationQuestion {
    var item: AcpComposerPendingUserInputItem {
        .init(id: id, title: title, prompt: prompt, placeholder: placeholder, defaultValue: defaultValue,
            choices: choices.map { .init(id: $0.id, title: $0.title, value: $0.value, description: $0.description) },
            multiSelect: multiSelect, isSecret: isSecret)
    }
}
#endif
