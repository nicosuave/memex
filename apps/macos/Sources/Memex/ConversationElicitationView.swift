import SwiftUI

#if canImport(SQACPHost)
import SQACPHost

/// Kept separate from approval options: accepting a form must carry validated values.
struct ConversationElicitationView: View {
    let approval: ConversationApproval
    let canRespond: Bool
    let onRespond: (String) -> Void
    @State private var values: [String: AgentElicitationValue] = [:]
    @State private var numericText: [String: String] = [:]
    @State private var error: String?

    private var form: Result<AgentElicitationForm, Error> {
        Result { try AgentElicitationForm(requestJSON: approval.detail ?? "{}") }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(approval.title).font(.headline)
            switch form {
            case .success(let form):
                Text(form.serverName).font(.caption).foregroundStyle(.secondary)
                ForEach(form.fields) { field in
                    VStack(alignment: .leading, spacing: 4) {
                        Text(field.title + (field.required ? " *" : "")).font(.subheadline)
                        if let description = field.description {
                            Text(description).font(.caption).foregroundStyle(.secondary)
                        }
                        input(field)
                        if !field.required, values[field.id] != nil {
                            Button("Omit \(field.title)") {
                                values.removeValue(forKey: field.id)
                                numericText.removeValue(forKey: field.id)
                            }.font(.caption).buttonStyle(.link)
                        }
                    }
                }
                Button("Submit form") { submit(form) }.disabled(!canRespond)
            case .failure(let failure):
                Text(failure.localizedDescription).font(.caption).foregroundStyle(.orange)
            }
            if let error { Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled) }
            HStack {
                Button("Decline") { respond(.init(action: .decline)) }
                Button("Cancel request") { respond(.init(action: .cancel)) }
            }.disabled(!canRespond)
            if let detail = approval.detail {
                DisclosureGroup("Request details") {
                    ScrollView { Text(detail).font(.system(.caption, design: .monospaced)).textSelection(.enabled) }
                        .frame(maxHeight: 180)
                }.font(.caption)
            }
        }
        .padding(12)
        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 8))
        .disabled(!canRespond)
        .task(id: approval.id) {
            guard case .success(let form) = form else { return }
            values = Dictionary(uniqueKeysWithValues: form.fields.compactMap { field in
                field.defaultValue.map { (field.id, $0) }
            })
            numericText = Dictionary(uniqueKeysWithValues: form.fields.compactMap { field in
                guard case .number(let value) = field.defaultValue else { return nil }
                return (field.id, String(value))
            })
        }
    }

    @ViewBuilder private func input(_ field: AgentElicitationField) -> some View {
        switch field.kind {
        case .boolean:
            Picker(field.title, selection: Binding(get: {
                guard case .boolean(let value) = values[field.id] else { return "" }
                return value ? "true" : "false"
            }, set: { value in values[field.id] = value.isEmpty ? nil : .boolean(value == "true") })) {
                Text("Choose…").tag("")
                Text("Yes").tag("true")
                Text("No").tag("false")
            }.labelsHidden()
        case .array:
            ForEach(field.choices) { choice in
                Toggle(choice.title, isOn: Binding(get: {
                    guard case .strings(let selected) = values[field.id] else { return false }
                    return selected.contains(choice.value)
                }, set: { enabled in
                    var selected: [String] = []
                    if case .strings(let current) = values[field.id] { selected = current }
                    selected.removeAll { $0 == choice.value }
                    if enabled { selected.append(choice.value) }
                    values[field.id] = .strings(selected)
                }))
            }
            if values[field.id] == nil {
                Button("Use empty selection") { values[field.id] = .strings([]) }.font(.caption)
            }
        case .number, .integer:
            TextField(field.kind == .integer ? "Whole number" : "Number", text: Binding(
                get: { numericText[field.id] ?? "" }, set: { numericText[field.id] = $0 }))
                .textFieldStyle(.roundedBorder)
        case .string:
            if field.choices.isEmpty {
                TextField(field.format ?? field.title, text: stringBinding(field.id)).textFieldStyle(.roundedBorder)
            } else {
                Picker(field.title, selection: Binding<String?>(get: {
                    guard case .string(let value) = values[field.id] else { return nil }
                    return value
                }, set: { value in values[field.id] = value.map(AgentElicitationValue.string) })) {
                    Text("Choose…").tag(String?.none)
                    ForEach(field.choices) { choice in Text(choice.title).tag(Optional(choice.value)) }
                }.labelsHidden()
            }
        }
    }

    private func stringBinding(_ id: String) -> Binding<String> {
        Binding(get: {
            guard case .string(let value) = values[id] else { return "" }
            return value
        }, set: { values[id] = .string($0) })
    }

    private func submit(_ form: AgentElicitationForm) {
        do {
            var content = values
            for field in form.fields where field.kind == .number || field.kind == .integer {
                if let text = numericText[field.id], !text.trimmingCharacters(in: .whitespaces).isEmpty {
                    guard let number = Double(text), number.isFinite else {
                        error = "\(field.title): enter a valid number."
                        return
                    }
                    content[field.id] = .number(number)
                } else { content.removeValue(forKey: field.id) }
            }
            try form.validate(content)
            respond(.init(action: .accept, content: content))
        } catch { self.error = error.localizedDescription }
    }

    private func respond(_ response: AgentElicitationReply) {
        guard canRespond else { return }
        do { error = nil; onRespond(try response.encoded()) }
        catch { self.error = error.localizedDescription }
    }
}
#endif
