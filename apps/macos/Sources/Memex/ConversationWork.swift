import Foundation
import SwiftUI

/// Derived only from native structured events or known tool schemas. Prose is
/// never promoted into a plan, and missing child status stays unknown.
enum ConversationWork {
    struct Step: Identifiable, Equatable {
        let id: Int
        let text: String
        let status: String
    }
    struct Plan: Equatable, Identifiable {
        let recordID: String
        let steps: [Step]
        var markdown: String? = nil
        var savedSource: String? = nil
        var id: String { recordID }
        var text: String { markdown ?? steps.map { "[\($0.status)] \($0.text)" }.joined(separator: "\n") }
        func provenance(session: Session) -> String { savedSource ?? "\(session.id)#\(recordID)" }
    }
    struct Agent: Identifiable, Equatable {
        let id: String
        var prompt: String?
        var status: String
        var parentID: String? = nil
        var group: String {
            switch status.lowercased() {
            case "running", "pending_init", "in_progress", "inprogress", "waiting": "Active"
            case "completed", "failed", "errored", "interrupted", "cancelled", "stopped": "Done"
            default: "Status unavailable"
            }
        }
    }
    struct State: Equatable {
        var plan: Plan?
        var agents: [Agent] = []
    }

    static func project(_ records: [TranscriptRecord]) -> State {
        var state = State()
        var calls: [String: Message] = [:]
        var agents: [String: Agent] = [:]
        var order: [String] = []
        func recordAgent(_ id: String, prompt: String?, status: String?, parentID: String? = nil) {
            guard !id.isEmpty else { return }
            if agents[id] == nil { order.append(id) }
            agents[id] = Agent(id: id, prompt: prompt ?? agents[id]?.prompt,
                               status: status ?? agents[id]?.status ?? "unknown",
                               parentID: parentID ?? agents[id]?.parentID)
        }
        for record in records {
            let message = record.record
            let activity = json(message.structuredActivity)
            if let childID = activity["child"]["id"].string {
                let child = activity["child"]
                recordAgent(childID, prompt: child["title"].string, status: child["status"].string,
                            parentID: child["parentSessionID"].string)
            }
            let input = json(message.toolInput)
            let name = message.toolName?.split(separator: ".").last.map(String.init)?.lowercased() ?? ""
            if let id = message.eventID, message.role == "tool_use" { calls[id] = message }
            let rawSteps: [RawTranscriptJSON]
            if !activity["plan"]["_0"].array.isEmpty { rawSteps = activity["plan"]["_0"].array }
            else if name == "update_plan" { rawSteps = input["plan"].array }
            else if name == "todowrite" || name == "todo_write" { rawSteps = input["todos"].array }
            else { rawSteps = [] }
            let steps = rawSteps.enumerated().compactMap { index, step -> Step? in
                guard let text = step["text"].string ?? step["step"].string ?? step["content"].string,
                      !text.isEmpty else { return nil }
                return Step(id: index, text: text, status: step["status"].string ?? "pending")
            }
            if let text = activity["planDocument"]["text"].string, !text.isEmpty {
                state.plan = Plan(recordID: record.id, steps: steps, markdown: text)
            } else if !steps.isEmpty { state.plan = Plan(recordID: record.id, steps: steps) }
            let subagent = activity["subagent"]["_0"]
            for id in subagent["agentIDs"].array.compactMap(\.string) {
                // Tool completion means the orchestration operation finished;
                // it does not prove the child finished its own work.
                recordAgent(id, prompt: subagent["prompt"].string, status: nil)
            }
            guard message.role == "tool_result", let call = message.parentToolUseID.flatMap({ calls[$0] }) else { continue }
            let operation = call.toolName?.split(separator: ".").last.map(String.init)?.lowercased() ?? ""
            let arguments = json(call.toolInput)
            let result = json(message.toolOutput)
            if ["spawn_agent", "spawn_agent_thread", "fork_agent"].contains(operation),
               let id = result["agent_id"].string ?? result["thread_id"].string {
                recordAgent(id, prompt: arguments["message"].string ?? arguments["prompt"].string, status: result["status"].string)
            }
            if operation == "wait", case .object(let statuses) = result["status"] {
                for id in statuses.keys.sorted() {
                    let value = statuses[id] ?? .null
                    let status: String?
                    if let text = value.string { status = text }
                    else if case .object(let fields) = value {
                        status = ["completed", "errored", "interrupted", "running", "pending_init", "not_found"]
                            .first { fields[$0] != nil }
                    }
                    else { status = nil }
                    recordAgent(id, prompt: nil, status: status)
                }
            }
        }
        state.agents = order.compactMap { agents[$0] }
        return state
    }

    private static func json(_ text: String?) -> RawTranscriptJSON {
        guard let text else { return .null }
        return (try? JSONDecoder().decode(RawTranscriptJSON.self, from: Data(text.utf8))) ?? .null
    }
}

struct ConversationWorkView: View {
    let state: ConversationWork.State
    let conversation: LiveConversation?
    let session: Session
    let sessions: [Session]
    let navigate: (Session) -> Void
    let branchPlan: (ConversationWork.Plan) -> Void
    @State private var expanded = false
    @State private var contextError: String?
    @State private var editingPlan: ConversationWork.Plan?
    @State private var selectedChild: ConversationWork.Agent?

    var body: some View {
        if state.plan != nil || !state.agents.isEmpty {
            DisclosureGroup(isExpanded: $expanded) {
                ScrollView {
                    VStack(alignment: .leading, spacing: 10) {
                        if let contextError { Text(contextError).foregroundStyle(.orange) }
                        if let plan = state.plan {
                            Button("Open plan document…") { editingPlan = plan }
                            ForEach(plan.steps) { step in
                                Label(step.text, systemImage: step.status == "completed" ? "checkmark.circle.fill" : step.status == "in_progress" ? "circle.lefthalf.filled" : "circle")
                                    .help(step.status).textSelection(.enabled)
                            }
                            if let conversation {
                                HStack {
                                    Button("Refine") { prepare(plan, prompt: "Refine this plan: ", conversation: conversation) }
                                    Button("Implement") { prepare(plan, prompt: "Implement this plan.", conversation: conversation) }
                                    Button("Implement in new conversation…") { branchPlan(plan) }
                                }
                                .disabled(conversation.isOpenElsewhere)
                                Text("Adds the plan and instructions to your draft for review before sending.")
                                    .font(.caption2).foregroundStyle(.secondary)
                            }
                        }
                        ForEach(["Active", "Done", "Status unavailable"], id: \.self) { group in
                            let agents = state.agents.filter { $0.group == group }
                            if !agents.isEmpty {
                                Text("\(group) · \(agents.count)").fontWeight(.medium)
                                ForEach(agents) { agent in
                                    Button { selectedChild = agent } label: {
                                        HStack {
                                            Image(systemName: "person.crop.circle")
                                            Text(agent.prompt ?? agent.id).lineLimit(2)
                                            Spacer()
                                            Text(agent.status).foregroundStyle(.secondary)
                                            Image(systemName: "chevron.right")
                                        }.contentShape(Rectangle())
                                    }.buttonStyle(.plain)
                                }
                            }
                        }
                    }.padding(.top, 6)
                }.frame(maxHeight: 220)
            } label: {
                HStack {
                    if let plan = state.plan { Text(plan.steps.isEmpty ? "Plan document" : "Plan · \(plan.steps.filter { $0.status == "completed" }.count)/\(plan.steps.count)") }
                    if !state.agents.isEmpty { Text("Agents · \(state.agents.count)") }
                }
            }
            .font(.caption).padding(.horizontal, 20).padding(.vertical, 8)
            .sheet(item: $editingPlan) { plan in
                ConversationPlanEditor(plan: plan, session: session, canImplement: conversation != nil && conversation?.isOpenElsewhere != true,
                    implement: { edited in
                        if let conversation { _ = prepare(edited, prompt: "Implement this plan.", conversation: conversation) }
                    }, branch: branchPlan)
            }
            .sheet(item: $selectedChild) { child in
                ConversationChildView(agent: child, conversation: conversation, session: session,
                                      sessions: sessions, navigate: navigate)
            }
        }
    }

    @discardableResult private func prepare(_ plan: ConversationWork.Plan, prompt: String, conversation: LiveConversation) -> Bool {
        guard conversation.appendContext(title: "Plan", text: plan.text, source: plan.provenance(session: session)) else {
            contextError = "The plan could not be attached. Check conversation access and the attachment limit."
            return false
        }
        contextError = nil
        conversation.draft += (conversation.draft.isEmpty ? "" : "\n\n") + prompt
        return true
    }
}
