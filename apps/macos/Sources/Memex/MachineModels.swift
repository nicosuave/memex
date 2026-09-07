import Foundation

struct MachineChoice: Codable, Identifiable, Sendable, Equatable {
    let id: String
    let label: String

    static let local = MachineChoice(id: "local", label: "This Mac")
}

enum MachineSelection: Hashable, Sendable {
    case all
    case machine(String)
}
