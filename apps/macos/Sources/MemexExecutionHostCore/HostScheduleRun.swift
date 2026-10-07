import Foundation

struct HostScheduleTarget: Codable, Equatable {
    var workspaceID: String
    var provider: String
    var title: String
}

/// One saved occurrence, including creation failures and deliveries whose outcome
/// is unknown. Command acknowledgement alone is never treated as completion.
struct HostScheduleRun: Codable, Equatable {
    var id: String
    var scheduleID: String
    var conversationID: String?
    var createdAt: Date
    var updatedAt: Date
    var status: String
    var notificationPolicy: String
    var error: String?
    var nativeTurnID: String?
    var read = false

    var needsAttention: Bool {
        guard !read, notificationPolicy != "never" else { return false }
        if ["failed", "uncertain", "needs_input", "held"].contains(status) { return true }
        return notificationPolicy == "all" && ["completed", "interrupted"].contains(status)
    }

    mutating func reconcile(queueStatus: String, snapshot: HostValue?, at date: Date) {
        let previous = self
        if ["queued", "held", "uncertain", "cancelled"].contains(queueStatus) { status = queueStatus }
        else if let snapshot {
            let delivery = snapshot["deliveries"].array.first { $0["commandId"].string == id }
            nativeTurnID = delivery?["nativeTurnId"].string ?? nativeTurnID
            if ["failed", "rejected"].contains(delivery?["status"].string ?? "") {
                status = "failed"; error = delivery?["error"].string
            } else if let nativeTurnID,
                      let turn = snapshot["thread"]["turns"].array.first(where: { $0["nativeTurnId"].string == nativeTurnID }) {
                switch turn["status"].string {
                case "completed": status = "completed"
                case "failed": status = "failed"
                case "cancelled", "interrupted": status = "interrupted"
                case "running", "cancelling", "queued":
                    let requests = snapshot["thread"]["pendingRequests"].array
                    status = requests.isEmpty ? "running" : "needs_input"
                default: break
                }
            } else if queueStatus == "dispatched" { status = "dispatched" }
        }
        if status != previous.status || error != previous.error {
            updatedAt = date; read = false
        }
    }
}
