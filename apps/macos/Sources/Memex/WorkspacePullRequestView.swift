import SwiftUI

struct WorkspacePullRequestView: View {
    let directory: URL
    var initialURL: URL? = nil
    var addReviewContext: ((WorkspaceReviewContext) -> Bool)?
    @Environment(\.dismiss) private var dismiss
    @State private var selector = ""
    @State private var read: WorkspacePullRequestRead?
    @State private var busy = false
    @State private var error: String?
    @State private var comment = ""
    @State private var section = "Details"

    var body: some View {
        VStack(alignment: .leading, spacing: 10) {
            HStack {
                TextField("PR number or URL (blank uses current branch)", text: $selector)
                    .onSubmit { load() }
                Button("Load", action: load).disabled(busy)
                Button("Done") { dismiss() }
            }
            if busy { ProgressView("Reading pull request…") }
            if let error { Text(error).foregroundStyle(.orange).textSelection(.enabled) }
            if let read {
                let pr = read.pullRequest
                HStack {
                    Text("#\(pr.number) \(pr.title)").font(.headline)
                    Spacer()
                    Link("GitHub", destination: pr.url)
                }
                Text("\(pr.isDraft ? "Draft · " : "")\(pr.state) · \(pr.headRefName) → \(pr.baseRefName) · \(pr.headRefOid.prefix(8))")
                    .font(.caption).foregroundStyle(.secondary)
                Picker("Section", selection: $section) {
                    ForEach(["Details", "Diff", "Review threads"], id: \.self) { Text($0) }
                }.pickerStyle(.segmented)
                if section == "Diff" {
                    WorkspaceDiffText(text: read.patch, identity: pr.url.absoluteString + pr.headRefOid)
                } else {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 12) {
                            if section == "Details" {
                                Text(pr.body).textSelection(.enabled)
                                ForEach(pr.reviews) { review in
                                    Text("\(review.author?.login ?? "Deleted user") · \(review.state)").font(.headline)
                                    Text(review.body).textSelection(.enabled)
                                }
                                ForEach(pr.comments) { item in
                                    Text(item.author?.login ?? "Deleted user").font(.headline)
                                    Text(item.body).textSelection(.enabled)
                                }
                            } else {
                                if read.threads.isEmpty { Text("No inline review threads.") }
                                if read.hasMoreThreads { Text("Showing the first 100 threads. Open GitHub for the remaining threads.").foregroundStyle(.orange) }
                                ForEach(read.threads) { thread in
                                    Text("\(thread.path)\(thread.line.map { ":\($0)" } ?? "") · \(thread.isResolved ? "Resolved" : "Unresolved")\(thread.isOutdated ? " · Outdated" : "")").font(.headline)
                                    ForEach(thread.comments.nodes) { item in
                                        Link(item.author?.login ?? "Deleted user", destination: item.url)
                                        Text(item.body).textSelection(.enabled)
                                        if !item.diffHunk.isEmpty { Text(item.diffHunk).font(.system(.caption, design: .monospaced)).textSelection(.enabled) }
                                    }
                                    if thread.comments.pageInfo.hasNextPage { Text("More replies are available on GitHub.").foregroundStyle(.orange) }
                                    if let addReviewContext {
                                        Button("Add thread to chat") {
                                            let discussion = thread.comments.nodes.map { ($0.author?.login ?? "Deleted user") + ": " + $0.body }.joined(separator: "\n\n")
                                            let context = WorkspaceReviewContext(directory: directory, path: thread.path,
                                                scope: "PR #\(pr.number) · \(thread.path):\(thread.line.map(String.init) ?? "outdated") · \(thread.isResolved ? "resolved" : "unresolved")",
                                                patch: thread.comments.nodes.first?.diffHunk ?? "", comment: discussion,
                                                sourceURL: thread.comments.nodes.first?.url ?? pr.url)
                                            if !addReviewContext(context) { error = "Could not add thread context to this chat." }
                                        }.disabled(busy)
                                    }
                                    Divider()
                                }
                            }
                        }.frame(maxWidth: .infinity, alignment: .leading).padding(8)
                    }
                }
                if let addReviewContext {
                    HStack {
                        TextField("Review instructions for this chat", text: $comment)
                        Button("Add review to chat") {
                            let context = WorkspaceReviewContext(directory: directory, path: "", scope: "PR \(pr.url.absoluteString) at \(pr.headRefOid)", patch: read.patch, comment: comment, sourceURL: pr.url)
                            if addReviewContext(context) { comment = ""; error = nil }
                            else { error = "Could not add review context to this chat." }
                        }.disabled(busy)
                    }
                }
            } else { Spacer() }
        }.padding(16).frame(minWidth: 740, idealWidth: 900, minHeight: 540, idealHeight: 700)
        .task { selector = initialURL?.absoluteString ?? ""; load() }
    }

    private func load() {
        guard !busy else { return }
        busy = true
        error = nil
        read = nil
        Task { @MainActor in
            defer { busy = false }
            do { read = try await WorkspacePullRequestClient().read(directory: directory, selector: selector) }
            catch { self.error = error.localizedDescription }
        }
    }
}
