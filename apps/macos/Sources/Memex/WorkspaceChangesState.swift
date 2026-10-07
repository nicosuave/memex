import Foundation

/// Presentation state keeps successful reads visible while newer Git reads are pending.
/// Request identities also reject completions from a previous workspace or selection.
struct WorkspaceChangesState {
    private(set) var directory: URL?
    private(set) var snapshot: WorkspaceChangesSnapshot?
    private(set) var selectedPath: String?
    private(set) var loading = true
    private(set) var error: String?
    private(set) var loadingPatch = false
    private(set) var patchError: String?
    private(set) var patchRevision = UUID()
    private var patches: [String: String] = [:]
    private var refreshRequest: UUID?
    private var patchRequest: UUID?

    var patch: String? { selectedPath.flatMap { patches[$0] } }

    mutating func beginRefresh(directory: URL, initialSelectedPath: String?) -> UUID {
        if self.directory != directory {
            self = Self()
            self.directory = directory
            selectedPath = initialSelectedPath
        }
        loading = true
        error = nil
        let request = UUID()
        refreshRequest = request
        return request
    }

    mutating func finishRefresh(_ next: WorkspaceChangesSnapshot?, request: UUID) {
        guard refreshRequest == request else { return }
        snapshot = next
        loading = false
        refreshRequest = nil
        let paths = Set(next?.files.map(\.path) ?? [])
        patches = patches.filter { paths.contains($0.key) }
        if !paths.contains(selectedPath ?? "") { select(next?.files.first?.path) }
        patchRequest = nil
        loadingPatch = false
        patchError = nil
        patchRevision = UUID()
    }

    mutating func failRefresh(_ message: String, request: UUID) {
        guard refreshRequest == request else { return }
        loading = false
        error = message
        refreshRequest = nil
    }

    mutating func select(_ path: String?) {
        guard selectedPath != path else { return }
        selectedPath = path
        patchRequest = nil
        loadingPatch = false
        patchError = nil
        patchRevision = UUID()
    }

    mutating func beginPatch() -> (request: UUID, root: URL, file: WorkspaceChange)? {
        guard let snapshot, let file = snapshot.files.first(where: { $0.path == selectedPath }) else { return nil }
        let request = UUID()
        patchRequest = request
        loadingPatch = true
        patchError = nil
        return (request, snapshot.root, file)
    }

    mutating func finishPatch(_ text: String, request: UUID) {
        guard patchRequest == request, let selectedPath else { return }
        patches[selectedPath] = text
        loadingPatch = false
        patchRequest = nil
    }

    mutating func failPatch(_ message: String, request: UUID) {
        guard patchRequest == request else { return }
        patchError = message
        loadingPatch = false
        patchRequest = nil
    }

    mutating func retryPatch() { patchRevision = UUID() }
}
