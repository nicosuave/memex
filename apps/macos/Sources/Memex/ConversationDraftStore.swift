import CryptoKit
import Darwin
import Foundation
import Observation

/// Drafts belong to the same provider, machine and source-file identity as history.
/// Saving a draft never creates a runtime or schedules a send.
@MainActor @Observable
final class ConversationDraftStore {
    struct Draft: Codable, Equatable, Sendable {
        var text: String
        var deliveryUncertain = false
        var attachments: [ConversationAttachment] = []
        var pendingPrompt: ConversationPendingPrompt?
        var queue: [ConversationQueuedPrompt] = []
        var queueHeld = false

        init(text: String, deliveryUncertain: Bool = false, attachments: [ConversationAttachment] = [],
             pendingPrompt: ConversationPendingPrompt? = nil,
             queue: [ConversationQueuedPrompt] = [], queueHeld: Bool = false) {
            self.text = text
            self.deliveryUncertain = deliveryUncertain
            self.attachments = attachments
            self.pendingPrompt = pendingPrompt
            self.queue = queue
            self.queueHeld = queueHeld
        }

        private enum CodingKeys: String, CodingKey { case text, deliveryUncertain, attachments, pendingPrompt, queue, queueHeld }

        init(from decoder: Decoder) throws {
            let values = try decoder.container(keyedBy: CodingKeys.self)
            text = try values.decode(String.self, forKey: .text)
            deliveryUncertain = try values.decodeIfPresent(Bool.self, forKey: .deliveryUncertain) ?? false
            attachments = try values.decodeIfPresent([ConversationAttachment].self, forKey: .attachments) ?? []
            pendingPrompt = try values.decodeIfPresent(ConversationPendingPrompt.self, forKey: .pendingPrompt)
            queue = try values.decodeIfPresent([ConversationQueuedPrompt].self, forKey: .queue) ?? []
            queueHeld = try values.decodeIfPresent(Bool.self, forKey: .queueHeld) ?? false
        }

        /// A hold applies only to queued work, so it alone never keeps an entry.
        var normalized: Draft? {
            var draft = self
            if draft.queue.isEmpty { draft.queueHeld = false }
            let isEmpty = draft.text.isEmpty && draft.attachments.isEmpty && draft.pendingPrompt == nil
                && draft.queue.isEmpty && !draft.deliveryUncertain
            return isEmpty ? nil : draft
        }
    }

    private(set) var drafts: [String: Draft] = [:]
    private(set) var error: String?
    @ObservationIgnored private let writer: DraftFileWriter?
    @ObservationIgnored private let beforeWrite: (@Sendable () async -> Void)?
    @ObservationIgnored private let textDelay: Duration
    @ObservationIgnored private var canWrite = true
    @ObservationIgnored private var revision = 0
    @ObservationIgnored private var pendingWrite: Task<Void, Never>?
    @ObservationIgnored private var delayedWrite: Task<Void, Never>?
    /// Sessions this process changed since its last save. A save merges only
    /// these into the file, so another Memex process's drafts are not replaced.
    @ObservationIgnored private var changed: Set<String> = []

    /// A nil directory gives tests and previews an isolated, in-memory store.
    /// Typing is saved after `textDelay`; any other change is saved immediately,
    /// and `flush()` writes everything before durable work proceeds.
    init(directory: URL? = nil, textDelay: Duration = .milliseconds(500),
         beforeWrite: (@Sendable () async -> Void)? = nil) {
        writer = directory.map { DraftFileWriter(directory: $0) }
        self.textDelay = textDelay
        self.beforeWrite = beforeWrite
        guard let directory else { return }
        do {
            let saved = try DraftFileWriter.load(directory)
            for (id, draft) in saved.drafts {
                drafts[id] = draft.normalized
                // Version 1 stored attachment bytes inline; rewrite them by digest.
                if saved.version == 1 || drafts[id] != draft { changed.insert(id) }
            }
            if !changed.isEmpty { persist() }
        } catch let failure as CocoaError where failure.code == .fileReadNoSuchFile {
        } catch {
            // Do not overwrite unreadable saved drafts with a partial new catalog.
            canWrite = false
            self.error = "Saved drafts could not be read. New edits are kept in this window only."
        }
    }

    static func persistent() -> ConversationDraftStore {
        let directory = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent("Library/Application Support/dev.memex.app/Drafts", isDirectory: true)
        return ConversationDraftStore(directory: directory)
    }

    func set(_ draft: Draft, for sessionID: String) {
        let value = draft.normalized
        let previous = drafts[sessionID]
        guard previous != value else { return }
        drafts[sessionID] = value
        changed.insert(sessionID)
        if Self.differsOnlyInText(previous, value) { schedulePersist() } else { persist() }
    }

    func retrySave() async {
        persist()
        await flush()
    }

    /// Awaits every change made before the call, including delayed typing.
    func flush() async {
        while true {
            if delayedWrite != nil { persist() }
            guard let pendingWrite else { return }
            let revision = revision
            await pendingWrite.value
            if self.revision == revision && delayedWrite == nil { return }
        }
    }

    private static func differsOnlyInText(_ lhs: Draft?, _ rhs: Draft?) -> Bool {
        var lhs = lhs ?? Draft(text: ""), rhs = rhs ?? Draft(text: "")
        lhs.text = ""
        rhs.text = ""
        return lhs == rhs
    }

    private func schedulePersist() {
        guard delayedWrite == nil, canWrite else { return }
        let delay = textDelay
        delayedWrite = Task { [weak self] in
            do { try await Task.sleep(for: delay) } catch { return }
            self?.persist()
        }
    }

    private func persist() {
        delayedWrite?.cancel()
        delayedWrite = nil
        guard canWrite, writer != nil || beforeWrite != nil else { changed.removeAll(); return }
        revision += 1
        let revision = revision
        var entries: [String: Draft?] = [:]
        for id in changed { entries[id] = .some(drafts[id]) }
        changed.removeAll()
        let previous = pendingWrite
        let writer = writer
        let beforeWrite = beforeWrite
        // Saves are serialized in order; each merges its sessions under the file lock.
        pendingWrite = Task {
            await previous?.value
            await beforeWrite?()
            do {
                try await writer?.save(entries)
                if self.revision == revision { error = nil }
            } catch {
                // Keep these sessions dirty so the next save or retry includes them.
                changed.formUnion(entries.keys)
                if self.revision == revision {
                    self.error = "The draft could not be saved. Keep this window open until saving succeeds."
                }
            }
        }
    }
}

private struct SavedDrafts: Codable, Sendable {
    let version: Int
    let drafts: [String: ConversationDraftStore.Draft]
}

/// `drafts.json` keeps text and state. Attachment bytes of at least
/// `inlineLimit` are stored once, by SHA-256, beside it instead of as base64
/// inside every rewrite. Version 1 files stored all bytes inline.
private actor DraftFileWriter {
    static let version = 2
    static let inlineLimit = 1024
    static let digestPrefix = "sha256:"
    let directory: URL

    init(directory: URL) { self.directory = directory }

    nonisolated static func load(_ directory: URL) throws -> SavedDrafts {
        let data = try Data(contentsOf: directory.appendingPathComponent("drafts.json"))
        let decoder = JSONDecoder()
        let blobs = blobDirectory(directory)
        decoder.dataDecodingStrategy = .custom { decoder in
            let value = try decoder.singleValueContainer().decode(String.self)
            if value.hasPrefix(digestPrefix) {
                let digest = String(value.dropFirst(digestPrefix.count))
                let bytes = try Data(contentsOf: blobs.appendingPathComponent(digest))
                // Captured bytes are exact or the catalog is treated as unreadable.
                guard Self.digest(bytes) == digest else { throw CocoaError(.fileReadCorruptFile) }
                return bytes
            }
            guard let bytes = Data(base64Encoded: value) else { throw CocoaError(.fileReadCorruptFile) }
            return bytes
        }
        let saved = try decoder.decode(SavedDrafts.self, from: data)
        guard (1...version).contains(saved.version) else { throw CocoaError(.fileReadCorruptFile) }
        return saved
    }

    func save(_ entries: [String: ConversationDraftStore.Draft?]) throws {
        let manager = FileManager.default
        try manager.createDirectory(at: directory, withIntermediateDirectories: true,
                                    attributes: [.posixPermissions: 0o700])
        let descriptor = Darwin.open(directory.appendingPathComponent("drafts.lock").path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR)
        guard descriptor >= 0 else { throw POSIXError(.EIO) }
        defer { Darwin.close(descriptor) }
        guard flock(descriptor, LOCK_EX) == 0 else { throw POSIXError(.EIO) }
        defer { _ = flock(descriptor, LOCK_UN) }

        var stored = try readStored()
        let collected = BlobCollector()
        let encoder = JSONEncoder()
        encoder.dataEncodingStrategy = .custom { data, encoder in
            var container = encoder.singleValueContainer()
            guard data.count >= Self.inlineLimit else { return try container.encode(data.base64EncodedString()) }
            let digest = Self.digest(data)
            collected.add(digest, data)
            try container.encode(Self.digestPrefix + digest)
        }
        for (id, draft) in entries {
            if let draft { stored[id] = try JSONSerialization.jsonObject(with: encoder.encode(draft)) }
            else { stored.removeValue(forKey: id) }
        }
        let blobs = Self.blobDirectory(directory)
        let pending = collected.blobs
        if !pending.isEmpty {
            try manager.createDirectory(at: blobs, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        }
        for (digest, bytes) in pending {
            let file = blobs.appendingPathComponent(digest)
            guard !manager.fileExists(atPath: file.path) else { continue }
            try bytes.write(to: file, options: .atomic)
            try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
        }
        let file = directory.appendingPathComponent("drafts.json")
        let data = try JSONSerialization.data(withJSONObject: ["version": Self.version, "drafts": stored],
                                              options: [.sortedKeys])
        try data.write(to: file, options: .atomic)
        try manager.setAttributes([.posixPermissions: 0o600], ofItemAtPath: file.path)
        // Remove bytes no saved draft references, including after migration.
        var referenced: Set<String> = []
        Self.collectDigests(stored, into: &referenced)
        for name in (try? manager.contentsOfDirectory(atPath: blobs.path)) ?? [] where !referenced.contains(name) {
            try? manager.removeItem(at: blobs.appendingPathComponent(name))
        }
    }

    /// The saved catalog as JSON, so sessions owned by another process are
    /// carried over without decoding their attachment bytes.
    private func readStored() throws -> [String: Any] {
        let data: Data
        do { data = try Data(contentsOf: directory.appendingPathComponent("drafts.json")) }
        catch let failure as CocoaError where failure.code == .fileReadNoSuchFile { return [:] }
        guard let object = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let version = object["version"] as? Int, (1...Self.version).contains(version),
              let drafts = object["drafts"] as? [String: Any] else {
            // Never replace a catalog this version cannot read.
            throw CocoaError(.fileReadCorruptFile)
        }
        return drafts
    }

    private static func collectDigests(_ value: Any, into digests: inout Set<String>) {
        if let string = value as? String, string.hasPrefix(digestPrefix) {
            digests.insert(String(string.dropFirst(digestPrefix.count)))
        } else if let object = value as? [String: Any] {
            for item in object.values { collectDigests(item, into: &digests) }
        } else if let array = value as? [Any] {
            for item in array { collectDigests(item, into: &digests) }
        }
    }

    nonisolated static func blobDirectory(_ directory: URL) -> URL {
        directory.appendingPathComponent("Attachments", isDirectory: true)
    }

    nonisolated static func digest(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }
}

private final class BlobCollector: @unchecked Sendable {
    private let lock = NSLock()
    private var collected: [String: Data] = [:]
    var blobs: [String: Data] { lock.withLock { collected } }
    func add(_ digest: String, _ data: Data) { lock.withLock { collected[digest] = data } }
}
