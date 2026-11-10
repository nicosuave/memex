import Darwin
import Foundation
import Testing
@testable import Memex

#if canImport(SQACPHost)
import SQACPHost
#endif

@Test func codexOwnershipChecksHeldLocksWithoutCreatingOrChangingFiles() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    let id = UUID().uuidString.lowercased()
    let session = Session(source: "codex", sessionID: id,
        sourcePath: root.appendingPathComponent("sessions/2026/10/04/session.jsonl").path,
        project: "test", cwd: root.path, machine: "local")
    let locks = root.appendingPathComponent("thread-writer-locks")
    #expect(try !ConversationOwnership.isOpenElsewhere(session))
    #expect(!FileManager.default.fileExists(atPath: locks.path))
    try FileManager.default.createDirectory(at: locks, withIntermediateDirectories: true)
    let lock = locks.appendingPathComponent(id + ".lock")
    let content = Data("unchanged".utf8)
    try content.write(to: lock)
    #expect(try !ConversationOwnership.isOpenElsewhere(session))
    let descriptor = open(lock.path, O_RDWR | O_CLOEXEC)
    #expect(descriptor >= 0)
    defer { close(descriptor) }
    #expect(flock(descriptor, LOCK_EX | LOCK_NB) == 0)
    #expect(try ConversationOwnership.isOpenElsewhere(session))
    #expect(try Data(contentsOf: lock) == content)
    #expect(flock(descriptor, LOCK_UN) == 0)
    #expect(try !ConversationOwnership.isOpenElsewhere(session))
    #expect(try Data(contentsOf: lock) == content)
}

@Test func codexOwnershipUsesOwningHomeForSymlinkedSessionStorage() throws {
    let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: root) }
    let home = root.appendingPathComponent("alternate-home")
    let storage = root.appendingPathComponent("storage")
    let locks = home.appendingPathComponent("thread-writer-locks")
    try FileManager.default.createDirectory(at: locks, withIntermediateDirectories: true)
    try FileManager.default.createDirectory(at: storage, withIntermediateDirectories: true)
    try FileManager.default.createSymbolicLink(at: home.appendingPathComponent("sessions"), withDestinationURL: storage)
    let id = UUID().uuidString.lowercased()
    let lock = locks.appendingPathComponent(id + ".lock")
    try Data().write(to: lock)
    let descriptor = open(lock.path, O_RDWR | O_CLOEXEC)
    #expect(descriptor >= 0)
    defer { close(descriptor) }
    #expect(flock(descriptor, LOCK_EX | LOCK_NB) == 0)
    var session = Session(source: "codex", sessionID: id,
        sourcePath: home.appendingPathComponent("sessions/session.jsonl").path,
        project: "test", cwd: root.path, machine: "local")
    #expect(try ConversationOwnership.isOpenElsewhere(session))
    session.machine = "nicbook-atm"
    #expect(try !ConversationOwnership.isOpenElsewhere(session))
    let claude = Session(source: "claude", sessionID: id, sourcePath: session.sourcePath,
        project: "test", cwd: root.path, machine: "local")
    #expect(try !ConversationOwnership.isOpenElsewhere(claude))
}

#if canImport(SQACPHost)
@Test func nativeWriterConflictIsRecognizedWithoutMaskingOtherProviderErrors() {
    let conflict = conversationProviderError(AgentConversationServiceError.runtime(
        "Codex App Server: thread 01a1079b-aea8-7231-b7aa-62dab17e6ddd already has an active writer"))
    #expect(conflict.kind == .openElsewhere)
    let other = conversationProviderError(AgentConversationServiceError.runtime("Authentication failed"))
    #expect(other.kind == .other)
    #expect(other.message == "Authentication failed")
}
#endif
