import AppKit
import AVFoundation
import Testing
import WebKit
@testable import Memex

@MainActor @Suite(.serialized) struct WorkspaceBrowserRecordingTests {
    init() { _ = NSApplication.shared }

    private func page() async throws -> (WorkspaceBrowserSession, NSWindow) {
        let session = WorkspaceBrowserSession()
        let window = NSWindow(contentRect: NSRect(x: 0, y: 0, width: 320, height: 240), styleMask: [.titled], backing: .buffered, defer: false)
        window.isReleasedWhenClosed = false
        window.contentView = session.webView
        window.orderFront(nil)
        session.webView.loadHTMLString("<html><head><title>Recording fixture</title></head><body style='background:#3388aa'>Real WebKit recording<script>setInterval(()=>document.body.style.background=document.body.style.background==='red'?'blue':'red',100)</script></body></html>", baseURL: URL(string: "http://localhost/"))
        let deadline = Date().addingTimeInterval(10)
        while session.webView.title != "Recording fixture" || session.webView.isLoading {
            guard Date() < deadline else { window.close(); throw WorkspaceBrowserAutomationError(message: "Fixture page timed out") }
            try await Task.sleep(for: .milliseconds(20))
        }
        return (session, window)
    }

    @Test func realWebKitFramesProducePlayableH264Video() async throws {
        let (session, window) = try await page()
        defer { window.close() }
        let artifact = try await WorkspaceBrowserRecording().record(session: session, duration: 1, fps: 3, authorized: { true })
        let url = URL(fileURLWithPath: artifact.path)
        defer { try? FileManager.default.removeItem(at: url) }
        #expect(artifact.mimeType == "video/mp4")
        #expect(artifact.frameCount == 3)
        #expect(artifact.byteCount > 0 && artifact.byteCount <= 8 * 1024 * 1024)
        let asset = AVURLAsset(url: url)
        #expect(try await asset.load(.isPlayable))
        #expect(try await asset.load(.duration).seconds >= 0.9)
        let tracks = try await asset.loadTracks(withMediaType: .video)
        #expect(tracks.count == 1)
        let generator = AVAssetImageGenerator(asset: asset)
        let decoded = try await generator.image(at: CMTime(value: 1, timescale: 3))
        // WebKit snapshots use backing pixels; a Retina window yields twice
        // the point width before the recorder applies its pixel limit.
        let expectedWidth = Int(min(1280, session.webView.bounds.width * window.backingScaleFactor)) / 2 * 2
        #expect(decoded.image.width == expectedWidth)
    }

    @Test func revocationAndExplicitStopRemovePartialOutput() async throws {
        let (session, window) = try await page()
        defer { window.close() }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("memex-browser-recordings")
        let before = Set((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? [])
        var authorized = true
        let recorder = WorkspaceBrowserRecording()
        let recording = Task { try await recorder.record(session: session, duration: 3, fps: 3, authorized: { authorized }) }
        try await Task.sleep(for: .milliseconds(400))
        authorized = false
        await #expect(throws: (any Error).self) { try await recording.value }
        #expect(Set((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? []) == before)
        let stopped = WorkspaceBrowserRecording()
        let second = Task { try await stopped.record(session: session, duration: 3, fps: 3, authorized: { true }) }
        try await Task.sleep(for: .milliseconds(400))
        stopped.cancel()
        await #expect(throws: (any Error).self) { try await second.value }
        #expect(Set((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? []) == before)
    }

    @Test func taskCancellationCleansPartialRecording() async throws {
        let (session, window) = try await page()
        defer { window.close() }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("memex-browser-recordings")
        let before = Set((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? [])
        let recorder = WorkspaceBrowserRecording()
        let recording = Task { try await recorder.record(session: session, duration: 5, fps: 5, authorized: { true }) }
        try await Task.sleep(for: .milliseconds(400))
        recording.cancel()
        await #expect(throws: (any Error).self) { try await recording.value }
        #expect(Set((try? FileManager.default.contentsOfDirectory(atPath: directory.path)) ?? []) == before)
    }

    @Test func invalidBoundsAndMissingViewportFailWithoutArtifacts() async throws {
        let session = WorkspaceBrowserSession()
        await #expect(throws: (any Error).self) {
            try await WorkspaceBrowserRecording().record(session: session, duration: 6, fps: 3, authorized: { true })
        }
        await #expect(throws: (any Error).self) {
            try await WorkspaceBrowserRecording().record(session: session, duration: 1, fps: 6, authorized: { true })
        }
        await #expect(throws: (any Error).self) {
            try await WorkspaceBrowserRecording().record(session: session, duration: 1, fps: 3, authorized: { false })
        }
    }

    @Test func ordinaryBrowserGrantCannotRecord() async throws {
        let suite = "recording-grants-" + UUID().uuidString
        let defaults = try #require(UserDefaults(suiteName: suite))
        defer { defaults.removePersistentDomain(forName: suite) }
        let store = WorkspaceBrowserStore(defaults: defaults)
        let tabs = store.tabs(for: "recording-chat")
        let grant = try store.automation.allow(conversationID: "recording-chat", capabilities: [.snapshot])
        let request = WorkspaceBrowserAutomationRequest(hostID: grant.hostID, grantID: grant.id,
            conversationID: "recording-chat", tabID: tabs.selected.id, action: .record)
        await #expect(throws: (any Error).self) { try await store.automation.dispatch(request) }
    }
}
