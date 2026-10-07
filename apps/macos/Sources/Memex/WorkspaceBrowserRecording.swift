import AppKit
import AVFoundation
import Foundation
import WebKit

struct WorkspaceBrowserRecordingArtifact: Codable, Sendable {
    let path: String
    let mimeType: String
    let durationSeconds: Double
    let frameCount: Int
    let byteCount: Int
    let ownership: String
}

/// Records only this WKWebView's viewport. No display capture or OS recording
/// permission is involved. Partial output is removed on cancellation or failure.
@MainActor final class WorkspaceBrowserRecording {
    private var cancelled = false
    private var writer: AVAssetWriter?
    private var output: URL?
    func cancel() { cancelled = true; writer?.cancelWriting(); cleanup() }

    func record(session: WorkspaceBrowserSession, duration: Int, fps: Int,
                authorized: @escaping @MainActor () -> Bool) async throws -> WorkspaceBrowserRecordingArtifact {
        guard (1...5).contains(duration), (1...5).contains(fps) else { throw failure("Recording duration and frame rate must each be between 1 and 5.") }
        let directory = FileManager.default.temporaryDirectory.appendingPathComponent("memex-browser-recordings", isDirectory: true)
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        let url = directory.appendingPathComponent(UUID().uuidString + ".mp4")
        output = url
        var completed = false
        defer { if !completed { writer?.cancelWriting(); cleanup() } }
        try check(authorized)
        let first = try await frame(session)
        try check(authorized)
        let width = max(2, min(1280, first.width) / 2 * 2)
        let height = max(2, Int(Double(first.height) * Double(width) / Double(first.width)) / 2 * 2)
        guard height <= 2048 else { throw failure("Viewport is too tall for recording. Resize it before recording.") }
        let writer = try AVAssetWriter(outputURL: url, fileType: .mp4)
        self.writer = writer
        let input = AVAssetWriterInput(mediaType: .video, outputSettings: [AVVideoCodecKey: AVVideoCodecType.h264,
            AVVideoWidthKey: width, AVVideoHeightKey: height,
            AVVideoCompressionPropertiesKey: [AVVideoAverageBitRateKey: 1_500_000]])
        input.expectsMediaDataInRealTime = true
        let adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: input, sourcePixelBufferAttributes: [
            kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32ARGB,
            kCVPixelBufferWidthKey as String: width, kCVPixelBufferHeightKey as String: height,
            kCVPixelBufferCGImageCompatibilityKey as String: true, kCVPixelBufferCGBitmapContextCompatibilityKey as String: true])
        guard writer.canAdd(input) else { throw failure("H.264 recording is unavailable.") }
        writer.add(input)
        guard writer.startWriting() else { throw writer.error ?? failure("Could not start recording.") }
        writer.startSession(atSourceTime: .zero)
        let started = Date()
        let count = duration * fps
        for index in 0..<count {
            try check(authorized)
            let delay = Double(index) / Double(fps) - Date().timeIntervalSince(started)
            if delay > 0 { try await Task.sleep(for: .seconds(delay)) }
            let image = index == 0 ? first : try await frame(session)
            try check(authorized)
            guard Date().timeIntervalSince(started) < 10 else { throw failure("Recording capture timed out.") }
            let readyDeadline = Date().addingTimeInterval(1)
            while !input.isReadyForMoreMediaData {
                try check(authorized)
                guard Date() < readyDeadline, writer.status == .writing else { throw writer.error ?? failure("Recording encoder stalled.") }
                try await Task.sleep(for: .milliseconds(10))
            }
            var buffer: CVPixelBuffer?
            guard let pool = adaptor.pixelBufferPool,
                  CVPixelBufferPoolCreatePixelBuffer(nil, pool, &buffer) == kCVReturnSuccess, let buffer else { throw failure("Could not allocate a recording frame.") }
            CVPixelBufferLockBaseAddress(buffer, [])
            let context = CGContext(data: CVPixelBufferGetBaseAddress(buffer), width: width, height: height, bitsPerComponent: 8,
                bytesPerRow: CVPixelBufferGetBytesPerRow(buffer), space: CGColorSpaceCreateDeviceRGB(), bitmapInfo: CGImageAlphaInfo.noneSkipFirst.rawValue)
            context?.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))
            CVPixelBufferUnlockBaseAddress(buffer, [])
            guard context != nil, adaptor.append(buffer, withPresentationTime: CMTime(value: Int64(index), timescale: Int32(fps))) else {
                throw writer.error ?? failure("Could not encode the recording frame.")
            }
            let bytes = (try? url.resourceValues(forKeys: [.fileSizeKey]).fileSize) ?? 0
            guard bytes <= 8 * 1024 * 1024 else { throw failure("Recording exceeded the 8 MiB limit.") }
        }
        writer.endSession(atSourceTime: CMTime(value: Int64(duration), timescale: 1))
        input.markAsFinished()
        let finishTimeout = Task { @MainActor in
            do { try await Task.sleep(for: .seconds(2)) } catch { return }
            if writer.status == .writing { writer.cancelWriting() }
        }
        await writer.finishWriting()
        finishTimeout.cancel()
        try check(authorized)
        guard writer.status == .completed else { throw writer.error ?? failure("Could not finish the recording.") }
        let bytes = try url.resourceValues(forKeys: [.fileSizeKey]).fileSize ?? 0
        guard bytes > 0, bytes <= 8 * 1024 * 1024 else { throw failure("Recording is empty or exceeds 8 MiB.") }
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
        completed = true
        return .init(path: url.path, mimeType: "video/mp4", durationSeconds: Double(duration), frameCount: count,
                     byteCount: bytes, ownership: "Temporary artifact on the Memex desktop host; not uploaded or copied to a remote execution host.")
    }

    private func check(_ authorized: () -> Bool) throws {
        try Task.checkCancellation()
        guard !cancelled, authorized() else { throw failure("Recording stopped because browser access was revoked, the tab closed, or recording was cancelled.") }
    }
    private func frame(_ session: WorkspaceBrowserSession) async throws -> CGImage {
        guard session.webView.bounds.width > 0, session.webView.bounds.height > 0 else { throw failure("Show the browser viewport before recording.") }
        let configuration = WKSnapshotConfiguration()
        configuration.snapshotWidth = NSNumber(value: Double(min(1280, session.webView.bounds.width)))
        return try await withCheckedThrowingContinuation { continuation in
            let reply = WorkspaceRecordingFrameReply(continuation)
            session.webView.takeSnapshot(with: configuration) { image, error in
                if let error { reply.finish(.failure(error)); return }
                guard let cgImage = image?.cgImage(forProposedRect: nil, context: nil, hints: nil) else {
                    reply.finish(.failure(WorkspaceBrowserAutomationError(message: "WebKit did not return a recording frame."))); return
                }
                reply.finish(.success(cgImage))
            }
        }
    }
    private func cleanup() { if let output { try? FileManager.default.removeItem(at: output) }; output = nil }
    private func failure(_ message: String) -> WorkspaceBrowserAutomationError { .init(message: message) }
}

@MainActor private final class WorkspaceRecordingFrameReply {
    private var continuation: CheckedContinuation<CGImage, any Error>?
    private var timeout: Task<Void, Never>?
    init(_ continuation: CheckedContinuation<CGImage, any Error>) {
        self.continuation = continuation
        timeout = Task { [weak self] in
            do { try await Task.sleep(for: .seconds(2)) } catch { return }
            self?.finish(.failure(WorkspaceBrowserAutomationError(message: "WebKit recording frame timed out.")))
        }
    }
    func finish(_ result: Result<CGImage, any Error>) {
        guard let continuation else { return }
        self.continuation = nil; timeout?.cancel(); timeout = nil
        continuation.resume(with: result)
    }
}
