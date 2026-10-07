import AppKit
import AVFoundation
import Observation
import Speech
import SwiftUI

/// Audio stays local. Recognition results are reviewed before insertion, and
/// insertion never submits a prompt. Failed recordings remain available to retry.
@MainActor @Observable
final class ConversationDictation {
    enum Phase: Equatable { case idle, authorizing, recording, transcribing }
    private(set) var phase: Phase = .idle
    var transcript = ""
    private(set) var error: String?
    private(set) var recordingURL: URL?
    @ObservationIgnored private var recorder: AVAudioRecorder?
    @ObservationIgnored private var recognition: SFSpeechRecognitionTask?
    @ObservationIgnored private var recordingLimit: Task<Void, Never>?
    @ObservationIgnored private var recognitionLimit: Task<Void, Never>?
    @ObservationIgnored private var generation = UUID()
    private let directory: URL

    init(directory: URL = FileManager.default.homeDirectoryForCurrentUser
        .appendingPathComponent("Library/Application Support/dev.memex.app/Dictation")) {
        self.directory = directory
        recordingURL = (try? FileManager.default.contentsOfDirectory(at: directory,
            includingPropertiesForKeys: nil))?.filter { $0.pathExtension == "m4a" }
            .sorted { $0.lastPathComponent > $1.lastPathComponent }.first
    }

    func start() async {
        guard phase == .idle else { return }
        guard Bundle.main.object(forInfoDictionaryKey: "NSMicrophoneUsageDescription") != nil,
              Bundle.main.object(forInfoDictionaryKey: "NSSpeechRecognitionUsageDescription") != nil else {
            error = "Dictation requires the installed Memex app with microphone and speech permission descriptions."
            return
        }
        let token = UUID()
        generation = token
        phase = .authorizing
        defer { if phase == .authorizing { phase = .idle } }
        let microphone = await AVCaptureDevice.requestAccess(for: .audio)
        let speech = await withCheckedContinuation { continuation in
            SFSpeechRecognizer.requestAuthorization { continuation.resume(returning: $0 == .authorized) }
        }
        guard generation == token else { return }
        guard microphone && speech else {
            error = "Allow Memex microphone and speech recognition access in System Settings → Privacy & Security, then try again."
            return
        }
        guard let recognizer = SFSpeechRecognizer(), recognizer.isAvailable, recognizer.supportsOnDeviceRecognition else {
            error = "On-device speech recognition is unavailable for this language. Enable/download its dictation support in System Settings and retry."
            return
        }
        do {
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true,
                attributes: [.posixPermissions: 0o700])
            let url = directory.appendingPathComponent("\(Int(Date().timeIntervalSince1970 * 1000))-\(UUID().uuidString).m4a")
            let recorder = try AVAudioRecorder(url: url, settings: [AVFormatIDKey: kAudioFormatMPEG4AAC,
                AVSampleRateKey: 44100, AVNumberOfChannelsKey: 1, AVEncoderAudioQualityKey: AVAudioQuality.high.rawValue])
            guard recorder.record(forDuration: 120) else { throw ConversationRuntimeError(message: "The microphone could not start recording.") }
            try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: url.path)
            self.recorder = recorder
            recordingURL = url
            transcript = ""
            error = nil
            phase = .recording
            recordingLimit = Task { [weak self] in
                do { try await Task.sleep(for: .seconds(120)) } catch { return }
                self?.finishRecording()
            }
        } catch { self.error = error.localizedDescription }
    }

    func finishRecording() {
        guard phase == .recording else { return }
        recorder?.stop()
        recorder = nil
        recordingLimit?.cancel()
        recordingLimit = nil
        phase = .idle
        transcribe()
    }

    func transcribe() {
        guard phase == .idle, let recordingURL else { return }
        guard let recognizer = SFSpeechRecognizer(), recognizer.isAvailable,
              recognizer.supportsOnDeviceRecognition,
              SFSpeechRecognizer.authorizationStatus() == .authorized else {
            error = "On-device speech recognition is unavailable or permission is denied. Your recording is retained; enable access in System Settings and retry."
            return
        }
        let request = SFSpeechURLRecognitionRequest(url: recordingURL)
        request.requiresOnDeviceRecognition = true
        request.shouldReportPartialResults = true
        let token = UUID()
        generation = token
        phase = .transcribing
        error = nil
        recognition = recognizer.recognitionTask(with: request) { [weak self] result, failure in
            let text = result?.bestTranscription.formattedString
            let finished = result?.isFinal == true
            let message = failure?.localizedDescription
            Task { @MainActor in
                guard let self, self.generation == token else { return }
                if let text { self.transcript = text }
                if finished || message != nil {
                    self.phase = .idle
                    self.error = message.map { "Transcription failed: \($0). Your recording is retained; retry or export it." }
                    self.recognition = nil
                    self.recognitionLimit?.cancel()
                    self.recognitionLimit = nil
                }
            }
        }
        recognitionLimit = Task { [weak self] in
            do { try await Task.sleep(for: .seconds(90)) } catch { return }
            guard let self, self.generation == token, self.phase == .transcribing else { return }
            self.cancel()
            self.error = "Transcription timed out. Your recording is retained; retry or export it."
        }
    }

    func cancel() {
        generation = UUID()
        recorder?.stop()
        recorder = nil
        recognition?.cancel()
        recognition = nil
        recordingLimit?.cancel()
        recognitionLimit?.cancel()
        phase = .idle
    }

    func export() async {
        guard let recordingURL else { return }
        let panel = NSSavePanel()
        panel.nameFieldStringValue = recordingURL.lastPathComponent
        guard await panel.begin() == .OK, let url = panel.url else { return }
        do { try Data(contentsOf: recordingURL).write(to: url, options: .atomic) }
        catch { self.error = "Could not export recording: \(error.localizedDescription)" }
    }

    func deleteRecording() {
        guard phase == .idle, let recordingURL else { return }
        do {
            try FileManager.default.trashItem(at: recordingURL, resultingItemURL: nil)
            self.recordingURL = nil
            transcript = ""
            error = nil
        } catch { self.error = error.localizedDescription }
    }
}

struct ConversationDictationView: View {
    let insert: (String) -> Void
    @State private var dictation = ConversationDictation()
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Text("Dictate a prompt").font(.title2)
            Text("Record up to two minutes. Speech is transcribed on this Mac. Review the text before adding it to your draft.").font(.callout)
            if dictation.phase == .authorizing { ProgressView("Requesting microphone and speech access…") }
            if dictation.phase == .recording { Label("Recording", systemImage: "mic.fill").foregroundStyle(.red) }
            if dictation.phase == .transcribing { ProgressView("Transcribing recording…") }
            TextEditor(text: $dictation.transcript).frame(minHeight: 140)
            if let error = dictation.error { Text(error).font(.caption).foregroundStyle(.orange).textSelection(.enabled) }
            if let url = dictation.recordingURL {
                Text("Recording retained locally: \(url.lastPathComponent)").font(.caption).foregroundStyle(.secondary)
            }
            HStack {
                if dictation.phase == .recording {
                    Button("Stop recording") { dictation.finishRecording() }
                } else {
                    Button("Record") { Task { await dictation.start() } }.disabled(dictation.phase != .idle)
                }
                Menu("Recording") {
                    Button("Retry transcription") { dictation.transcribe() }
                    Button("Export recording…") { Task { await dictation.export() } }
                    Button("Delete recording", role: .destructive) { dictation.deleteRecording() }
                }.disabled(dictation.recordingURL == nil || dictation.phase != .idle)
                Spacer()
                Button("Close") { dictation.cancel(); dismiss() }
                Button("Insert into draft") { insert(dictation.transcript); dismiss() }
                    .disabled(dictation.phase != .idle || dictation.transcript.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }
        }.padding(20).frame(width: 580)
        .onDisappear { dictation.cancel() }
    }
}
