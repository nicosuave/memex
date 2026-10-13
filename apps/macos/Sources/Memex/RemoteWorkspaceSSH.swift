import Foundation
import SwiftUI
import MemexExecutionHostCore

struct ExecutionHostSSHConfiguration: Codable, Equatable, Sendable {
    var hostname: String
    var user: String
    var port: Int = 22
    var identityFile: String = ""
    /// Randomized per new configuration so a well-known port is not a squatting target.
    var localPort: Int = Int.random(in: 49152...65000)
    var remotePort: Int = 6363
    var endpoint: URL { URL(string: "http://127.0.0.1:\(localPort)")! }

    func arguments() throws -> [String] {
        guard !hostname.isEmpty, !hostname.hasPrefix("-"), !hostname.contains(where: { $0.isWhitespace || $0.isNewline }),
              !hostname.contains("\0"), !hostname.contains("@"), !user.hasPrefix("-"),
              !user.contains(where: { $0.isWhitespace || $0.isNewline }), !user.contains("\0"),
              (1...65535).contains(port), (1024...65535).contains(localPort), (1...65535).contains(remotePort),
              identityFile.isEmpty || (identityFile.hasPrefix("/") && !identityFile.contains("\0")) else {
            throw HostFailure("ssh_configuration", "Enter an exact SSH hostname, valid ports, and an absolute identity-file path")
        }
        // No connection sharing: the forward must be bound by this ssh process so
        // its ownership of the local listener can be verified.
        var result = ["-N", "-T", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
            "-o", "ControlMaster=no", "-o", "ControlPath=none",
            "-o", "ExitOnForwardFailure=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=3", "-p", String(port),
            "-L", "127.0.0.1:\(localPort):127.0.0.1:\(remotePort)"]
        if !user.isEmpty { result += ["-l", user] }
        if !identityFile.isEmpty { result += ["-i", identityFile] }
        return result + ["--", hostname]
    }
}

@MainActor
final class ExecutionHostSSHTunnels {
    static let shared = ExecutionHostSSHTunnels()
    private var processes: [Int: (ExecutionHostSSHConfiguration, Process)] = [:]
    @discardableResult func start(_ configuration: ExecutionHostSSHConfiguration) throws -> Bool {
        if let existing = processes[configuration.localPort], existing.1.isRunning {
            guard existing.0 == configuration else { throw HostFailure("ssh_configuration", "This local port belongs to another Memex tunnel") }
            return false
        }
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/ssh")
        process.arguments = try configuration.arguments()
        process.standardInput = FileHandle.nullDevice
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try process.run()
        processes[configuration.localPort] = (configuration, process)
        return true
    }
    func isRunning(_ configuration: ExecutionHostSSHConfiguration) -> Bool {
        guard let current = processes[configuration.localPort], current.0 == configuration else { return false }
        return current.1.isRunning
    }

    /// Waits (bounded) until the ssh process Memex spawned owns the listening
    /// socket on 127.0.0.1:localPort. Any other owner, including a process that
    /// bound the port first, means the credential is never sent.
    func requireOwnedListener(_ configuration: ExecutionHostSSHConfiguration, timeout: Duration = .seconds(10),
                              owns: (pid_t, Int) -> Bool = ProcessSockets.listensOnLoopback) async throws {
        let deadline = ContinuousClock.now + timeout
        while true {
            guard let current = processes[configuration.localPort], current.0 == configuration, current.1.isRunning else {
                throw HostFailure("ssh_connection", "SSH could not establish the tunnel. Check authentication, trusted host key, and ports with your SSH client.")
            }
            if owns(current.1.processIdentifier, configuration.localPort) { return }
            guard ContinuousClock.now < deadline else {
                throw HostFailure("ssh_tunnel", "Local port \(configuration.localPort) is not held by Memex's SSH tunnel. The execution credential was not sent.")
            }
            try await Task.sleep(for: .milliseconds(100))
        }
    }

    /// Restarts a stopped tunnel, then verifies listener ownership before each request.
    nonisolated static func transportCheck(for connection: ExecutionHostConnection) -> ExecutionTransportCheck? {
        guard let ssh = connection.ssh else { return nil }
        return {
            try await MainActor.run { _ = try ExecutionHostSSHTunnels.shared.start(ssh) }
            try await ExecutionHostSSHTunnels.shared.requireOwnedListener(ssh)
        }
    }
    func stop(_ configuration: ExecutionHostSSHConfiguration) {
        guard let current = processes[configuration.localPort], current.0 == configuration else { return }
        if current.1.isRunning { current.1.terminate() }
        processes.removeValue(forKey: configuration.localPort)
    }
    func stopAll() {
        for (_, process) in processes.values where process.isRunning { process.terminate() }
        processes.removeAll()
    }
}

struct ExecutionHostSSHOnboardingView: View {
    let onPaired: (ExecutionHostConnection) -> Void
    @State private var configuration = ExecutionHostSSHConfiguration(hostname: "", user: "")
    @State private var machineID = ""
    @State private var token = ""
    @State private var busy = false
    @State private var error: String?
    var body: some View {
        Section("SSH connection") {
            Text("Connect to an already configured execution host through SSH. Authenticate and trust its host key in your SSH client first; Memex never installs software or sends an agent prompt during pairing.")
                .font(.caption).foregroundStyle(.secondary)
            TextField("Exact SSH hostname", text: $configuration.hostname)
            TextField("SSH username (blank uses SSH config)", text: $configuration.user)
            TextField("SSH port", value: $configuration.port, format: .number)
            TextField("Identity file (optional absolute path)", text: $configuration.identityFile)
            TextField("Local tunnel port", value: $configuration.localPort, format: .number)
            TextField("Host Memex HTTP port", value: $configuration.remotePort, format: .number)
            TextField("Exact Memex machine identifier", text: $machineID)
            SecureField("Execution pairing token", text: $token)
            Button("Connect and pair \(configuration.hostname)") {
                busy = true; error = nil
                Task {
                    defer { busy = false }
                    do {
                        let host = try await ExecutionHostConnections.shared.pairSSH(configuration, machineID: machineID, token: token)
                        token = ""; onPaired(host)
                    } catch { self.error = error.localizedDescription }
                }
            }.disabled(busy || configuration.hostname.isEmpty || machineID.isEmpty || token.isEmpty)
            if let error { Text(error).foregroundStyle(.red).textSelection(.enabled) }
        }
    }
}
