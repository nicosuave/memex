import Foundation
import Darwin

/// Host-owned PTYs survive viewer disconnects. Only explicit close or host exit
/// ends a shell. Ring-buffer cursors make reconnects bounded and loss explicit.
final class RemoteWorkspaceTerminals {
    private final class Terminal: @unchecked Sendable {
        let id: String
        let root: String
        let process: Process
        let master: FileHandle
        let lock = NSLock()
        var output = Data()
        var start: Int64 = 0
        var ready = false
        var pendingInput = Data()
        var inputFailure: String?
        init(id: String, root: String, process: Process, master: FileHandle) {
            self.id = id; self.root = root; self.process = process; self.master = master
        }
        func append(_ data: Data) {
            lock.withLock {
                output.append(data)
                if !ready {
                    ready = true
                    if !pendingInput.isEmpty {
                        do { try master.write(contentsOf: pendingInput); pendingInput.removeAll() }
                        catch { inputFailure = "Terminal startup input could not be written: " + error.localizedDescription }
                    }
                }
                if output.count > 1024 * 1024 {
                    let excess = output.count - 1024 * 1024
                    output.removeFirst(excess); start += Int64(excess)
                }
            }
        }
        func send(_ data: Data) throws {
            try lock.withLock {
                if let inputFailure { throw HostFailure("terminal_io", inputFailure) }
                guard pendingInput.count + data.count <= 65536 else { throw HostFailure("terminal_limit", "Terminal startup input exceeds 64 KiB") }
                if ready { try master.write(contentsOf: data) }
                else { pendingInput.append(data) }
            }
        }
        func stop() {
            master.readabilityHandler = nil
            if process.isRunning {
                let pid = process.processIdentifier
                if pid > 1, getpgid(pid) == pid, pid != getpgrp() { kill(-pid, SIGHUP) }
                else { process.terminate() }
            }
            try? master.close()
        }
        deinit { stop() }
    }
    private var sessions: [String: Terminal] = [:]
    func hasRunningTerminal(root: URL) -> Bool { sessions.values.contains { $0.root == root.path && $0.process.isRunning } }

    func handle(_ request: HostRequest, root: URL) throws -> HostValue {
        let p = request.params
        guard let id = p["terminalId"]?.string, !id.isEmpty, id.utf8.count <= 128 else {
            throw HostFailure("invalid_params", "A terminal identity is required")
        }
        if request.method == "workspace.terminal.open", sessions[id] == nil {
            guard sessions.count < 8 else { throw HostFailure("terminal_limit", "Close an existing terminal before opening another") }
            var master: Int32 = -1, slave: Int32 = -1
            var size = winsize(ws_row: 24, ws_col: 100, ws_xpixel: 0, ws_ypixel: 0)
            guard openpty(&master, &slave, nil, nil, &size) == 0 else { throw HostFailure("terminal_io", "Cannot allocate host terminal") }
            // script supplies the shell's controlling PTY (job control and
            // Ctrl-C). Keep the outer transport raw so it forwards control bytes.
            var terminalAttributes = termios()
            if tcgetattr(slave, &terminalAttributes) == 0 {
                cfmakeraw(&terminalAttributes)
                _ = tcsetattr(slave, TCSANOW, &terminalAttributes)
            }
            _ = fcntl(master, F_SETFD, FD_CLOEXEC)
            let input = FileHandle(fileDescriptor: slave, closeOnDealloc: true)
            let output = FileHandle(fileDescriptor: master, closeOnDealloc: true)
            let child = Process()
            child.executableURL = URL(fileURLWithPath: "/usr/bin/script")
            child.arguments = ["-q", "/dev/null", "/bin/zsh", "-f", "-i"]
            child.currentDirectoryURL = root
            var environment = ProcessInfo.processInfo.environment
            environment["TERM"] = "dumb"
            child.environment = environment
            child.standardInput = input; child.standardOutput = input; child.standardError = input
            do { try child.run() } catch { try? input.close(); try? output.close(); throw error }
            try? input.close()
            let terminal = Terminal(id: id, root: root.path, process: child, master: output)
            output.readabilityHandler = { [weak terminal] handle in
                let bytes = handle.availableData
                if bytes.isEmpty { handle.readabilityHandler = nil }
                else { terminal?.append(bytes) }
            }
            sessions[id] = terminal
        }
        guard let terminal = sessions[id], terminal.root == root.path else {
            throw HostFailure("terminal_identity", "This terminal does not belong to the granted workspace, or its host restarted")
        }
        switch request.method {
        case "workspace.terminal.input":
            guard terminal.process.isRunning, let input = p["input"]?.string, input.utf8.count <= 65536 else {
                throw HostFailure("invalid_params", "Terminal input must be at most 64 KiB and the shell must be running")
            }
            try terminal.send(Data(input.utf8))
        case "workspace.terminal.resize":
            guard let rows = p["rows"]?.number, let cols = p["columns"]?.number,
                  rows.isFinite, cols.isFinite, (1...500).contains(rows), (1...500).contains(cols) else {
                throw HostFailure("invalid_params", "Terminal dimensions must be between 1 and 500")
            }
            var size = winsize(ws_row: UInt16(rows), ws_col: UInt16(cols), ws_xpixel: 0, ws_ypixel: 0)
            guard ioctl(terminal.master.fileDescriptor, TIOCSWINSZ, &size) == 0 else { throw HostFailure("terminal_io", "Cannot resize terminal") }
            if terminal.process.isRunning { kill(terminal.process.processIdentifier, SIGWINCH) }
        case "workspace.terminal.close":
            terminal.stop(); sessions.removeValue(forKey: id)
            return .object(["closed": .bool(true)])
        case "workspace.terminal.open", "workspace.terminal.read": break
        default: throw HostFailure("method_not_found", "Unknown terminal operation")
        }
        let cursor = p["cursor"]?.number ?? 0
        guard cursor.isFinite, cursor >= 0, cursor < Double(Int64.max) else { throw HostFailure("invalid_params", "Invalid terminal cursor") }
        return terminal.lock.withLock {
            let offset = min(max(Int64(cursor) - terminal.start, 0), Int64(terminal.output.count))
            let bytes = terminal.output.dropFirst(Int(offset)).prefix(65536)
            return .object(["terminalId": .string(id), "data": .string(Data(bytes).base64EncodedString()),
                "cursor": .number(Double(terminal.start + offset + Int64(bytes.count))),
                "truncated": .bool(Int64(cursor) < terminal.start), "running": .bool(terminal.process.isRunning),
                "inputError": terminal.inputFailure.map(HostValue.string) ?? .null])
        }
    }
}
