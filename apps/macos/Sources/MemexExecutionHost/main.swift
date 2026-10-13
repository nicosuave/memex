import Foundation
import MemexExecutionHostCore

do {
    var root = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".memex")
    var workspaces: [URL] = []
    var arguments = CommandLine.arguments.dropFirst().makeIterator()
    while let argument = arguments.next() {
        if argument == "--help" {
            print("MemexExecutionHost [--root PATH] --workspace PATH [--workspace PATH ...]\nRuns supervised providers independently of viewing clients. Only explicitly registered workspaces are executable. Pair remote clients using state/execution/control-token through an authenticated TLS proxy or SSH tunnel to memex web.")
            exit(0)
        }
        guard ["--root", "--workspace"].contains(argument), let value = arguments.next(), value.hasPrefix("/") else {
            throw HostFailure("arguments", "Expected --root or --workspace followed by an absolute path")
        }
        if argument == "--root" { root = URL(fileURLWithPath: value) }
        else { workspaces.append(URL(fileURLWithPath: value)) }
    }
    let directory = root.appendingPathComponent("state/execution")
    // Acquire process ownership before opening the runtime or recovering the catalog.
    let server = try HostSocketServer(directory: directory)
    #if canImport(SQACPHost)
    let host = try ExecutionHost(directory: directory, workspaceRoots: workspaces) { hostID in
        try NativeExecutionProvider(directory: directory, hostID: hostID)
    }
    FileHandle.standardError.write(Data("Execution host \(host.hostID) ready at \(directory.appendingPathComponent("control.sock").path)\n".utf8))
    try server.run(host: host)
    #else
    _ = server
    throw HostFailure("runtime_unavailable", "This build does not include SQACPHost. Build with MEMEX_AGENT_RUNTIME_ROOT and its matching runtime archive.")
    #endif
} catch {
    FileHandle.standardError.write(Data("\(error.localizedDescription)\n".utf8))
    exit(1)
}
