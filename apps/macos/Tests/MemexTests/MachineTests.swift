import Foundation
import Testing
@testable import Memex

@Test func machineIdentitySeparatesOtherwiseIdenticalSessions() throws {
    let json = #"{"source":"codex","session_id":"same","source_path":"/same","project":"memex"}"#
    let local = try JSONDecoder().decode(Session.self, from: Data(json.utf8))
    var remote = local
    remote.machine = "fixture-remote"
    #expect(local.machineID == "local")
    #expect(local.id != remote.id)
    var explicitLocal = local
    explicitLocal.machine = "local"
    #expect(local.id == explicitLocal.id)
}

@Test func searchRetainsMachineAndOnlyUsesMetadataFromThatMachine() throws {
    let json = #"{"source":"codex","session_id":"same","source_path":"/same","project":"memex","snippet":"match","machine":"fixture-remote"}"#
    let hit = try JSONDecoder().decode(SearchHit.self, from: Data(json.utf8))
    let local = Session(source: "codex", sessionID: "same", sourcePath: "/same", project: "memex", label: "Local title")
    let unmatched = hit.session(known: [local.id: local])
    #expect(unmatched.machineID == "fixture-remote")
    #expect(unmatched.title == "match")
    var remote = local
    remote.machine = "fixture-remote"
    remote.label = "Remote title"
    let matched = hit.session(known: [local.id: local, remote.id: remote])
    #expect(matched.machineID == "fixture-remote")
    #expect(matched.title == "Remote title")
    #expect(matched.snippet == "match")
}

@Test func clientDiscoversMachinesAndRoutesEveryRequest() async throws {
    let directory = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let executable = directory.appendingPathComponent("fixture-cli")
    let script = #"""
    #!/bin/sh
    shift 2
    command="$1"
    shift
    machine=""
    fields=""
    while [ "$#" -gt 0 ]; do
      case "$1" in
        --machine) shift; machine="$1" ;;
        --fields) shift; fields="$1" ;;
      esac
      shift
    done
    if [ "$command" = machines ]; then
      printf '[{"id":"local","label":"This Mac"},{"id":"fixture-remote","label":"Remote fixture"}]'
      exit 0
    fi
    [ "$machine" = fixture-remote ] || { echo 'Wrong machine' >&2; exit 2; }
    if [ "$command" = search ]; then
      case ",$fields," in *,machine,*) ;; *) exit 3 ;; esac
    fi
    printf '[]'
    """#
    try script.write(to: executable, atomically: true, encoding: .utf8)
    try FileManager.default.setAttributes([.posixPermissions: 0o700], ofItemAtPath: executable.path)
    let client = MemexClient(executable: executable)
    let choices = try await client.machines()
    #expect(choices == [.local, MachineChoice(id: "fixture-remote", label: "Remote fixture")])
    #expect(try await client.sessions(limit: 5, machine: "fixture-remote").isEmpty)
    #expect(try await client.projects(machine: "fixture-remote").isEmpty)
    #expect(try await client.search("query", project: nil, source: nil, limit: 5, machine: "fixture-remote").isEmpty)
    let session = Session(source: "codex", sessionID: "s", sourcePath: "/remote/path", project: "memex", machine: "fixture-remote")
    #expect(try await client.records(for: session, offset: 0).isEmpty)
}
