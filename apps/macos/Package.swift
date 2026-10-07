// swift-tools-version: 6.0
import PackageDescription
import Foundation

// A machine-local checkout supplies the optional runtime. No private source URL
// or binary dependency is fetched by ordinary Memex builds.
let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent()
let runtimeRoot = ProcessInfo.processInfo.environment["MEMEX_HISTORY_ONLY"] == "1" ? nil : (ProcessInfo.processInfo.environment["MEMEX_AGENT_RUNTIME_ROOT"]
    ?? (try? String(contentsOf: root.appendingPathComponent(".local-runtime-root"), encoding: .utf8))?
        .trimmingCharacters(in: .whitespacesAndNewlines))
var dependencies: [Package.Dependency] = [
    .package(url: "https://github.com/swiftlang/swift-markdown.git", from: "0.8.0"),
    .package(url: "https://github.com/Lakr233/libghostty-spm.git", exact: "2.2.2026100303"),
]
var appDependencies: [Target.Dependency] = [
    .product(name: "Markdown", package: "swift-markdown"),
    .product(name: "GhosttyTerminal", package: "libghostty-spm"),
    .target(name: "MemexExecutionHostCore"),
]
var runtimeDependencies: [Target.Dependency] = []
var linkerSettings: [LinkerSetting] = []
if let runtimeRoot, !runtimeRoot.isEmpty {
    dependencies.append(.package(path: runtimeRoot + "/packages/sq-acp"))
    dependencies.append(.package(path: runtimeRoot + "/packages/sq-ui"))
    runtimeDependencies = [.product(name: "SQACP", package: "sq-acp"), .product(name: "SQACPHost", package: "sq-acp")]
    appDependencies += runtimeDependencies
    appDependencies.append(.product(name: "SQACPUI", package: "sq-ui"))
    appDependencies.append(.product(name: "SQMcpApps", package: "sq-acp"))
    appDependencies.append(.product(name: "SQMcpAppsUI", package: "sq-acp"))
    let archive = ProcessInfo.processInfo.environment["MEMEX_AGENT_RUNTIME_LIBRARY"]
        ?? runtimeRoot + "/packages/sq-acp/target/runtime-native/aarch64-apple-darwin/release/libsq_acp_runtime.a"
    linkerSettings = [.unsafeFlags(["-Xlinker", archive]),
                      .linkedFramework("CoreFoundation"), .linkedFramework("CoreServices"), .linkedLibrary("iconv")]
}

let package = Package(
    name: "Memex",
    platforms: [.macOS(.v14)],
    products: [
        .executable(name: "Memex", targets: ["Memex"]),
        .executable(name: "MemexExecutionHost", targets: ["MemexExecutionHost"]),
    ],
    dependencies: dependencies,
    targets: [
        .executableTarget(name: "Memex", dependencies: appDependencies, linkerSettings: linkerSettings),
        .target(name: "MemexExecutionHostCore", dependencies: runtimeDependencies, linkerSettings: linkerSettings),
        .executableTarget(name: "MemexExecutionHost",
                          dependencies: [.target(name: "MemexExecutionHostCore")] + runtimeDependencies,
                          linkerSettings: linkerSettings),
        .testTarget(name: "MemexTests", dependencies: ["Memex"]),
        .testTarget(name: "MemexExecutionHostCoreTests", dependencies: ["MemexExecutionHostCore"], linkerSettings: linkerSettings),
    ]
)
