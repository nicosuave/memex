// swift-tools-version: 6.0
import PackageDescription

let package = Package(
    name: "Memex",
    platforms: [.macOS(.v14)],
    products: [.executable(name: "Memex", targets: ["Memex"])],
    dependencies: [.package(url: "https://github.com/swiftlang/swift-markdown.git", from: "0.8.0")],
    targets: [
        .executableTarget(name: "Memex", dependencies: [.product(name: "Markdown", package: "swift-markdown")]),
        .testTarget(name: "MemexTests", dependencies: ["Memex"]),
    ]
)
