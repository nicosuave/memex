# Memex for Qt

A native Qt Quick companion to Memex, built with Rust and
[Qt Bridge for Rust](https://github.com/qt/qtbridge-rust). Rust owns the client,
background work, filtering, activity aggregation, conversation paging, find, and
platform actions. QML supplies native Qt Quick controls and transcript rendering.
It uses the existing Memex configuration and index through the private daemon
socket, with CLI fallback. It does not start or reconfigure the daemon.

## Run locally

Requires Rust 1.88+, Qt 6.10+ development packages (including private Qt Core
headers, Qt Declarative and Qt Quick Controls), and an installed Memex CLI.
`qmake` must resolve to Qt 6. Linux also needs LLD, Gold or Mold: CXX-Qt's
initialization archives cannot be linked with GNU bfd. The app uses CXX-Qt's
platform linker helper to select an installed compatible linker.

```sh
apps/qt/scripts/run.sh
```

To produce an optimized executable archive (also requires `jq` and `tar`):

```sh
apps/qt/scripts/build.sh
# Faster development build, used by pull-request CI:
apps/qt/scripts/build.sh dev
```

Archives are written to `apps/qt/build/memex-qt-OS-ARCH-PROFILE.tar.gz` with the
executable, usage/parity documentation and project license. The build script
uses Cargo's reported artifact path, including when Boxington manages the target
directory. These archives require an installed Memex CLI and compatible Qt runtime;
they are not standalone installers or signed macOS application bundles.
After extracting an archive, run `./memex-qt` from its directory. Linux CI archives
are built against Fedora 44's Qt 6.11 runtime and are not portable Ubuntu binaries.

## Install on Linux (AppImage)

Tagged releases publish a self-contained
`memex-qt-VERSION-linux-ARCH.AppImage` alongside the CLI archives. It bundles
the Qt runtime and the Memex CLI, so neither needs to be installed separately:

```sh
chmod +x memex-qt-VERSION-linux-x86_64.AppImage
./memex-qt-VERSION-linux-x86_64.AppImage
```

Running an AppImage directly needs `libfuse2` on the host. Without it, either
extract and run (`./memex-qt-*.AppImage --appimage-extract-and-run`) or
extract once and launch `squashfs-root/AppRun`.

To build the AppImage locally (also requires `curl`, `jq`, `file`, Qt 6.10+
with `qmake` on PATH, and OpenSSL development packages for the bundled CLI:
`openssl-devel` on Fedora, `libssl-dev` plus `pkg-config` on Ubuntu;
`linuxdeploy` tooling is downloaded automatically):

```sh
apps/qt/scripts/build-appimage.sh
# Or stamp a version into the file name:
apps/qt/scripts/build-appimage.sh 0.21.0
```

Verify a built AppImage with:

```sh
apps/qt/scripts/smoke-appimage.sh apps/qt/build/memex-qt-*-linux-*.AppImage
```

The smoke test extracts the AppImage without FUSE, checks that the Qt runtime
and CLI are bundled, and boots the app headless. Release AppImages are built
on Ubuntu 24.04 because the bundled ONNX Runtime 1.28 requires glibc 2.38 or
newer; do not expect them or Fedora-built archives to run on older distributions.

The script uses Boxington (`mbx`) when installed. An explicit CLI or data root
can be selected without changing user configuration:

```sh
MEMEX_CLI=/absolute/path/to/memex MEMEX_ROOT=/absolute/path/to/data \
  apps/qt/scripts/run.sh
```

A `MEMEX_CLI` override deliberately disables the daemon transport. Otherwise,
the app looks beside its executable, in a macOS bundle's Helpers directory, and
on PATH. All QML and JavaScript resources are embedded in the executable; it does
not require the source checkout at runtime. Qt's shared libraries and QML runtime
modules must still be installed.

For a macOS Qt installation, add its `bin` directory to PATH and set
`DYLD_FRAMEWORK_PATH` to its `lib` directory as described by Qt Bridge. Linux
package names used by the test image are recorded in `Dockerfile`.

## Test in Docker

```sh
apps/qt/scripts/test-docker.sh
```

This builds a Fedora 44 image with Qt 6.11, Rust, LLD, and Qt's test runner. It runs
Rust tests, strict Clippy, full-window tests through Qt Bridge, and native reader
component tests with Qt's software renderer. UI tests use an isolated temporary
root and a **synthetic CLI fixture**, never the user's index or service.

The Cargo download cache is shared; mutable build outputs use a separate named
Docker volume for each checkout. `MEMEX_QT_TARGET_VOLUME` can select an existing
volume, and `MEMEX_QT_TEST_IMAGE` can select the image name. Docker is the tested
Linux environment; platform-specific macOS terminal adapters also have unit tests.
No compiler cache is installed or configured on the host by these scripts.

The `Qt app` GitHub Actions workflow runs the same suites in a Fedora 44 container
on a Linux x86_64 runner whenever Qt app code or the workflow changes. It uses
Boxington's object cache, uploads the development executable archive, and retains
Home/reader screenshots from the synthetic UI fixture. All app-window tests load
the executable's embedded QML resources. The local Docker image and CI install
their dependencies from the same `scripts/install-fedora-deps.sh` manifest.

To run the suites with an existing local Qt toolchain:

```sh
cargo fmt --manifest-path apps/qt/Cargo.toml --check
mbx test --manifest-path apps/qt/Cargo.toml --features test-support
mbx clippy --manifest-path apps/qt/Cargo.toml --all-targets --features test-support -- -D warnings
qmltestrunner -input apps/qt/tests/tst_presentation.qml
```

`test-support` builds the synthetic CLI and full-window test harness. Normal app
builds do not include that fixture. See [PARITY.md](PARITY.md) for the behavioral
contract and validation scope.

## Desktop behavior

The app starts at Home, with All Machines, all history/providers, and Chats only.
Filters, project sort and project summaries persist independently of transcript
pages. Machine results arrive independently; failed peers retain cached data and
show errors. Search is lexical and opens the matched record. Find scans the full
conversation, including earlier pages and tool/context source, and navigates
literal occurrences.

Resume opens a fresh installed terminal using the CLI-provided command and
working directory. Remote conversations must be resumed on their own machine.
Local Codex conversations can open ChatGPT on macOS. File references from remote
machines never resolve against this computer. Local files open read-only previews;
remote HTTP links open only after an explicit user action.

Qt Bridge is pinned to an upstream revision in `Cargo.toml`; `Cargo.lock` pins its
transitive dependencies. Qt/Qt Bridge/CXX-Qt retain their upstream licenses. This
package dynamically links Qt; distributing a standalone desktop bundle requires
including the appropriate Qt runtime modules and license notices. The release
AppImage is such a bundle: usage/parity documentation and the project license
travel inside it under `usr/share/doc/memex-qt`.
